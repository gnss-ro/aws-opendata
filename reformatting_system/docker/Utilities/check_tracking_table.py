#!/usr/bin/env python3
'''
Puprpose:
This script is meant to track the batch jobs run for ro processing.
Webscrape/job_tracking.py has the basic format needed to run batch jobs and put
the run in the trakcing dynamodb table.

Each Batch job has the partition key of the run type [webscrape, batchprocess,
createjobs, ro] and the date (YYYYMMDD).  The sort key is the center and the
batch jobID to make querying possible. We have to then loop through partition key
and sort key combo query the table.

Input:
the date string (YYYYMM) of when the jobs were run. (Note the ro processing date)

Work Flow:
1. the script will clean previous jobs that have succeeded as we no longer
need to track them.
2. Query the tracking table for all that are 'RUNNING'.  Then check Batch to see
if the jobs have FAILED or SUCCEEDED.  The table is then updated accordingly with
additional helpful parameters in the table.
3. Manual step must happen here.  All Failed jobs from step 2 will have a 'rerun_status'
of 'check'.  Here someone will need to figure out if the job can be rerun or there
is something wrong with a file.  If it's a code issue fix the ECR and then set this
Item's 'rerun_status' to "true".  (Note: note using dynamo bool as it tricky to parse)
4. Query the table for all jobs that are ready to rerun and update the table so
they can be checked again later.
5. Summary output stating how many jobs failed and how many were checked.  Also
the number that have been rerun.
6. Jobs that are rerun will have status >> SUCCEEDED as a new item will be created for
the new attempt.

Author: Amy McVey
Date: May 19, 2023
'''

import os, sys
import string
import boto3
from boto3.dynamodb.conditions import Key, Attr

sys.path.append('..')
from Webscrape import job_tracking as track

#create s3 boto3 object and session
session = boto3.session.Session( profile_name="aernasaprod", region_name="us-east-1" )
#session = boto3.Session( region_name= "us-east-1")
dynamodb = session.resource('dynamodb')
batch = session.client( service_name="batch")
cloudwatch_logs = session.client('logs')
sns_client = session.client('sns')

tracking_table_name = "job-tracking"
job_type_list = ['webscrape','batchprocess','createjobs','convert','sync','export','eumetsat_download']
center_list = ['ucar','romsaf','jpl','ro-','eumetsat']

tracking_table = dynamodb.Table(tracking_table_name)

def send_sns(message):
    topic_arn = f'arn:aws:sns:us-east-1:996144042418:webscrape'

    response = sns_client.publish(
        TopicArn= topic_arn,
        Message= message,
        Subject= f'Batch Job Tracking',
        MessageStructure= 'String',
        MessageAttributes= {
            'njobs': {
                'DataType': 'String',
                'StringValue': "filler",
            }
        }
    )

def get_batch_info(jobID):
    try:
        response = batch.describe_jobs(
            jobs=jobID
        )

        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/describe_jobs.html
        new_status = response["jobs"][0]['status']

        if new_status == "RUNNING":
            exit_code = ""
        elif "exitCode" in response["jobs"][0]['container'].keys():
            exit_code = response["jobs"][0]['container']['exitCode']
        else: #HOST EC2 Terminated
            exit_code = 999

        log_stream = response["jobs"][0]['container']['logStreamName']
    except:
        new_status = "OLD"
        exit_code = ""
        log_stream = ""

    return new_status, exit_code, log_stream

def query_table(job_date,center,attr,val):
    response_list = []
    response = tracking_table.query(
        TableName = tracking_table_name,
        KeyConditionExpression = Key('job-date').eq(job_date) & Key('jobID').begins_with(center),
        FilterExpression= Attr(attr).eq(val)
    )
    response_list.extend(response['Items'])

    while "LastEvaluatedKey" in response.keys():
        print("checking query again")
        response = tracking_table.query(
            TableName = tracking_table_name,
            KeyConditionExpression = Key('job-date').eq(job_date) & Key('jobID').begins_with(center),
            FilterExpression= Attr(attr).eq(val),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )

        response_list.extend(response['Items'])

    return response_list

def update_table_prep(each,new_status, exit_code, log_stream):
    #print("updating table for:",each['job-date'],each['jobID'])

    if new_status == "FAILED":
        rerun_status = "check"
    elif new_status == "SUCCEEDED":
        rerun_status = "false"
        
    if exit_code == 999: #for spot killed jobs
            rerun_status = "true"
        
    new_names = ['status','exit_code','rerun_status','logStreamName']
    new_vals = [new_status,exit_code,rerun_status, log_stream]

    update_table(each,new_names,new_vals)

def update_table(each,new_names,new_vals):
    new_count = list(string.ascii_uppercase[0:len(new_names)])

    new_names_tupple = []
    new_vals_tupple = []
    for i in range(0,len(new_names)):
        new_names_tupple.append((new_count[i],new_names[i]))
        new_vals_tupple.append((new_count[i],new_vals[i]))

    #  Update table with values in update_values
    response = tracking_table.update_item(
            Key = {'job-date': each['job-date'], 'jobID': each['jobID']},
            UpdateExpression = "SET " + ", ".join( [ f"#attr{key} = :val{key}" for key in new_count ] ),
            ExpressionAttributeNames = { f"#attr{key}": val for key, val in new_names_tupple },
            ExpressionAttributeValues = { f":val{key}": val for key, val in new_vals_tupple },
    )

def get_item_to_check(check_date):
    #list of dictionary
    job_data_list = []

    #cover all possible run types
    for job in job_type_list:
        job_date = f"{job}-{check_date}"
        #query table for job_date
        for center in center_list:
            job_data_list.extend(query_table(job_date,center,"status", "RUNNING"))

    return job_data_list

def check_update_status(job_data_list):
    fail_count = 0

    for each in job_data_list:

        center = each['jobID'].split('-')[0] + '-'
        new_status, exit_code, log_stream = get_batch_info([each['jobID'].split(center)[1]])

        if new_status == "FAILED":
            #output to screen job's cloudwatch log
            fail_count += 1
            print(new_status,each['jobname'])
            #get_logStream(log_stream)

        if new_status == "FAILED" or new_status == "SUCCEEDED" or new_status == "OLD":
            update_table_prep(each,new_status,exit_code, log_stream)
        else:
            continue

    return fail_count

def count_jobs2check():
    list = []
    #check succeeded
    response = tracking_table.scan(
        TableName = tracking_table_name,
        FilterExpression= Attr('rerun_status').eq("check")
    )
    list.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = tracking_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        list.extend(response['Items'])

    return len(list)

def rerun_jobs(check_date):
    #list of dictionary
    job_data_list = []
    count = 0

    #cover all possible run types
    for job in job_type_list:
        job_date = f"{job}-{check_date}"
        #query table for job_date
        for center in center_list:
            job_data_list.extend(query_table(job_date,center,"rerun_status", "true"))

    for each in job_data_list:
        #for tracking jobs via dynamo
        job_tracking = {}
        job_tracking = {
            'job-date': each['job-date'],
            'jobname': each['jobname'],
            'test': "false",
            'ram': int(each['ram']),
            'version': each['version'],
            'center': each['center'],
            "mission": each['mission'],
            'process_date': each['process_date'],
            'command': each['command']
        }

        count += 1
        dependsID = track.main(job_tracking)
        #prep table update lists
        new_names = ['status','exit_code','rerun_status']
        new_vals = ["SUCCEEDED","0","false"]

        update_table(each,new_names,new_vals)

    return count

def get_logStream(log_stream):

    response = cloudwatch_logs.get_log_events(
        logGroupName = "/aws/batch/job",
        logStreamName= log_stream
    )

    for each in response['events']:
        print(each['message'])

def clean_succeeded():
    list = []
    #check succeeded
    response = tracking_table.scan(
        TableName = tracking_table_name,
        FilterExpression= Attr('status').eq("SUCCEEDED")
    )
    list.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = tracking_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        list.extend(response['Items'])

    succeeded = len(list)

    if succeeded != 0:
        print("The following runs have completed successfully, removing ...",succeeded)

    #check OLD
    response = tracking_table.scan(
        TableName = tracking_table_name,
        FilterExpression= Attr('status').eq("OLD")
    )
    list.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = tracking_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        list.extend(response['Items'])

    old = len(list) - succeeded
    if old != 0:
        print("The following runs are marked OLD, removing ...",old)

    #check false FAILED
    response = tracking_table.scan(
        TableName = tracking_table_name,
        FilterExpression= Attr('status').eq("FAILED") & Attr('rerun_status').eq("false")
    )
    list.extend(response['Items'])

    fail = len(list) - old
    if fail != 0:
        print("The following runs had FAILED, removing ...",fail)

    while 'LastEvaluatedKey' in response:
        response = tracking_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        list.extend(response['Items'])

    if len(list) > 0:
        for each in list:
            #print(each['job-date'],each['jobID'])
            #delete item
            response = tracking_table.delete_item(
                Key = {'job-date': each['job-date'], 'jobID': each['jobID']}
            )

    return len(response['Items'])

def main(check_date):
    job_data_list =[]

    #clean dynamo of items that have succeeded to keep it manageable
    print('Cleaning Table ...')
    clean_succeeded()

    print('Checking Table ...')
    #make list of items in table to check on
    job_data_list = get_item_to_check(check_date)

    print('Updating Table ...')
    fail_count = check_update_status(job_data_list)

    print('Rerunning Jobs ...')
    #check how many jobs need to be rerun, do so, and update the table
    #NOT DONE IF RUN WITH CRON
    need_rerun = rerun_jobs(check_date)

    print('Counting Jobs to Check ...')
    check_count = count_jobs2check()

    print('\n### Batch Job Tracking Summary ###')
    print("failed runs: ",fail_count," out of a total: ", len(job_data_list))
    print("rerunning: ",need_rerun)
    print("to check: ",check_count)

    message = f"Date Checked: {check_date}\nfailed runs: {fail_count} out of a total: {len(job_data_list)}"
    #send_sns(message)

if __name__ == "__main__":
    main("202308")
