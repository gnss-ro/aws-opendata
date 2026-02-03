#!/usr/bin/env python3

import os, sys
import boto3

#create s3 boto3 object and session
try:
    session = boto3.Session( profile_name="nasa", region_name="us-east-1" )
except:
    session = boto3.Session( region_name="us-east-1" )

dynamodb = session.resource('dynamodb')
batch = session.client( service_name="batch")

tracking_table = dynamodb.Table("job-tracking")

def create(job_tracking):
    #Put Item:
    return
    #racking_table.put_item(
    #   Item = job_tracking)

def submit_batch_test(job_tracking):
    if "romsafD" in job_tracking['jobname'] or "clean" in job_tracking['jobname'] or "sync" in job_tracking['jobname']:
        timeout = 36000
    else:
        timeout = 7200

    response = batch.submit_job(
        jobName = job_tracking['jobname'],
        jobQueue = job_tracking['queue'],
        jobDefinition = job_tracking['jobdef'],
        containerOverrides =
        {
            'command': job_tracking['command'] ,
            'vcpus': 1,
            'memory': job_tracking['ram'],
            'environment': [
                {
                    'name': "TEST",
                    'value': "1"
                }
            ]
        },
        timeout={
            'attemptDurationSeconds': timeout
        }

    )

    if len(job_tracking["center"]) > 0:
        job_tracking["jobID"] = f'{job_tracking["center"]}-{response["jobId"]}'
    else:
        job_tracking["jobID"] = f'ro-{response["jobId"]}'

    create(job_tracking)

def submit_batch(job_tracking):
    if "dependsOn" in job_tracking.keys():
        response = batch.submit_job(
            jobName = job_tracking['jobname'],
            jobQueue = job_tracking['queue'],
            jobDefinition = job_tracking['jobdef'],
            dependsOn = [{"jobId":job_tracking['dependsOn']}],
            containerOverrides =
            {
                'command': job_tracking['command'] ,
                'vcpus': 1,
                'memory': job_tracking['ram']
            },
            timeout={
                'attemptDurationSeconds': 17200
            }

        )
    else:
        response = batch.submit_job(
            jobName = job_tracking['jobname'],
            jobQueue = job_tracking['queue'],
            jobDefinition = job_tracking['jobdef'],
            containerOverrides =
            {
                'command': job_tracking['command'] ,
                'vcpus': 1,
                'memory': job_tracking['ram']
            },
            timeout={
                'attemptDurationSeconds': 17200
            }

        )

    if len(job_tracking["center"]) > 0:
        job_tracking["jobID"] = f'{job_tracking["center"]}-{response["jobId"]}'
    else:
        job_tracking["jobID"] = f'ro-{response["jobId"]}'

    create(job_tracking)

    return response["jobId"]

def main(job_tracking):

    if len(job_tracking.keys()) ==0:
        print("do nothing")

    #calculate some parameters
    jobType = job_tracking['job-date'].split('-')[0]
    if jobType in ["batchprocess","createjobs","sync",'eumetsat_download',"clean","romsafD"]:
        job_tracking['queue'] = "ro-processing-SPOT"
    elif jobType in ["export","webscrape","convert"]:
        job_tracking['queue'] = "ro-processing-EC2"
    else:
        job_tracking['queue'] = "ro-processing-EC2"

    jobName = job_tracking['jobname']
    if 'level' not in job_tracking.keys():
        if "atmPhs" in jobName or "conPhs" in jobName:
            job_tracking["level"] = "level1b"
        elif "atmPrf" in jobName or "wetP" in jobName:
            job_tracking["level"] = "level2"
        elif "atmosphericRetrieval" in jobName or "refractivityRetrieval" in jobName:
            job_tracking["level"] = "level2"
        elif "calibratedPhase" in jobName:
            job_tracking["level"] = "level1b"
        else:
            job_tracking["level"] = ""

    #set defaults
    job_tracking['status'] = "RUNNING"
    job_tracking['exit_code'] = ""
    job_tracking['rerun_status'] = ""

    if job_tracking['test'] == "true":
        job_tracking['jobdef'] = "ro-processing-framework-test"
        dependsID = submit_batch_test(job_tracking)
    else:
        job_tracking['jobdef'] = "ro-processing-framework"
        dependsID = submit_batch(job_tracking)

    # return ID so another job can depend on it
    return dependsID

if __name__ == "__main__":
    job_tracking = {}
    main(job_tracking)


'''
job_tracking = {}
job_tracking = {
    'job-date': f"batchprocess-{todayDate}",
    'jobname': jobName,
    'test': "false",
    'ram': 1800,
    'version': AWSversion,
    'center': params['center'],
    "mission": mission,
    'process_date': os.path.basename(command[1]).split('.')[0][-9:],
    'command': command
}

dependsID = track.main(job_tracking)
'''
