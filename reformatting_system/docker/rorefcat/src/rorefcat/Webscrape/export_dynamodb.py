#!/usr/bin/env python3

import os
import boto3
import pandas as pd
import datetime
import time
import json

from . import job_tracking as track

#session = boto3.Session(profile_name="aernasaprod", region_name = "us-east-1")
session = boto3.Session(region_name = "us-east-1")
s3 = session.resource( "s3" )
s3_client = session.client('s3', region_name = "us-east-1")
db_client = session.client('dynamodb', region_name = "us-east-1")
batch = session.client( service_name="batch")
todayMMDD = datetime.datetime.today().strftime('%m%d')
todayDate = datetime.datetime.today().strftime("%Y%m")

def del_s3_export(bucket, prefix):
    s3_bucket = s3.Bucket( bucket )
    for obj in s3_bucket.objects.filter( Prefix=f"{prefix}" ):
        obj.delete()

def export_dynamo_json(export_bucket, export_table_arn, export_prefix):

    #export dynamo table to s3 bucket with prefix in json format
    response = db_client.export_table_to_point_in_time(
        TableArn= export_table_arn,
        S3Bucket= export_bucket,
        S3Prefix= export_prefix,
        S3SseAlgorithm='AES256',
        ExportFormat='DYNAMODB_JSON'
    )

    #print(response)
    arn = response['ExportDescription']['ExportArn']
    print("Exporting dynamodb table ... may take 15 minutes ...")
    #check status of export
    response = db_client.describe_export(
        ExportArn = arn
    )

    while response['ExportDescription']['ExportStatus'] != 'COMPLETED':
        #if export is done, get the manifest file path
        #TODO could possibly make this lambda trigger if it takes forever
        response = db_client.describe_export(
            ExportArn = arn
        )
        time.sleep(180)
        print(response['ExportDescription']['ExportStatus'])

    print(response['ExportDescription'])
    #returns path of the summary.json, but we want the file.jason 'dynamoDB_Exports/AWSDynamoDB/01644253587581-5824221e/manifest-summary.json'
    export_sumary_path = response['ExportDescription']['ExportManifest']
    return export_sumary_path.replace('summary','files')

def read_manifest(manifest_file_s3_path, local_prefix, bucket):
    #return list of json files to read
    print("Reading export manifest ....")
    manifest_file_local_path = os.path.join(local_prefix,'manifest-files.json')

    if not os.path.exists(local_prefix):
        os.mkdir(local_prefix)

    manifest_file_s3_download_path = manifest_file_s3_path.split(bucket)[1][1:]
    print(bucket, manifest_file_s3_download_path, manifest_file_local_path)

    s3_client.download_file(bucket, manifest_file_s3_download_path, manifest_file_local_path)

    df = pd.read_json(manifest_file_local_path, lines=True)

    file_list = []
    for item in df["dataFileS3Key"]:
        file_s3_path = item
        local_path_gz = os.path.join(local_prefix,str(item.split('/')[-1]))
        local_path_ungz = local_path_gz[:-3]
        if not os.path.isfile(local_path_ungz):
            #copy s3 file locally and unzip
            s3_client.download_file(bucket, file_s3_path, local_path_gz)
            #os.system(f'gzip -d {local_path_gz}')

        file_list.append(local_path_ungz)
    #returns array of local gz files to loop through
    return(file_list)

def separate_by_mission_and_year(file_list, local_prefix, mission):
    print("Separating table items by mission and year ....")
    #loops through files and writes Items to respective {mission}_YYYY-MM-DD.txt
    mission_file_objects = []
    file_name_array= []
    for file in file_list:
        print(file)
        os.system(f'gzip -d {file}.gz')
        df = {}
        df = pd.read_json(file, lines=True)
        #df.info(memory_usage=True)
        for i, item in enumerate(df["Item"]):
            if item["mission"]['S'] != mission: continue

            #split by day
            y_m_d = item["date-time"]['S'][:10]

            file_name = os.path.join(local_prefix,f'{mission}_{y_m_d}.json')
            #create and open file for mission and date
            if file_name not in file_name_array:
                file_name_array.append(file_name)
                file = open(file_name,'w')
                mission_file_objects.append(file)

            file_index = file_name_array.index(file_name)
            Dict = {}
            Dict['Item'] = item
            mission_file_objects[file_index].write(json.dumps(Dict)+'\n')

    for file in mission_file_objects:
        file.close()

    return file_name_array

def reformat_for_Datapipeline_import(file):
    datapipeline_array = []
    df = pd.read_json(file, lines=True)

    for each in df["Item"]:
        line = json.dumps(each)
        #line = line.replace("S","s")
        #line = line.replace("N","n")
        datapipeline_array.append(line)

    with open(file, 'w') as newfile:
        for item in datapipeline_array:
            newfile.write(f"{item}\n")

def submit_batch_all(version, manifest_uri, valid_missions):
    '''this module submits this container but with args to susbet for datapipline
    and then for utility by mission for efficiency, then sync dynamo to open data
     s3'''
    for m in valid_missions['aws']:
        print( f"submitting: export jobs for ",m )
        command = ["liveupdate_wrapper","export", version, "--manifest_file_s3_path", \
                manifest_uri, "--mission", m]
        jobName = f'export_{version.replace(".","_")}_{m}_{todayMMDD}'

        #for tracking jobs via dynamo
        job_tracking = {}
        job_tracking = {
            'job-date': f"export-{todayDate}",
            'jobname': jobName,
            'test': "false",
            'ram': 15000,
            'version': version,
            'center': "",
            "mission": "",
            'process_date': "",
            'command': command
        }

        dependsID = track.main(job_tracking)


def sync_dynamo(version, dependsID):
    #sync dynamo files
    jobName = f"sync-dynamo-{version.replace('.','_')}"
    version = version.replace("_",".")
    prefix = f"dynamo/v{version}"
    command = ["liveupdate_wrapper", "sync", version, "--prefix",prefix]

    print( f"submitting: dynamo sync job" )
    #for tracking jobs via dynamo
    job_tracking = {}
    job_tracking = {
        'job-date': f"sync-{todayDate}",
        'jobname': jobName,
        'test': "false",
        'ram': 1500,
        'version': version,
        'center': "",
        "mission": "",
        'process_date': "",
        'dependsOn': dependsID,
        'command': command
    }

    dependsID = track.main(job_tracking)

def submit_batch_utility(version,m):
    #submit to convert for utility
    command = ["liveupdate_wrapper", "convert", version, "--mission",m]
    jobName = f"export_convert_{version.replace('.','_')}_{m}_{todayMMDD}"

    #for tracking jobs via dynamo
    job_tracking = {}
    job_tracking = {
        'job-date': f"convert-{todayDate}",
        'jobname': jobName,
        'test': "false",
        'ram': 15000,
        'version': version,
        'center': "",
        "mission": m,
        'process_date': "",
        'command': command
    }

    dependsID = track.main(job_tracking)

    TEST = os.getenv( "TEST" )
    if m == "cosmic1" and TEST is None:
        sync_dynamo(version, dependsID)
    elif TEST is not None:
        print("not syncing, this is a test.")

#  Main program.

def main(version,manifest_file_s3_path,mission,valid_missions):

    aws_version = version['AWSversion']
    export_bucket = version['module'].stagingBucket
    export_table_arn = f"arn:aws:dynamodb:us-east-1:996144042418:table/{version['module'].dynamodbTable}"
    export_prefix = f"dynamo/v{aws_version.replace('_','.')}/export_raw"
    local_prefix = '/opt/test_files'
    output_s3_prefix = f"dynamo/v{aws_version.replace('_','.')}/export_subsets"
    output_s3_prefix_pipeline = f"dynamo/v{aws_version.replace('_','.')}/export_pipeline"

    print(export_bucket)
    if not manifest_file_s3_path:
        #delete previous export_raw
        del_s3_export(export_bucket, export_prefix)
        del_s3_export(export_bucket, output_s3_prefix_pipeline)
        del_s3_export(export_bucket, output_s3_prefix)
        
        #export table to json on s3 and wait for manifest name
        manifest_file_s3_path = export_dynamo_json(export_bucket, export_table_arn, export_prefix)
        manifest_file_s3_path = f's3://{export_bucket}/{manifest_file_s3_path}'
        print('export table', manifest_file_s3_path)
        submit_batch_all(aws_version, manifest_file_s3_path, valid_missions)
    else:
        print(manifest_file_s3_path)
        #read in manifests file to get list of files
        json_file_list = read_manifest(manifest_file_s3_path, local_prefix, export_bucket)

        output_json_list = separate_by_mission_and_year(json_file_list, local_prefix, mission)
        print(output_json_list)

        #cp csv to s3
        for file in output_json_list:
            #reformat for easy import using AWS Data Pipeline
            reformat_for_Datapipeline_import(file)
            s3_path = os.path.join(output_s3_prefix_pipeline,os.path.basename(file))
            s3_client.upload_file(file, export_bucket, s3_path)

        submit_batch_utility(aws_version,mission)
        print(f"JSON files copied to {export_bucket}/{output_s3_prefix}/")



if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Take input from lambda and create input json file for batchprocess")
    parser.add_argument("aws_version", type=str) #"v1_0" or v1.0
    parser.add_argument("--manifest_file_s3_path", dest="manifest_file_s3_path", type=str, default=None )
    parser.add_argument("--mission", dest="mission", type=str, default=None)
    args = parser.parse_args()

    main(args.aws_version,args.manifest_file_s3_path,args.mission)
