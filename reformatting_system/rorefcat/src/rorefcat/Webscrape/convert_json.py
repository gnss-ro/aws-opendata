#!/usr/bin/env python3
'''This takes the dynamo export json and converst it so the dynamo utiltiy can
parse easily'''

import os
import boto3
import json
import s3fs

from ..Utilities.resources import stagingBucket

ro_fs = s3fs.S3FileSystem( client_kwargs={'region_name':'us-east-1'} )
#ro_fs = s3fs.S3FileSystem( profile='nasa', client_kwargs={'region_name':'us-east-1'} )

#session = boto3.Session(profile_name='nasa', region_name = "us-east-1")
session = boto3.Session(region_name = "us-east-1")
s3 = session.resource( "s3" )
batch = session.client( service_name="batch")
s3_client = session.client('s3', region_name = "us-east-1")

def reformat_for_dynamo_utility(obj):
    fileDict = {}
    #open the file with s3fs joining the bucket and key
    with ro_fs.open(os.path.join(obj.bucket_name,obj.key), 'r') as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        tempDict = {}
        fileDict[i] = {}
        tempDict = json.loads(line.strip())
        nKeys = ['longitude', 'latitude', 'local_time']

        for k in tempDict.keys():
            if k == "occ_duration": continue
            if k in nKeys:
                fileDict[i][k] = float(tempDict[k]['N'])
            elif k in ["gps_seconds", "leo-ttt"]:
                continue
            elif k == "setting" and tempDict[k]['S'] == "True":
                fileDict[i][k]= True
            elif k == "setting" and tempDict[k]['S'] == "False":
                fileDict[i][k]= False
            else:
                if 'S' in tempDict[k].keys():
                  fileDict[i][k] = tempDict[k]['S']
                else:
                  fileDict[i][k] = tempDict[k]['N']

    local_file = os.path.basename(obj.key)
    with open(local_file,'w') as newfile:
        newfile.write(json.dumps(fileDict))

    return(local_file)

def sync_folder(version, mission, year_list):
    valid_file_types = [ "calibratedPhase", "refractivityRetrieval", "atmosphericRetrieval" ]

    #get valid processing_centers and se which have the provided mission
    proc_list = ro_fs.ls( os.path.join( "gnss-ro-data-staging", f'contributed/v1.1/' ) )
    processing_centers = [os.path.basename(item) for item in proc_list]

    center_list = []
    for center in processing_centers:
        mission_list = ro_fs.ls( os.path.join( "gnss-ro-data-staging", f'contributed/v1.1/{center}/' ) )
        valid_missions = [os.path.basename(item) for item in mission_list]
        if mission in valid_missions: center_list.append(center)

    for type in valid_file_types:
        for year in year_list:
            for center in center_list:

                prefix = f"contributed/v{version}/{center}/{mission}/{type}/{year}"
                jobName = f"sync-{center}-{mission}-{type}-{year}-{version.replace('.','_')}"
                command = ["liveupdate_wrapper", "sync", version, "--prefix",prefix]

                response = batch.submit_job(
                    jobName = jobName,
                    jobQueue = "ro-processing-SPOT",
                    jobDefinition = "ro-processing-framework",
                    containerOverrides =
                    {
                        'command': command ,
                        'vcpus': 1,
                        'memory': 1500
                    },
                    retryStrategy = {
                        "attempts": 2
                    }
                )

#  Main program.

def main(version,mission):
    aws_version = version['AWSversion']
    output_s3_prefix = f"dynamo/v{aws_version.replace('_','.')}/export_subsets"
    output_s3_prefix_pipeline = f"dynamo/v{aws_version.replace('_','.')}/export_pipeline"
    export_bucket = stagingBucket

    s3_bucket = s3.Bucket( export_bucket )
    year_list = []
    for obj in s3_bucket.objects.filter( Prefix=f"{output_s3_prefix_pipeline}" ):
        if mission in obj.key:
            file = reformat_for_dynamo_utility(obj)
            year_list.append(file.split('_')[1][:4])
            s3_path = os.path.join(output_s3_prefix,os.path.basename(file))
            #put new file back on s3
            s3_client.upload_file(file, export_bucket, s3_path)

    # with list of years for the missions submit batch sync all ro files to open data
    TEST = os.getenv( "TEST" )
    if TEST is None:
        print(f"ready to sync {mission}, wait for DataSync cron")
        #sync_folder(aws_version, mission, list(set(year_list)))
    elif TEST is not None:
        print("not syncing, this is a test.")

if __name__ == "__main__":
    
    main("2.0","cosmic1")
