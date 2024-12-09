import os
import datetime
import numpy as np
import s3fs
import json

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from botocore import UNSIGNED
import time

session = boto3.session.Session( profile_name="aernasaprod", region_name="us-east-1" )
batch = session.client( service_name="batch")
s3 = s3fs.S3FileSystem( client_kwargs={ 'region_name': "us-east-1" }, profile = "aernasaprod")

def submit_batch(jobName, command):
    response = batch.submit_job(
        jobName = jobName,
        jobQueue = "ro-processing-SPOT",
        jobDefinition = f"ro-processing-framework",
        containerOverrides =
        {
            'command': command ,
            'vcpus': 1,
            'memory': 1900,
        }
    )

def s3_upload(local_file, bucket_name, objKey):

    s3_client = session.client('s3')
    s3_client.upload_file(local_file, bucket_name, objKey)

def create_batch_json(nc_rerun,datstr,AWSversion):
    #for each batchprocess error create batchprocess json file for reprocessing
    jobsperfile = 3000

    for center in nc_rerun.keys():
        for prefix in nc_rerun[center].keys():
            nfiles = int(np.ceil(len(nc_rerun[center][prefix])/jobsperfile))
            print(nfiles)

            for c in range(0,nfiles):
                njobS = jobsperfile * (c)
                njobE = jobsperfile * (c+1)

                if njobE > len(nc_rerun[center][prefix]):
                    njobE = len(nc_rerun[center][prefix])

                batch_json = {
                    "InputPrefix": prefix,
                    "ProcessingCenter": center,
                    "InputFiles": nc_rerun[center][prefix][njobS:njobE]
                }
                json_filename = f"{center}-reprocessV{AWSversion.replace('.', '_')}-logs{datstr}-{c+1:03d}.json"
                with open(json_filename,'w') as file:
                    file.write(json.dumps(batch_json))

                #cp file to s3
                uri = os.path.join("batchprocess-jobs",json_filename)
                s3_upload(json_filename, "gnss-ro-processing-definitions", uri)

                #submit batch job
                command = ['batchprocess', f"s3://gnss-ro-processing-definitions/{uri}","--version",AWSversion, "--clobber"]
                jobName = json_filename[:-5]
                print(jobName, njobS, njobE, len(nc_rerun[center][prefix][njobS:njobE]))
                submit_batch(jobName, command)

def check_logs(datstr,bucket, AWSversion):
    df = {}

    prefix  = f'logs/{AWSversion.replace(".","_")}/{datstr}/errors'
    log_file_list = s3.ls( os.path.join( bucket, prefix ) )
    error_list = []

    for log in log_file_list:
        df[log] = {}

        with s3.open(log,'r') as file:
            lines = file.readlines()
            for line in lines:

                if line[:4] == "/opt":
                    error_list.append(line)

        for line in error_list:
            fail_file = line.split(" ")[0]
            fail_type = line.split(":")[1]
            if fail_file not in df[log].keys():
                df[log][fail_file] = []

            if "Results: " in line:
                #Results: {"status": "fail", "job": {"processing_center": "ucar", "file_type": "level1b", "input_prefix": "s3://gnss-ro-data-test", "input_file": "untarred/spire/noaa/nrt/level1b/2022/037/conPhs_nrt_2022_037/conPhs_S104.2022.037.21.20.G04_0001.0001_nc"}, "messages": ["EntryExists", "LoggingEntryInDatabase", "NoInfoAdded"]}
                #print(line.split("Results: ")[1])
                keep = json.loads(line.split("Results: ")[1])
                df[log][fail_file].append(keep)
            else:
                df[log][fail_file].append(line)


    for key in df.keys():
        for k in df[key].keys():
            #print(k,len(df[key][k]))
            try:
                #can't set of dict, only []
                df[key][k] = list(set(df[key][k]))
            except:
                continue
    '''
    with open("log.lst",'w') as file:
        for l in log_file_list:
            file.write(l + "\n")
    '''

    nc_rerun = {}
    other_errors = []
    for key in df.keys():
        for k in df[key].keys():
            for each in df[key][k]:
                #print(key,k,each)

                try:
                    #add .nc file to list for reprocessing
                    if each["job"]["processing_center"] not in nc_rerun.keys():
                         nc_rerun[each["job"]["processing_center"]] = {}
                    if each["job"]["input_prefix"] not in nc_rerun[each["job"]["processing_center"]].keys():
                         nc_rerun[each["job"]["processing_center"]][each["job"]["input_prefix"]] = []

                    nc_rerun[each["job"]["processing_center"]][each["job"]["input_prefix"]].append(each["job"]["input_file"])
                except:

                    other_errors.append(each)

    return nc_rerun, list(set(other_errors))

if __name__ == "__main__":
    datstr = "20230428"
    AWSversion = "1.1"  #1.1, 2.0
    bucket = "gnss-ro-processing-definitions"#"gnss-ro-data-test"
    Test = True

    nc_rerun, other_errors = check_logs(datstr,bucket, AWSversion) #"gnss-ro-data-test", 1.1, 2.0

    if Test:
        print(nc_rerun['ucar'].keys())
        #print(nc_rerun)
        print(other_errors)
    else:
        create_batch_json(nc_rerun,datstr,AWSversion)

    #TODO reprocess other_errors
