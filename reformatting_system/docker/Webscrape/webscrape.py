#!/usr/bin/env python3

import os, sys
import json
import boto3
import requests
import tarfile
from datetime import datetime

import Webscrape.job_tracking as track

#session = boto3.session.Session(profile_name = "aernasaprod", region_name = 'us-east-1' )
session = boto3.session.Session( region_name = 'us-east-1' )
todayDate = datetime.today().strftime("%Y%m")

def main(tarfile, AWSversion, romsaf):

    params = {
        'bucket_name': 'ucar-earth-ro-archive-liveupdate',
        'staging_bucket_name': 'gnss-ro-data-staging',
        'untarred': "untarred",
        'tar': "tarballs",
        'json_prefix': "batchprocess-jobs",
        "workdir": '/opt/',
        "local_untarred": 'untarred/',
        "repo_url": "https://data.cosmic.ucar.edu/gnss-ro/",
        "center": "ucar",
        "tarfile": tarfile
    }

    #change params for romsaf otherwise assume ucar
    if romsaf:
        params['bucket_name'] = 'romsaf-earth-ro-archive-liveupdate'
        params['repo_url'] = "https://www.romsaf.org/pub/icdr/v1-series/profs/"
        params['center'] = "romsaf"
        #tarfile = atm/2017/atm_20170223_metop_I_2320_0010.tgz

    #  For testing...

    TEST = os.getenv( "TEST" )

    if TEST is not None:
        params['bucket_name'] = "gnss-ro-data-test"
        params['staging_bucket_name'] = "gnss-ro-data-test"

    #get list of files in tar
    input_files = webscrape_input_driver([tarfile], params)

    json_local_loc = create_batch_json(input_files, params)

    json_objkey = os.path.join(params['json_prefix'], json_local_loc )
    upload_to_s3(json_local_loc, params['bucket_name'], json_objkey)

    batch = session.client( service_name="batch")

    command = ["batchprocess", f"s3://{params['bucket_name']}/{json_objkey}","--version",AWSversion]

    jobName = os.path.basename(json_objkey).replace(".","_")
    print("command",command)

    #s3://ucar-earth-ro-archive-liveupdate/batchprocess-jobs/cosmic2_wetPf2_nrt_2022_058.tar.json
    try:
        if TEST is None:
            command.append('--clobber')

            #for tracking jobs via dynamo
            job_tracking = {}
            job_tracking = {
                'job-date': f"batchprocess-{todayDate}",
                'jobname': f"{AWSversion.replace('.', '_')}_{jobName}_{params['center']}",
                'test': "false",
                'ram': 1800,
                'version': AWSversion,
                'center': params['center'],
                "mission": os.path.basename(command[1]).split('_')[0],
                'process_date': os.path.basename(command[1]).split('.')[0][-9:],
                'command': command
            }

            dependsID = track.main(job_tracking)

        else:
            command.append('--clobber')
            #for tracking jobs via dynamo
            job_tracking = {}
            job_tracking = {
                'job-date': f"batchprocess-{todayDate}",
                'jobname': f"{AWSversion.replace('.', '_')}_{jobName}_{params['center']}_test",
                'test': "true",
                'ram': 1800,
                'version': AWSversion,
                'center': params['center'],
                "mission": os.path.basename(command[1]).split('_')[0],
                'process_date': os.path.basename(command[1]).split('.')[0][-9:],
                'command': command
            }

            dependsID = track.main(job_tracking)

    except Exception as e:
        print(e)
        sys.exit(3)

    print(jobName)

def create_batch_json(input_files, params):

    input_json = {
        'InputPrefix': f"s3://{params['bucket_name']}",
        'ProcessingCenter': params['center'],
        'InputFiles': input_files
    }
    mission = input_files[0].split('/')[1]
    file_base = os.path.basename(input_files[0])
    json_file_loc = mission + "_" + os.path.basename(params['tarfile'][:-7]) + ".json"

    with open(json_file_loc, 'w') as jsonFile:
        json.dump(input_json, jsonFile)

    return json_file_loc

def upload_to_s3(file_to_upload, bucket_name, objKey):

    s3_client = session.client('s3')
    try:
        s3_client.upload_file(file_to_upload, bucket_name, objKey)
    except Exception as e:
        print(e)
        sys.exit(3)

def download_and_untar(input_files, params):

    new_input_file_list = []
    for fileUrl in input_files:
        #set url to download
        repo_file_url = os.path.join(params['repo_url'], fileUrl)
        print(repo_file_url)
        if params['center'] == "ucar":
            with requests.get(repo_file_url, stream=True) as r:
                if r.status_code == 200:
                    print("Got file... ", repo_file_url)
                    #set and make local dir for download
                    local_dir = os.path.join(params['local_untarred'], fileUrl[:-7], '')
                    os.makedirs(local_dir, exist_ok=True)
                    path_to_file = os.path.join(params['local_untarred'], fileUrl)
                    #writes/downloads file to explicit path
                    with open(path_to_file, 'wb') as f:
                        f.write(r.content)
                else:
                    print(r.status_code)
        if params['center'] == "romsaf":
            with requests.session() as soup_session:
                with soup_session.get(repo_file_url, auth=('Amy McVey', '0Mjr2u'), stream=True) as r:
                    if r.status_code == 200:
                        print("Got file... ", repo_file_url)
                        #set and make local dir for download
                        local_dir = os.path.join(params['local_untarred'], fileUrl[:-4], '')
                        os.makedirs(local_dir, exist_ok=True)
                        path_to_file = os.path.join(params['local_untarred'], fileUrl)
                        #writes/downloads file to explicit path
                        with open(path_to_file, 'wb') as f:
                            f.write(r.content)
                    else:
                        print(r.status_code)

        print("Untarring file... ", path_to_file)

        #extract tarball
        tar = tarfile.open(path_to_file, "r:gz")
        tar.extractall(path=local_dir)
        tar.close()

        upload_to_s3(path_to_file, params['bucket_name'], f"{params['tar']}/{fileUrl}")

        print("Untarred to... ", local_dir)
        #get list of files from tarball
        local_file_list = os.listdir(local_dir)

        #romsaf has an extra folder level of /YYYY-MM-DD/
        if params['center'] == "romsaf":
            local_dir = os.path.join(local_dir,local_file_list[0])
            local_file_list = os.listdir(local_dir)

        untarred_path_list = [os.path.join(local_dir, filename) for filename in local_file_list]

    return untarred_path_list

def webscrape_input_driver(input_files, params):

    print("downloading and untarring...")
    local_file_path_list = download_and_untar(input_files, params)
    s3_key_list = []
    for file_to_upload in local_file_path_list:
        #bad romsaf files
        if "non-nominal" in file_to_upload: continue
        #tar file already uploaded to s3
        if "tar.gz" in file_to_upload or ".tgz" in file_to_upload: continue

        if params['center'] == "romsaf":
            root_folder = file_to_upload.split('/')[0]
            mission = file_to_upload.split('/')[1]
            year = file_to_upload.split('/')[3]
            ro_folder = file_to_upload.split('/')[4]
            date_folder = file_to_upload.split('/')[5]
            file_name = os.path.basename(file_to_upload)
            #untarred fileformat is not the same as what dynamointerface is looking for, so we change it.
            s3_key = os.path.join(root_folder, mission, year, ro_folder, date_folder, file_name)
        else:
            s3_key = file_to_upload

        upload_to_s3(file_to_upload, params['bucket_name'], s3_key)
        #print("uploaded file: ", s3_key)
        s3_key_list.append(s3_key)

    print("upload complete to ", params['bucket_name'])

    return s3_key_list

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Take input from lambda and create input json file for batchprocess")
    parser.add_argument("tarfile", type=str, help="Ucar file url string sent from lambda, 1")

    parser.add_argument( "--romsaf", dest='romsaf', action='store_true', help="run webscrape process for romsaf" )
    args = parser.parse_args()

    AWSversion = "1.1"

    main(args.tarfile, AWSversion, args.romsaf)
