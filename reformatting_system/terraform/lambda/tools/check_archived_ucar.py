import sys, os
import json
from datetime import datetime

import requests
import boto3
from bs4 import BeautifulSoup

#create s3 boto3 object and session
session = boto3.Session( profile_name = "aernasaprod", region_name= "us-east-1")

s3_client = session.client('s3')
s3_resource = session.resource('s3')

liveupdate_bucket_name = 'ucar-earth-ro-archive-liveupdate'
staging_bucket_name = "gnss-ro-data-staging"
archive_bucket_name = "ucar-earth-ro-archive-untarred"

def s3_get_key(key):
    gz_file_list = []
    local_file = os.path.basename(key)
    s3_client.download_file(liveupdate_bucket_name, key, local_file)

    with open(local_file, 'r') as file:
        lines = file.readlines()

        for l in lines:
            if "tar.gz" in l:
                gz_file_list.append(l.strip())

    return gz_file_list

def check_prefix(key):
    response = s3_client.list_objects(
        Bucket = archive_bucket_name,
        Prefix = key
    )
    if "Contents" in response.keys():
        return len(response['Contents'])
    else:
        return 0

def check_gz_list(gz_file_list):
    print(len(gz_file_list))
    final_list = []
    for gz in gz_file_list:
        file_name = os.path.basename(gz)
        jd = str(file_name.split('.')[0][-3:])

        key = os.path.join(gz.split('/')[0],gz.split('/')[1],gz.split('/')[2],gz.split('/')[3],jd,"")
        if "level1a" in key: continue
        count = check_prefix(key)
        #print(key, count)

        if count < 100:
            print(key, count)
        else:
            final_list.append(os.path.join(key,file_name))

    return final_list

def create_liveupdate_tar_list():
    #scrape nasa s3 liveupdate bucket for tarballs
    obj_list = []
    livestream_scrape = s3_resource.Bucket(liveupdate_bucket_name)

    #check to make sure objects are in the right folder with the right suffix.
    for bucket_object in livestream_scrape.objects.filter( Prefix=f"tarballs/" ):
        if "cosmic2" in bucket_object.key: continue
        if "spire" in bucket_object.key: continue
        if "tar.gz" in bucket_object.key:
            obj_list.append(bucket_object.key.split('tarballs/')[1])

    with open("ucar_objKey_liveupdate.txt",'w') as file:
        for each in obj_list:
            file.write(each+"\n")

def clean_liveupdate_tar():
    local_file = "ucar_objKey_liveupdate.txt"

    with open(local_file, 'r') as file:
        lines = file.readlines()

        for l in lines:
            key = "tarballs/"+l.strip()
            print(key)
            try:
                s3_client.delete_object(Bucket = liveupdate_bucket_name, Key = key)
            except:
                print("already gone")

def read_ucar_scrape():
    scrape_dict = {}
    with open("ucar_scrape.json", 'r') as file:
        lines = file.read()
        scrape_dict  = json.loads(lines)

    for f in scrape_dict['cosmic1']['files']:
        if "repro2013/level2/2012/0" in f:
            print(f)

if __name__ == "__main__":

    #create_liveupdate_tar_list()
    #clean_liveupdate_tar()
    read_ucar_scrape()
    quit()

    gz_file_list = []
    final_list = []

    gz_file_list = s3_get_key("ucar_objKey.txt")
    '''
    sub_list = []
    for gz in gz_file_list:
        if "cosmic1/repro2013/level1b/2012" in gz:
            sub_list.append(gz)
    '''
    final_list = check_gz_list(gz_file_list)

    with open("ucar_objKey_new.txt",'w') as file:
        for each in final_list:
            file.write(each+"\n")
