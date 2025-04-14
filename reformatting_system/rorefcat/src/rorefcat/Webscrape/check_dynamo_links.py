import os
import datetime
import numpy as np
import re
import s3fs
import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from botocore import UNSIGNED
import time

AWSregion = "us-east-1"
valid_file_types = [ "ucar_calibratedPhase", "ucar_refractivityRetrieval", "ucar_atmosphericRetrieval", \
                    "romsaf_refractivityRetrieval", "romsaf_atmosphericRetrieval", \
                    "jpl_calibratedPhase", "jpl_refractivityRetrieval", "jpl_atmosphericRetrieval", \
                    "eumetsat_calibratedPhase" \
                ]

#session = boto3.Session( profile_name = "nasa", region_name=AWSregion)
#s3 = s3fs.S3FileSystem( client_kwargs={ 'region_name': AWSregion }, profile = "nasa")
session = boto3.Session( region_name=AWSregion)
s3 = s3fs.S3FileSystem( client_kwargs={ 'region_name': AWSregion })
bucket = "gnss-ro-data-staging"
dTable = "gnss-ro-data-stagingv1_1"

#s3 = s3fs.S3FileSystem( client_kwargs={ 'region_name': AWSregion }, config_kwargs={ 'signature_version': UNSIGNED } )
#bucket = "gnss-ro-data"

s3boto = session.resource('s3')
s3client = session.client('s3')

def putItem(item):
    dynamodb_resource = session.resource( "dynamodb" )
    table = dynamodb_resource.Table( dTable )

    #Put Item:
    table.put_item(
        Item = item
    )

def queryItem(partitionKey,sortKey):
    dynamodb_resource = session.resource( "dynamodb" )
    table = dynamodb_resource.Table( dTable )

    filler ={}
    response = table.query(
        TableName = dTable,
        KeyConditionExpression = Key('leo-ttt').eq(partitionKey) & Key('date-time').eq(sortKey)
    )

    if len(response['Items']) != 0:
        return response['Items'][0]
    else:
        return filler

def updateTable_rm(partitionKey, sortKey, type):
    dynamodb_resource = session.resource( "dynamodb" )
    table = dynamodb_resource.Table( dTable )

    table.update_item(
        Key={'leo-ttt': partitionKey, 'date-time': sortKey},
                        UpdateExpression=f"REMOVE {type}"
    )

    print(f"Removed {type} from {partitionKey} {sortKey}")

def deleteItem(partitionKey, sortKey):
    dynamodb_resource = session.resource( "dynamodb" )
    table = dynamodb_resource.Table( dTable )

    table.delete_item(
        Key={'leo-ttt': partitionKey, 'date-time': sortKey}
    )

    #print(f"Removed {partitionKey} {sortKey}")

def read_json(mission,year):
    #list all json files to search through
    initial_file_array = s3.ls( os.path.join( bucket, f'dynamo/v1.1/export_subsets' ) )

    final_array = [file for file in initial_file_array if mission in file and year in file]
    #print("number of files:", len(final_array))

    return final_array

def main(mission,dstr):
    '''Checks dynamo table items to ensure any open data linked files
    actually exist'''
    print(f"checking {mission} for {dstr}")

    file_array = read_json(mission,dstr)

    for file in file_array:
        # open each file for said mission
        with s3.open(file, 'r') as f:
            df_dict = json.loads( f.readline() )
        df = list( df_dict.values() )
        print("number of items in file:", len(df))
        # loop through items in file and check attr and files
        for each in df:
            # make sure we check each center_filetype combo
            for type in valid_file_types:
                if type in each.keys():
                    try:
                        #load the NASA account one in case it wasn't sync'd
                        s3client.get_object(Bucket = bucket, Key=each[type])
                    except:
                        #print("can't find: ", each[type])
                        updateTable_rm(f'{each["receiver"]}-{each["transmitter"]}', each['date-time'], type)

if __name__ == "__main__":

    main("cosmic1","2008-12-01")

