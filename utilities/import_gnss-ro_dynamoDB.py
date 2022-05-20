"""import_gnss-ro_dynamoDB.py:

Purpose: This script will create a dynamoDB table of the availalbe GNSS RO files
    within the search parameters provided (see Example Execution below).

Setting up AWS Credentials:
    You need to set up credentials in order to create a dynamoDB table in your aws account.
    First create your access keys in the AWS console if you haven't already
    https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys
    Then follow the directions here to be able to access your AWS resources progromatically
    https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_profiles.html
    Create a profile named default.

The prerequisite nonstandard python modules that must be installed are
  * boto3
  * pandas

Computer Requirements:
    at least 8 GB of RAM

Example Execution:
to get all cosmic1 files for Feburary 2008
>>python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name gnss-ro-import-table --mission cosmic1 --date_str "2008-02"

to get all champ files and create a table with the default name
>>python3 import_gnss-ro_dynamoDB.py --mission champ

to get all 2020 files
>>python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name gnss-ro-import-table --date_str "2020"

to get all metop files for 2015
>>python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name gnss-ro-import-table --mission metop --date_str "2015"

to run the data_analysis_demonstration.py first run:
>>python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name gnss-ro-import-table --mission cosmic1 --date_str "2009-01-04"

to import the full catalog table NOTE: this may take a very long time (not recommended, we are working on a faster way)
>>python3 import_gnss-ro_dynamoDB.py --full

Version: 1.0
Author: Amy McVey (amcvey@aer.com)
Date: May 17, 2022
Python Version: 3.8

"""
##################################################
#  IMPORTANT: Configuration
##################################################

#  Define the name of the AWS profile to be used for authentication
#  purposes. The authentication will be needed to access the DynamoDB
#  database table.

aws_profile = "aernasaprod"

#  Define the AWS region where the gnss-ro-data Open Data Registry
#  S3 bucket is hosted *and* where the DynamoDB database is manifested.

aws_region = "us-east-1"


##################################################
#  Configuration complete.

# DO NOT CHANGE BELOW THIS LINE
##################################################

#  Import python standard modules.

import os
import sys
import datetime
import time
import argparse

#  Import installed modules.

import boto3
import pandas as pd

# Initiate Variables
valid_missions = ['gpsmet', 'grace', 'sacc', 'champ', 'cosmic1', 'tsx', 'tdx', 'snofs', 'metop', 'kompsat5', 'paz', 'cosmic2']
local_file_list = []
obj_key_list = []

#  AWS access. Be sure to establish authentication for profile aws_profile
#  for successful use.
session = boto3.Session(profile_name=aws_profile, region_name = aws_region)

#  S3 object and resource
s3_client = session.client('s3')

s3 = session.resource( "s3" )
bucket = s3.Bucket( "gnss-ro-data" )

#  DynamoDB table object.

db_client = session.client('dynamodb')


################################################################################
#  Methods.
################################################################################

def search_json_subsets(local_file_list, obj_key_list, date_str):
    """Filters json files which were subst from the gnss-ro-data dynamoDB table
    export based on the input parameters date or mission """

    item_import_list = []
    for i in range(0, len(local_file_list)):
        #download_s3 file
        s3_client.download_file( "gnss-ro-data", obj_key_list[i], local_file_list[i])

        df = pd.read_json(local_file_list[i], lines=True)
        #df.info(memory_usage=True)

        for item in df["Item"]:
            #if "2009-01-01" in item["date-time"]
            #if "cosmic1" in item["mission"]
            if date_str:
                if date_str in item["date-time"]['S']:
                    item_import_list.append(item)
            else:
                item_import_list.append(item)

    return item_import_list

def import_array(item_import_list,table_name):
    """Imports the array of jason objects in batch format to the specified
    dynamoDB table"""

    batch_write_count = int(len(item_import_list)/25)+1
    for c in range(0,batch_write_count):
        item_list = []
        for i in range(0,25): #batch_write can only do up to 25 puts in a single call
            Dict = {}
            Dict["PutRequest"] = {}
            true_index = (25*c)+i
            if true_index < len(item_import_list):
                Dict["PutRequest"]["Item"] = item_import_list[true_index]
                item_list.append(Dict)
        if len(item_list)> 0:
            response = db_client.batch_write_item(
                RequestItems={ table_name: item_list })
            time.sleep(1)

def create_dynamo(new_dynamo_table_name):
    """Creates a dynamoDB table to import gnss-ro-data catalog itmes with the
    name provided if applicaple or the default name"""

    response = db_client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'leo-ttt',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'date-time',
                'AttributeType': 'S'
            },
        ],
        TableName = new_dynamo_table_name,
        KeySchema=[
            {
                'AttributeName': 'leo-ttt',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'date-time',
                'KeyType': 'RANGE'
            },
        ],
        BillingMode='PAY_PER_REQUEST',
    )
    time.sleep(10)

#  Main program.

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Take input from lambda and create input json file for batchprocess")
    parser.add_argument("--dynamodb_table_name", dest="dynamodb_table_name", type=str, default="gnss-ro-import" )
    parser.add_argument("--mission", dest="mission", type=str, default=None ) #cosmic1
    parser.add_argument("--date_str", dest="date_str", type=str, default=None ) #"2009-01-01" or "2009"
    parser.add_argument("--full", dest="full", action="store_true" )

    args = parser.parse_args()

    #check for valid missions
    if args.mission and args.mission not in valid_missions:
        print("please provide a valid misssion", valid_missions)
        sys.exit(0)

    if args.date_str:
        year = args.date_str[:4]
    else:
        year = "None"

    #  Iterate over subset json files
    for obj in bucket.objects.filter( Prefix=f"dynamo_export_subsets/" ):
        if args.mission and args.mission in obj.key and args.date_str and year in obj.key:
            local_file_list.append(os.path.basename(obj.key))
            obj_key_list.append(obj.key)
        elif args.mission and args.mission in obj.key and not args.date_str:
            local_file_list.append(os.path.basename(obj.key))
            obj_key_list.append(obj.key)
        elif args.date_str and year in obj.key and not args.mission:
            local_file_list.append(os.path.basename(obj.key))
            obj_key_list.append(obj.key)
        elif args.full:
            local_file_list.append(os.path.basename(obj.key))
            obj_key_list.append(obj.key)
            year = "all"

    if len(local_file_list) == 0:
        print("no files fit the parameters provided, check your date string YYYY or YYYY-MM")
        sys.exit(0)
    else:
        print("files to search",len(local_file_list))

    #create new dynamo table bofore import
    response = db_client.list_tables()
    if args.dynamodb_table_name not in response['TableNames']:
        create_dynamo(args.dynamodb_table_name)
        print(f"creating dynamodb table: {args.dynamodb_table_name}")
    else:
        print("table exists, continuing with import")

    if args.full:
        #import whole table
        print(f"Looping through all subset json files for FULL database import\n")
        for file in local_file_list:
            item_list = search_json_subsets([file], obj_key_list, args.date_str)
            min_to_finish = round(len(item_list)/23.5/60,1)
            print(f"items to import from {file}: {len(item_list)} (This may take {min_to_finish} minutes.)")
            import_array(item_list,args.dynamodb_table_name)
    else:
        item_list = search_json_subsets(local_file_list, obj_key_list, args.date_str)
        min_to_finish = round(len(item_list)/23.5/60,1)
        print(f"items to import: {len(item_list)} (This may take {min_to_finish} minutes.)")
        import_array(item_list,args.dynamodb_table_name)

    pass
