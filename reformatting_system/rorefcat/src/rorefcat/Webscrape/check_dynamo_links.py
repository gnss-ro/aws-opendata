import os
import sys
import argparse 
import datetime
import numpy as np
import re
import s3fs
import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore import UNSIGNED
import time
from ..Reformatters import reformatters
from ..Versions import get_version, valid_versions

AWSregion = "us-east-1"
default_AWSversion = "1.1"


#  Construct session and s3fs instance. 

session = boto3.Session( region_name=AWSregion)
s3 = s3fs.S3FileSystem( client_kwargs={ 'region_name': AWSregion })

s3boto = session.resource('s3')
s3client = session.client('s3')


def putItem(item, table):

    #Put Item:
    table.put_item(
        Item = item
    )

def queryItem(partitionKey,sortKey,table):

    filler = {}
    response = table.query(
        # TableName = dTable,
        KeyConditionExpression = Key('leo-ttt').eq(partitionKey) & Key('date-time').eq(sortKey)
    )

    if len(response['Items']) != 0:
        return response['Items'][0]
    else:
        return filler

def updateTable_rm(partitionKey, sortKey, type, table):

    table.update_item(
        Key={'leo-ttt': partitionKey, 'date-time': sortKey},
                        UpdateExpression=f"REMOVE {type}"
    )

    print(f"Removed {type} from {partitionKey} {sortKey}")
    sys.stdout.flush()

def deleteItem(partitionKey, sortKey, table):

    table.delete_item(
        Key={'leo-ttt': partitionKey, 'date-time': sortKey}
    )

    # print(f"Removed {partitionKey} {sortKey}")
    # sys.stdout.flush() 

def read_json(mission,year,version):
    #list all json files to search through
    initial_file_array = s3.ls( os.path.join( version['module'].stagingBucket, f'dynamo/v1.1/export_subsets' ) )

    final_array = [file for file in initial_file_array if mission in file and year in file]
    # print("number of files:", len(final_array))
    # sys.stdout.flush() 

    return final_array

def process( mission, daterange, version ):
    '''Checks dynamo table items to ensure any open data linked files
    actually exist'''
    # print(f"checking {mission} for {dstr}")
    # sys.stdout.flush()
    if isinstance(daterange,str): 
        drange = [ daterange, daterange ]
    else: 
        drange = list( daterange )

    #  Define available file types. 

    file_indexing = version['module'].file_indexing
    valid_file_types = []
    for center, refs in reformatters.items(): 
        valid_file_types += [ f'{center}_{file_indexing[ft]}' for ft in refs.keys() if re.search(r'^level[0123][abc]$',ft) ]

    #  Define database table. 

    dynamodb_resource = session.resource( "dynamodb" )
    table = dynamodb_resource.Table( version['module'].dynamodbTable )

    #  Time bracket. 

    first_day = datetime.datetime.fromisoformat( drange[0] )
    last_day = datetime.datetime.fromisoformat( drange[1] )
    day = first_day + datetime.timedelta(days=0)

    while day <= last_day: 

        dstr = day.strftime( "%Y-%m-%d" )
        file_array = read_json(mission,dstr,version)

        for file in file_array:
            # open each file for said mission
            with s3.open(file, 'r') as f:
                df_dict = json.loads( f.readline() )
            df = list( df_dict.values() )
            print( f"Number of items in file {file} = {len(df)}" )
            sys.stdout.flush() 
            # loop through items in file and check attr and files
            for each in df:
                # make sure we check each center_filetype combo
                for type in valid_file_types:
                    if type in each.keys():
                        ret = s3client.list_objects_v2( Bucket=version['module'].stagingBucket, Prefix=each[type] )
                        if ret['KeyCount'] == 0: 
                            # print("can't find: ", each[type])
                            updateTable_rm(f'{each["receiver"]}-{each["transmitter"]}', each['date-time'], type, table )

        day = day + datetime.timedelta(days=1)


def main(): 

    parser = argparse.ArgumentParser( description="Edit the DynamoDB database for missing links (references to data files that don't exist)" )
        
    parser.add_argument( "mission", type=str, help="Name of the mission whose metadata should be edited." )

    parser.add_argument( "daterange", type=str, help="""Either the date or the range of dates over which metadata should be edited.
        If a single date, it should be have format "YYYY-MM-DD". If a range of dates, it should be a colon-separated string 
        containing two dates that define the (inclusive) range: "YYYY-MM-DD:YYYY-MM-DD".""" )

    parser.add_argument( "--version", dest='AWSversion', type=str, default=default_AWSversion,
            help=f'The output format version. The default is AWS version "{default_AWSversion}". ' + \
                    'The valid versions are ' + ', '.join( [ f'"{v}"' for v in valid_versions ] ) + "." )

    args = parser.parse_args()

    #  Handle the version. 

    version = get_version( args.AWSversion )
    if version is None:
        print( f'AWS version "{args.AWSversion}" is unrecognized.' )
        exit( -1 )

    #  Parse the date range. 

    m1 = re.search( r'^(\d{4}-\d{2}-\d{2}):(\d{4}-\d{2}-\d{2})$', args.daterange )
    m2 = re.search( r'^\d{4}-\d{2}-\d{2}$', args.daterange )

    if m1: 
        daterange = [ m1.group(1), m1.group(2) ]
    elif m2: 
        daterange = str( args.daterange )
    else: 
        print( """Bad daterange format. Must be either "YYYY-MM-DD" or "YYYY-MM-DD:YYYY-MM-DD".""" )
        sys.stdout.flush()

    process( args.mission, daterange, version )
    pass


if __name__ == "__main__":
    main()


