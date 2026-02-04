#!/usr/bin/env python3

import os
import sys
import json
import boto3
import re
import subprocess

from ..Webscrape import convert_json as convert
from ..Webscrape import export_dynamodb as export
from ..Webscrape import webscrape as webscrape
from ..Webscrape import check_dynamo_links as dynamo_check
from ..Versions import get_version
from ..Missions import valid_missions

#  Main program.

def main(): 

    #  Argument parser.

    import argparse

    parser = argparse.ArgumentParser( description="run webscrape, sync to open " +
                    "data, or export dynamodb and process" )

    # mandatory args
    parser.add_argument( "runMode", type=str, help='webscrape,sync,export,convert,check_Dlinks')

    parser.add_argument( "AWSversion", type=str,
            help=f'The AWS version for processing. i.e. "1.1" or "2.0" ' )

    # webscrape args
    parser.add_argument("--tarfile", dest='tarfile', type=str, default=None, help="Ucar file url string sent from lambda")
    parser.add_argument("--romsaf", dest='romsaf', action='store_true', help="run webscrape untar for romsaf otherwise ucar" )

    # sync
    parser.add_argument("--prefix", dest='prefix', type=str, default=None, \
                        help="prefix to sync to open bucket, or for eumetsat ftp download")

    # export arg
    parser.add_argument("--manifest_file_s3_path", dest="manifest_file_s3_path", type=str, default=None, \
                        help="s3 uri for the manifest.json file from the dynamo export")

    # export, convert, check_dynamo
    parser.add_argument("--mission", dest="mission", type=str, default=None, \
                        help="a single valid mission")

    # check_dynamo
    parser.add_argument("--datestr", dest="datestr", type=str, default=None, \
                        help="a date string like 2024-04-03")

    # local Workspace testing
    parser.add_argument("--auth", dest="auth", action='store_true', help="use local credentials")

    #  Process the command line.

    args = parser.parse_args()

    #  Get version module.

    TEST = os.getenv( "TEST" )

    version = get_version( args.AWSversion )#must be "1.1" or "2.0"
    if version is None:
        print( f'AWS version "{args.AWSversion}" is unrecognized.' )
        exit( -1 )

    if args.runMode == "webscrape":
        try:
            webscrape.main(args.tarfile, args.AWSversion, args.romsaf)
        except Exception as e:
            print(args.runMode,e)
            sys.exit(1)

    if args.runMode == "sync":
        '''
        this all is used in batch to sync the staging bucket with the open data bucket
        it's much faster run from the nasa account and not the Prod workspace
        also can use mulitple calls to aws sync this way
        '''
        if args.auth:
            command = f"aws s3 sync s3://gnss-ro-data-staging/{args.prefix}/ s3://gnss-ro-data/{args.prefix}/ --acl bucket-owner-full-control --profile {AWSversion}"
        else:
            command = f"aws s3 sync s3://gnss-ro-data-staging/{args.prefix}/ s3://gnss-ro-data/{args.prefix}/ --acl bucket-owner-full-control"

        if TEST is None:
            print(command)
            try:
                subprocess.run(command,shell=True, capture_output=False)
            except Exception as e:
                print(args.runMode,e)
                sys.exit(1)
        else:
            print('test',command)

    if args.runMode == "export":

        try:
            export.main(version,args.manifest_file_s3_path,args.mission,valid_missions)
        except Exception as e:
            print(args.runMode,e)
            sys.exit(1)

    if args.runMode == "convert":
        convert.main(version,args.mission)
        try:
            convert.main(version,args.mission)
        except Exception as e:
            print("fail",args.runMode,e)
            sys.exit(1)

    if args.runMode == "check_Dlinks":
        try:
            dynamo_check.main(args.mission, args.datestr)
        except Exception as e:
            print(args.runMode,e)
            sys.exit(1)


if __name__ == "__main__":
    main()
    pass

