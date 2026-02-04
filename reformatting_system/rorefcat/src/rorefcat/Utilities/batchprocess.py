#!/usr/bin/env python3

import os
import sys
import json
import boto3
import re

from ..Reformatters import valid_processing_centers, reformatters, varnames
from ..Versions import get_version, valid_versions
from ..Database.dynamodbinterface import ProcessReformat
from ..Utilities.resources import AWSregion

#  Set defaults.

default_AWSversion = "1.1"
valid_file_types = { center: [ k for k in ref.keys() if re.search( r'^level\d', k ) ] for center, ref in reformatters.items() }


#  Logger.

import logging

LOGGER = logging.getLogger(__name__)

def batchprocess( jsonfile, version, session=None, workingdir=None, clobber=False ):
    """This function will translate all files contained in the jsonfile.
    Appropriate entries and information will be entered into the
    DynamoDB table dbtable. Output files will be written into an
    output path with prefix output_prefix. The output prefix can be an
    S3 bucket, in which case it should lead with "s3://". If AWS
    authentication is required, then generate a boto3 session object
    and provide it as session. A working directory is needed
    as an area where files can be downloaded from S3, output files
    created for subsequent upload to S3. It can grow large, so be sure
    to purge it periodically. If previously existing output files
    already exist and/or information in the database already exists
    and you wish to clobber/overwrite, set clobber to True. Otherwise,
    no existing output file or already existing information in the
    database will be overwritten.

    The contents of the jsonfile should be

    (1) scalar variable "InputPrefix". Its value should be the prefix
        indicating where to find the elements of the list InputFiles.
        The prefix can lead with "s3://", in which case the input files
        reside in an S3 bucket. Full paths of the input files are defined
        by joining the prefix with an element of the InputFiles.
    (2) scalar variable "ProcessingCenter". Its value should be one of
        the valid processing centers that contribute to the NASA ACCESS
        project.
    (3) list of strings "InputFiles". Each elements of the list should
        contain information on the mission, receiving satellite,
        transmitting satellite, and time of the RO sounding. Generally,
        should contain a directory hierarchy, because that directories
        contained in the path define some of these variables."""

    #  Version settings.

    dbtable = version['module'].dynamodbTable
    output_prefix = "s3://" + version['module'].stagingBucket

    #  Read the json file.

    input_prefix, processing_center, input_files = None, None, None

    if jsonfile[:5] == "s3://":

        #  Download from S3 bucket.

        s3 = session.client( "s3" )
        file_split = re.split( "/", jsonfile[5:] )
        bucketName, bucketPath, localFile = file_split[0], "/".join( file_split[1:] ), file_split[-1]
        s3.download_file( bucketName, bucketPath, localFile )

        #  Read JSON.

        with open( localFile, 'r' ) as fs:
            js = json.load( fs )

        #  Remove downloaded file.

        os.unlink( localFile )

    else:

        #  Read JSON.

        with open( jsonfile, 'r' ) as fs:
            js = json.load( fs )

    input_prefix = js['InputPrefix']
    processing_center = js['ProcessingCenter']
    input_files = js['InputFiles']

    #  Check for valid arguments.

    if input_prefix is None:
        LOGGER.error( "No input prefix given" )
        return

    if processing_center not in valid_processing_centers:
        LOGGER.error( f"{processing_center} is not a valid processing center." )
        return

    if input_files is None:
        LOGGER.error( "No input files given" )
        return

    #  Set up translators.

    process = {}
    for file_type in valid_file_types[processing_center]:
        process.update( { file_type: ProcessReformat(
                file_type, processing_center, dbtable, version, session=session,
                workingdir=workingdir ) } )

    #  Loop over input files.

    for input_file in input_files:

        LOGGER.info( f"Processing {input_file}" )

        #  What is the input file type?

        ret_varnames = varnames[processing_center]( input_file )
        if ret_varnames['status'] == "fail":
#           LOGGER.error( f"Cannot get varnames from {input_file}" )
            continue

        #  Define file_type.

        if ret_varnames['input_file_type'] in valid_file_types[processing_center]:
            file_type = ret_varnames['input_file_type']

        elif ret_varnames['input_file_type'] in [ "atmPhs", "conPhs", "calibratedPhase", "1B" ]:
            file_type = "level1b"

        elif ret_varnames['input_file_type'] in [ "atmPrf", "atm", "refractivityRetrieval" ]:
            file_type = "level2a"

        elif ret_varnames['input_file_type'] in [ "wetPrf", "wetPf2", "wet", "atmosphericRetrieval" ]:
            file_type = "level2b"

        #  Define the job_dict.

        job_dict = { 'processing_center': processing_center,
            'file_type': file_type, 'input_prefix': input_prefix,
            'input_file': input_file }

        #  Process according to file type.

        try:

            ret_process = process[file_type]( input_prefix, input_file, output_prefix, clobber=clobber )

            if ret_process['status'] == "fail":
                info = json.dumps( { 'status': "fail", 'job': job_dict, 'messages': ret_process['messages'] } )
                if "BadRetrieval" in ret_process['messages']: 
                    # LOGGER.warning( "Results: " + info )
                    pass
                else: 
                    LOGGER.error( "Results: " + info )

            elif ret_process['status'] == "success":
                info = json.dumps( { 'status': "success", 'job': job_dict, 'messages': ret_process['messages'] } )
                LOGGER.info( "Results: " + info )

        except Exception as excpt:

            info = json.dumps( { 'status': "exception", 'job': job_dict } )
            LOGGER.exception( "Results: " + info )
            LOGGER.exception( json.dumps( excpt.args ) )



#  Main program.

def main(): 

    #  Argument parser.

    import argparse

    parser = argparse.ArgumentParser( description="Process a set of RO sounding input files contributed " +
            "by a processing center as provided in a JSON input file. The files will be translated into " +
            "AWS RO data formats and logged into a DynamoDB table." )

    parser.add_argument( "jsonfile", type=str, help='Path to a JSON file that contains a list of input files ' +
            'to be translated and logged. Three variables are necessary. "ProcessingCenter" specifies the ' +
            'processing center that provided the data. Valid processing centers are ' +
            ', '.join( valid_processing_centers ) + '. "InputPrefix" is a string that defines ' +
            'the root path of the input data. When led by "s3://", it points to an input S3 bucket. This prefix ' +
            'is prepended to each of the elements in the input file list. "InputFiles" should point to a list ' +
            'of strings, each element of which is the relative path to an input file. Each element can and ' +
            'should contain a directory hierarchy that provides information on the RO mission, receiver, ' +
            'transmitter, and time. The input prefix and each element in the input list are joined to form ' +
            'an absolute path to the input file.' )

    parser.add_argument( "--version", dest='AWSversion', type=str, default=default_AWSversion,
            help=f'The output format version. The default is AWS version "{default_AWSversion}". ' + \
                    'The valid versions are ' + ', '.join( [ f'"{v}"' for v in valid_versions ] ) + "." )

    parser.add_argument( "--workingdir", dest='workingdir', type=str, default="workingdir",
            help='Path to a working directory. The working directory will be used when reading input or ' +
            'writing output to an S3 bucket. Temporary files are not removed, so be sure to purge the ' +
            'working directory periodically.' )

    parser.add_argument( "--clobber", dest='clobber', action='store_true',
            help='This keyword mandates that information in the database and output files be ' +
            'clobbered/overwritten when processing the input files. By default, no overwriting ' +
            'takes place.' )

    parser.add_argument( "--verbose", dest='verbose', action='store_true',
            help='If verbose is set, then information is added to the logging output to ' +
                        'standard output. By default, only warnings, errors, and exceptions ' +
                        'are written to standard output.' )

    #  Process the command line.

    args = parser.parse_args()

    #  Get version module.

    version = get_version( args.AWSversion )
    if version is None:
        print( f'AWS version "{args.AWSversion}" is unrecognized.' )
        exit( -1 )

    definitionsBucket = version['module'].definitionsBucket
    dynamodbTable = version['module'].dynamodbTable
    loggingRoot = version['module'].loggingRoot
    output_prefix = "s3://" + version['module'].stagingBucket

    #  Log to local file and to stdout.

    json_base = os.path.basename(args.jsonfile)
    error_logging_file = json_base[:-5] + ".errors.log"
    warning_logging_file = json_base[:-5] + ".warnings.log"

    handlers = []
    formatter = logging.Formatter('%(pathname)s:%(lineno)d %(levelname)s: %(message)s')

    e_filehandle = logging.FileHandler( filename=error_logging_file )
    e_filehandle.setLevel( "ERROR" )
    e_filehandle.setFormatter( formatter )
    handlers.append(e_filehandle)
    #handlers.append( logging.FileHandler( filename=error_logging_file ).setLevel( "ERROR" ) )
    print( f"Logging errors and exceptions to {error_logging_file}." )

    w_filehandle = logging.FileHandler( filename=warning_logging_file )
    w_filehandle.setLevel( "WARNING" )
    w_filehandle.setFormatter( formatter )
    handlers.append(w_filehandle)
    #handlers.append( logging.FileHandler( filename=warning_logging_file ).setLevel( "WARNING" ).setFormatter( formatter ) )
    print( f"Logging warnings, errors and exceptions to {warning_logging_file}." )

    if args.verbose:
        i_streamhandle = logging.StreamHandler( sys.stdout )
        i_streamhandle.setLevel( "INFO" )
        i_streamhandle.setFormatter( formatter )
        handlers.append(i_streamhandle)
        #handlers.append( logging.StreamHandler( sys.stdout ).setLevel( "INFO" ).setFormatter( formatter ) )
        print( f"Logging information, warnings, errors and exceptions to standard output." )
    else:
        i_streamhandle = logging.StreamHandler( sys.stdout )
        i_streamhandle.setLevel( "WARNING" )
        i_streamhandle.setFormatter( formatter )
        handlers.append(i_streamhandle)
        #handlers.append( logging.StreamHandler( sys.stdout ).setLevel( "WARNING" ).setFormatter( formatter ) )
        print( f"Logging warnings, errors and exceptions to standard output." )

    for h in handlers:
        LOGGER.addHandler(h)


    #  Check the profile.

    session = boto3.session.Session( region_name=AWSregion )

    #  Execute.

    LOGGER.info( f'Writing to DynamoDB table "{dynamodbTable}".' )
    LOGGER.info( f'Writing reformatted data files into "{output_prefix}".' )

    batchprocess( args.jsonfile, version, session=session,
        workingdir=args.workingdir, clobber=args.clobber )

    #  Upload log files if they have content.

    if os.path.getsize(error_logging_file) > 0 :
        s3 = session.client( service_name='s3', region_name=AWSregion )
        upload_path = os.path.join( loggingRoot, "errors", error_logging_file )
        print( 'Uploading error log to "s3://{:}".'.format( os.path.join( definitionsBucket, upload_path ) ) )
        s3.upload_file( error_logging_file, definitionsBucket, upload_path )

    if os.path.getsize(warning_logging_file) > 0 :
        s3 = session.client( service_name='s3', region_name=AWSregion )
        upload_path = os.path.join( loggingRoot, "warnings", warning_logging_file )
        print( 'Uploading warning log to "s3://{:}".'.format( os.path.join( definitionsBucket, upload_path ) ) )
        s3.upload_file( warning_logging_file, definitionsBucket, upload_path )

    #  Done.

    exit(0)


if __name__ == "__main__":
    main()
    pass

