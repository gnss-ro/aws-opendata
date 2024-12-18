"""
module: definejobs.py
version: 2.0
author: Stephen Leroy (sleroy@aer.com)
date: February 10, 2023

Purpose:
Provide a set of tools that defines processing center RO data preprocessing
jobs. The two main tools are "definejobs" and "createjobdefinitionsjson",
the latter being a wrapper for the former. See inline doc strings for usage.
"""

#  Imports.

import os, re, copy
from datetime import datetime, timedelta
import s3fs
import boto3
import aiobotocore
import json

from ..Reformatters import reformatters
from ..Utilities import s3fsauth
from ..Utilities.dynamodbinterface import process_reformat_wrapper
from ..Missions import valid_missions, get_receiver_satellites

#  Logging.

import logging
LOGGER = logging.getLogger( __name__ )

#  Exception handling.

class Error( Exception ):
    pass

class definejobsError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment

class DASKjobsError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment


################################################################################
#  definejobs
################################################################################

def definejobs( daterange, missions, processing_centers, file_types, version,
        UCARprefix=None, ROMSAFprefix=None, JPLprefix=None, EUMETSATprefix=None,
        nonnominal=False, session=None, liveupdate=False ):
    """Generate a listing of jobs for translation for a given date range, lists of
    missions, processing centers and AWS file types. The date range is a two-element
    tuple or list of instance of datetime.datetime that defines an inclusive list of
    dates over which to scan the archives of data. The lists of processing_centers
    and file_types must be drawn from those defined for AWS in dynamodbinterface.py.

    The version must be a valid one, drawn from Versions.versions.

    The UCARprefix, ROMSAFprefix, JPLprefix, and EUMETSATprefix keys offer the option to override
    default paths regarding where to find UCAR, ROMSAF, JPL, and EUMETSAT input files. They
    can be found either on the local file system or in S3, in which case they
    should be prefixed with "s3://".

    If non-nominal occultation retrievals contributed by the ROM SAF should be
    included in the job definitions, set nonnominal to True.

    If AWS authentication is required in the current computing environment, it
    should be provided as a boto3 session. A logger generates output.

    Set liveupdate to True for liveupdate processing.

    The function returns a dictionary that completely describes where to find the
    input data files, subject to the function's restricting arguments, due for
    preprocessing. The returned dictionary contains two items:

        (1) "prefixes"
        (2) "jobs"

    The first of these keys, "prefixes", points to a dictionary that defines the
    prefixes of the input files. The keys of the "prefixes" dictionary are the
    requested processing_centers, and each contains a string that defines the input
    prefix for that processing center.  The second of these keys, "jobs", points
    to a list of dictionaries that define the directories containing the input
    files. Each dictionary in that list contains the following keys and contents:

        (a) date                A datetime.datetime instance of the date for RO data
        (b) mission             The RO mission (AWS name)
        (c) processing_center   The contributing processing center
        (d) file_type           The AWS RO file type ("level1b", "level2a", "level2b")
        (e) input_relative_dir  The directory in which the input files reside;
                                concatenate this with the absolute prefix of the
                                processing_center to establish an absolute path.
        (f) nfiles              The number of files in the directory that should
                                be preprocessed/translated.

    """

    #  Check input.

    for processing_center in processing_centers:
        if processing_center not in reformatters.keys():
            LOGGER.error(f'Processing center "{processing_center}" is not a valid processing center')
            #raise definejobsError( "InvalidInput", f'Processing center "{processing_center}" is not a valid processing center' )
            return 1

    for file_type in file_types:
        if file_type not in { f for center, reformatter in reformatters.items()
                    for f in reformatter.keys() }:
            LOGGER.error(f'File type "{file_type}" is not a valid file type')
            #raise definejobsError( "InvalidInput", f'File type "{file_type}" is not a valid file type' )
            return 1

    for mission in missions:
        if mission not in valid_missions['aws']:
            LOGGER.error(f'Mission "{mission}" is not a valid mission')
            #raise definejobsError( "InvalidInput", f'Mission "{mission}" is not a valid mission' )
            return 1

    #  Initialize s3fs instance.

    if session is None:
        create_session = True
    elif session.profile_name == "default":
        create_session = True
    else:
        create_session = False

    if create_session:
        s3 = s3fs.S3FileSystem( **( s3fsauth() ), client_kwargs={'region_name':session.region_name} )
    else:
        s3 = s3fs.S3FileSystem( profile=session.profile_name, client_kwargs={'region_name':session.region_name} )

    jobs = []

    #  Loop over date.

    date = datetime( year=daterange[0].year, month=daterange[0].month, day=daterange[0].day )

    while date <= daterange[1]:
        #  Get year, month, day, and day-of-year.

        year, month, day = date.year, date.month, date.day
        doy = ( date - datetime(year,1,1) ).days + 1
        print("line 148",processing_centers,valid_missions['aws'])
        for mission in missions:

            if "ucar" in processing_centers and mission in valid_missions['aws']:

                if UCARprefix is None:
                    if liveupdate:
                        UCARprefix = "s3://" + version['module'].ucar_liveupdateBucket + "/untarred"
                    else:
                        UCARprefix = "s3://" + version['module'].ucarBucket

                for file_type in file_types:

                    if file_type == "level1b":
                        level = "level1b"
                    elif file_type in [ "level2a", "level2b" ]:
                        level = "level2"

                    #  Loop over UCAR mission paths. AWS mission to UCAR mission is not always a
                    #  one-to-one mapping, and so UCARmissionMapping is consulted.

                    sats = get_receiver_satellites( "aws", mission=mission )
                    UCARmissions = sorted( list( { sat['ucar']['mission'] for sat in sats } ) )

                    for UCARmission in UCARmissions:

                        #  What processing versions are available?
                        try:
                            processingVersions = s3.ls( os.path.join( UCARprefix, UCARmission ) )
                        except:
                            LOGGER.error( "*** " + os.path.join( UCARprefix, UCARmission ) + " does not exist." )
                            continue

                        for processingVersion in processingVersions:
                            try:
                                subdirs = s3.ls( os.path.join( processingVersion, level, f"{year:4d}", f"{doy:03d}" ) )
                            except:
                                LOGGER.info( "*** s3://" + \
                                    os.path.join( processingVersion, level, f"{year:4d}", f"{doy:03d}" ) + \
                                    " does not exist." )
                                continue

                            type_subdirs = []

                            if file_type == "level1b":
                                for subdir in subdirs:
                                    head, tail = os.path.split( subdir )
                                    if re.search( r"^atmPhs", tail ) or re.search( r"^conPhs", tail ):
                                        type_subdirs.append( subdir )

                            elif file_type == "level2a":
                                for subdir in subdirs:
                                    head, tail = os.path.split( subdir )
                                    if re.search( r"^atmPrf", tail ):
                                        type_subdirs.append( subdir )

                            elif file_type == "level2b":
                                for subdir in subdirs:
                                    head, tail = os.path.split( subdir )
                                    if re.search( r"^wetPf2", tail ):
                                        type_subdirs.append( subdir )
                                    else:
                                        if re.search( r"^wetPrf", tail ) and len(type_subdirs) == 0:
                                            type_subdirs.append( subdir )

                            if len( type_subdirs ) != 1:
                                LOGGER.info("type_subdirs !=1")
                                continue

                            #  Get a list of all files for this day and file type.

                            dir_pattern = re.compile( UCARprefix[5:] + "/(.*)$" )
                            m = dir_pattern.search( type_subdirs[0] )
                            path = m.group(1)
                            try:
                                filepaths = s3.ls( type_subdirs[0] )
                            except:
                                LOGGER.info( "*** s3://" + type_subdirs[0] + " does not exist." )
                                continue

                            #  Define the new set of jobs.

                            job = { 'file_type': file_type, 'processing_center': "ucar",
                                    'mission': mission, 'date': date.strftime("%Y-%m-%d"),
                                    'input_relative_dir': path, 'nfiles': len(filepaths) }
                            jobs.append( job )

                            LOGGER.info( json.dumps( job ) )

                            break


            if "romsaf" in processing_centers and mission in valid_missions['aws']:

                if ROMSAFprefix is None:
                    if liveupdate:
                        ROMSAFprefix = "s3://" + version['module'].romsaf_liveupdateBucket + "/untarred"
                    else:
                        ROMSAFprefix = "s3://" + version['module'].romsafBucket

                sats = get_receiver_satellites( "aws", mission=mission )
                ROMSAFmissions = sorted( list( { sat['romsaf']['mission'] for sat in sats } ) )

                #  Loop over ROMSAF missions.

                for ROMSAFmission in ROMSAFmissions:

                    try:
                        if liveupdate:
                            subdirs = s3.ls( os.path.join( ROMSAFprefix, ROMSAFmission, f"{year:4d}" ) )
                        else:
                            subdirs = s3.ls( os.path.join( ROMSAFprefix, "romsaf", "download", ROMSAFmission,
                                    f"{year:4d}" ) )
                    except:
                        if liveupdate:
                            LOGGER.info( "*** " + \
                                    os.path.join( ROMSAFprefix, ROMSAFmission, f"{year:4d}" ) + \
                                    " does not exist." )
                        else:
                            LOGGER.info( "*** " + \
                                    os.path.join( ROMSAFprefix, "romsaf", "download", ROMSAFmission, f"{year:4d}" ) + \
                                    " does not exist." )

                    for file_type in file_types:

                    #  Select subdirectories corresponding to "atm" or to "wet" files.

                        if file_type == "level2a":
                            file_pattern = re.compile( f"^atm_{year:4d}{month:02d}{day:02d}" )
                        elif file_type == "level2b":
                            file_pattern = re.compile( f"^wet_{year:4d}{month:02d}{day:02d}" )
                        else:
                            continue

                        type_subdirs = []
                        for subdir in subdirs:
                            head, tail = os.path.split( subdir )
                            if file_pattern.search( tail ):
                                type_subdirs.append( subdir )

                        if len( type_subdirs ) != 1:
                            logger.info("type_subdirs != 1")
                            continue

                        #  Get a listing of all netcdf files for that mission/day.

                        fullpath = os.path.join( type_subdirs[0], f"{year:4d}-{month:02d}-{day:02d}" )
                        dir_pattern = re.compile( ROMSAFprefix[5:] + "/(.*)$" )
                        m = dir_pattern.search( fullpath )
                        path = m.group(1)

                        try:
                            paths = s3.ls( fullpath )
                        except:
                            LOGGER.info( "*** " + fullpath + " does not exist." )
                            continue

                        #  Define the new set of jobs.

                        filepaths = [ p for p in paths if re.search( r"[._]nc$", p ) ]
                        nfilepaths = len( filepaths )

                        if nfilepaths > 0:

                            job = { 'date': date.isoformat()[:10], 'mission': mission, 'processing_center': "romsaf",
                                'file_type': file_type, 'input_relative_dir': path, 'nfiles': nfilepaths }
                            jobs.append( job )

                            LOGGER.info( json.dumps( job ) )

                        else:

                            LOGGER.info( f"No files found in s3://{fullpath}" )

                        #  Check for non-nominal subdirectory.

                        if nonnominal:

                            fullpath = os.path.join( fullpath, "non-nominal" )
                            dir_pattern = re.compile( ROMSAFprefix + "/(.*)$" )
                            m = dir_pattern.search( fullpath )
                            path = m.group(1)

                            try:
                                paths = s3.ls( fullpath )
                            except Exception as excpt:
                                LOGGER.error( fullpath + " does not exist." )
                                LOGGER.exception( json.dumps( excpt.args ) )
                                continue

                            filepaths = [ p for p in paths if re.search( r"[._]nc$", p ) ]
                            nfilepaths = len( filepaths )

                            if nfilepaths > 0:

                                job = { 'date': date.isoformat()[:10], 'mission': mission, 'processing_center': "romsaf",
                                    'file_type': file_type, 'input_relative_dir': path, 'nfiles': nfilepaths }
                                jobs.append( job )

                                LOGGER.info( json.dumps( job ) )

                            else:

                                LOGGER.info( f"No files found in s3://{fullpath}" )

            if "jpl" in processing_centers and mission in valid_missions['aws']:

                if JPLprefix is None:
                    if liveupdate:
                        JPLprefix = "s3://" + version['module'].jpl_liveupdateBucket
                    else:
                        JPLprefix = "s3://" + version['module'].jplBucket

                for file_type in file_types:

                    if file_type == "level1b":
                        jpl_file_type = "calibratedPhase"
                    elif file_type == "level2a":
                        jpl_file_type = "refractivityRetrieval"
                    elif file_type == "level2b":
                        jpl_file_type = "atmosphericRetrieval"
                    else:
                        LOGGER.error(f'File type "{file_type}" for processing center "jpl" is unrecognized.')
                        #raise definejobsError( "InvalidFiletype",f'File type "{file_type}" for processing center "jpl" is unrecognized.' )

                    path = os.path.sep.join( [ JPLprefix, mission, jpl_file_type,
                        "{:4d}/{:02d}/{:02d}".format( date.year, date.month, date.day ) ] )

                    if JPLprefix[:5] == "s3://":
                        try:
                            files = s3.ls( path )
                        except:
                            LOGGER.info( "*** " + path + " does not exist." )
                            continue

                    else:
                        if not os.path.isdir( path ):
                            LOGGER.info( "*** " + path + " does not exist." )
                            continue
                        files = os.listdir( path )

                    files = [ f for f in files if re.search( r"[._]nc$", f ) ]
                    nfiles = len( files )

                    if nfiles > 0:

                        if JPLprefix[:5] == "s3://":
                            path_split = re.split( "/", path[5:] )
                            prefix_split = re.split( "/", JPLprefix[5:] )
                        else:
                            path_split = re.split( "/", path )
                            prefix_split = re.split( "/", JPLprefix )

                        input_relative_dir = "/".join( path_split[len(prefix_split):] )

                        job = { 'date': date.isoformat()[:10], 'mission': mission, 'processing_center': "jpl",
                                'file_type': file_type, 'input_relative_dir': input_relative_dir, 'nfiles': nfiles }
                        jobs.append( job )

                        LOGGER.info( json.dumps( job ) )

                    else:

                        LOGGER.info( f"*** No files found in {path}" )

            if "eumetsat" in processing_centers and mission in valid_missions['aws']:

                if EUMETSATprefix is None:
                    if liveupdate:
                        EUMETSATprefix = "s3://" + version['module'].eumetsat_liveupdateBucket + "/untarred"
                    else:
                        #only liveupdate bucket
                        break

                for file_type in file_types:

                    if file_type == "level1b":
                        level = "level1b"
                    else:
                        #only level1b
                        break

                    #  Loop over UCAR mission paths. AWS mission to UCAR mission is not always a
                    #  one-to-one mapping, and so UCARmissionMapping is consulted.

                    sats = get_receiver_satellites( "aws", mission=mission )
                    EUMETSATmissions = sorted( list( { sat['eumetsat']['mission'] for sat in sats } ) )
                    print("line 435",EUMETSATmissions)
                    for EUMETSATmission in EUMETSATmissions:

                        #  What processing versions are available?
                        try:
                            processingVersions = s3.ls( os.path.join( EUMETSATprefix, EUMETSATmission ) )
                        except:
                            LOGGER.error( "*** " + os.path.join( EUMETSATprefix, EUMETSATmission ) + " does not exist." )
                            continue

                        for processingVersion in processingVersions:
                            try:
                                subdirs = s3.ls( os.path.join( processingVersion, level, f"{year:4d}", f"{doy:03d}" ) )
                            except:
                                LOGGER.info( "*** s3://" + \
                                    os.path.join( processingVersion, level, f"{year:4d}", f"{doy:03d}" ) + \
                                    " does not exist." )
                                continue

                            type_subdirs = []

                            if file_type == "level1b":
                                for subdir in subdirs:
                                    head, tail = os.path.split( subdir )
                                    #go in every subdir as they have diff prefixes
                                    type_subdirs.append( subdir )
                            print("line 461",type_subdirs)
                            if len( type_subdirs ) != 1:
                                LOGGER.info("type_subdirs !=1")
                                continue

                            #  Get a list of all files for this day and file type.

                            dir_pattern = re.compile( EUMETSATprefix[5:] + "/(.*)$" )
                            m = dir_pattern.search( type_subdirs[0] )
                            path = m.group(1)
                            try:
                                filepaths = s3.ls( type_subdirs[0] )
                            except:
                                LOGGER.info( "*** s3://" + type_subdirs[0] + " does not exist." )
                                continue

                            #  Define the new set of jobs.

                            job = { 'file_type': file_type, 'processing_center': "eumetsat",
                                    'mission': mission, 'date': date.strftime("%Y-%m-%d"),
                                    'input_relative_dir': path, 'nfiles': len(filepaths) }

                            jobs.append( job )

                            LOGGER.info( json.dumps( job ) )

        #  Next day.

        date = date + timedelta(days=1)

    #  Define returned dictionary.

    prefixes = {}

    if "ucar" in processing_centers:
        prefixes.update( { 'ucar': UCARprefix } )
    if "romsaf" in processing_centers:
        prefixes.update( { 'romsaf': ROMSAFprefix } )
    if "jpl" in processing_centers:
        prefixes.update( { 'jpl': JPLprefix } )
    if "eumetsat" in processing_centers:
        prefixes.update( { 'eumetsat': EUMETSATprefix } )

    ret = { 'prefixes': prefixes, 'jobs': jobs }

    LOGGER.info( "prefixes={:}, njobs={:}".format( json.dumps( ret['prefixes'] ), len( ret['jobs'] ) ) )

    return ret


################################################################################
#  createjobdefinitionsjson
################################################################################

def createjobdefinitionsjson( daterange, missions, processing_centers, file_types, version,
        jsonfile, UCARprefix=None, ROMSAFprefix=None, JPLprefix=None, EUMETSATprefix=None,
        session=None ):
    """This is a wrapper to definejobs that converts its output to a JSON file. The
    daterange should be a string containing to dates of the form "YYYY-MM-DD" separated
    by a space. The first should be the first day over which to scan; the second should
    be the last day of the scan. missions should be a list of AWS missions over which
    to scan for input files. processing_centers should be a list of contributing
    processing centers to consider. file_types should be a list of AWS file types
    ("level1b", "level2a", etc.) to search over. The version should be one element of
    the Versions.versions list. Output is sent to JSON file jsonfile.

    The keys UCARprefix, ROMSAFprefix, JPLprefix offer override options regarding where
    UCAR, ROMSAF and JPL input files can be found. They can be defined as on the local
    file system or in an S3 bucket, in which case they should be prefixed with
    "s3://".

    The session is a predefined AWS session, authenticated as needed."""

    with open( jsonfile, 'w' ) as out:

        job_definitions = definejobs( daterange, missions, processing_centers, file_types, version,
            UCARprefix=UCARprefix, ROMSAFprefix=ROMSAFprefix, JPLprefix=JPLprefix,
            EUMETSATprefix=EUMETSATprefix, session=session )

        LOGGER.info( f"Writing to JSON file {jsonfile}." )

        json.dump( job_definitions, out, indent="  " )

    return


################################################################################
#  A next job iterator for use in DASK.
################################################################################

class DASKjobs():
    """Create an iterable of DASK preprocessing jobs based on the contents
    of a JSON file created by createjobdefinitionsjson and returned by
    json.load. It will also work with the output of definejobs. Each next
    step in the iterable responds with a 4-tuple of

    ( file_type, processing_center, input_root_path, input_relative_path )

    """

    def __init__( self, job_definitions, session=None ):

        #  Check input.

        if not isinstance( job_definitions, dict ):
            comment = "job_definitions is not a dictionary"
            LOGGER.error( comment )
            raise DASKjobsError( "InvalidJobDefinitions", comment )

        for key in [ 'prefixes', 'jobs' ]:
            if key not in set( job_definitions.keys() ):
                comment = f"job_definitions does not have key {key}"
                LOGGER.error( comment )
                raise DASKjobsError( "InvalidJobDefinitionsKeys", comment )

        if not isinstance( job_definitions['jobs'], list ):
            comment = "job_definitions['jobs'] is not a list"
            LOGGER.error( comment )
            raise DASKjobsError( "InvalidJobDefinitionsJobs", comment )

        if len( job_definitions['jobs'] ) == 0:
            comment = "no jobs in job_definitions['jobs']"
            LOGGER.error( comment )
            raise DASKjobsError( "NoJobDefinitionsJobs", comment )

        #  Establish s3fs.

        if session is None:
            create_session = True
        elif session.profile_name == "default":
            create_session = True
        else:
            create_session = False

        if create_session:
            self.s3 = s3fs.S3FileSystem( **( s3fsauth() ), client_kwargs={'region_name':session.region_name} )
        else:
            self.s3 = s3fs.S3FileSystem( profile=session.profile_name, client_kwargs={'region_name':session.region_name} )

        self.jobs = copy.deepcopy( job_definitions['jobs'] )
        self.prefixes = copy.deepcopy( job_definitions['prefixes'] )
        self.inputfiles = self.loadfiles( self.jobs[0] )

    def __iter__( self ):

        return self

    def loadfiles( self, job ):

        #  Load all files corresponding to a particular job definition.

        file_type, processing_center = job['file_type'], job['processing_center']
        input_root_path = self.prefixes[ processing_center ]
        input_path = os.path.join( input_root_path, job['input_relative_dir'] )
        files = self.s3.ls( input_path )

        inputfiles = [ f for f in files if re.search( r"[._]nc$", f ) ]

        if len( inputfiles ) == 0:
            inputfiles = [ f for f in files if re.search( r"[._]nc$", f ) ]

        if len( inputfiles ) == 0:
            comment = f"No files in s3://{input_path}"
            LOGGER.error( comment )
            #raise DASKjobsError( "NoInputFiles", comment )

        return inputfiles

    def __next__( self ):

        #  Identify the job. Increment to next job if necessary.
        if len( self.inputfiles ) == 0:

            #  Get rid of previous job.

            last_job = self.jobs.pop( 0 )

            if len( self.jobs ) == 0:
                raise StopIteration

            #  Load input files for the next job.

            self.inputfiles = self.loadfiles( self.jobs[0] )

        #  Identify the current job.

        job = self.jobs[0]

        #  Execute jobs.

        file_type, processing_center = job['file_type'], job['processing_center']
        input_root_path = self.prefixes[processing_center]

        #  Next input file.

        inputfile = self.inputfiles[0]

        #  Identify the input.

        pathsplit = inputfile.split( os.path.sep )
        rootpathsplit = input_root_path[5:].split( os.path.sep )
        input_relative_path = os.path.sep.join( pathsplit[len(rootpathsplit):] )

        #  Define the return tuple.

        ret = file_type, processing_center, input_root_path, input_relative_path

        #  Get rid of input file.

        last_file = self.inputfiles.pop(0)

        return ret
