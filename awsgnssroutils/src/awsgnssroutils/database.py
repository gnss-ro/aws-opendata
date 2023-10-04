"""database.py

Authors: Amy McVey (amcvey@aer.com) and Stephen Leroy (sleroy@aer.com)
Date: 3 October 2023

================================================================================

This module contains utilities to query the AWS Registry of Open Data repository
of GNSS radio occultation data. It does so using database files posted in the
AWS repository.

Functionality
=============
This module defines two classes: RODatabaseClient and OccList. The first
creates a portal to a database of RO metadata, and the second is an instance
of a list of radio occultations (ROs). Each are described below.

RODatabaseClient:
    Create an instance of a portal to a metadata on all RO data in the AWS
    Registry of Open Data. It provides an option to create a repository of
    the RO metadata on the local file system as keyword "repository".

OccList:
    An instance of the class OccList is contains the metadata on a list of RO
    soundings along with pointers to the RO data files in the AWS Registry of
    Open Data S3 bucket. AWS functionality is completely embedded in the
    methods of the OccList class. Those methods include the ability to
    subset/filter the list according to geolocation and time,
    GNSS transmitter/constellation, GNSS receiver, whether it is a rising or a
    setting occultation, etc. It also includes the ability to combine
    instances of OccList, save the OccList to a JSON format file for future
    restoration by RODatabaseClient.restore, and even download RO data files.

Very useful utilities...

setdefaults: 
    A function that sets defaults for use of RODatabaseClient and OccList. 
    It allows the user to specify defaults for storage of RO metadata 
    database files ("repository") and for downloads of RO data files 
    ("rodata"). In doing so, the user won't have to specify the repository 
    every time an instance of RODatabaseClient is created nor specify a 
    data download path every time the OccList.download method is called. 

populate:
    Pre-populate the metadata database in the default metadata storage 
    directory. This will greatly accelerate all queries of the database, 
    including first queries. Be sure to have run setdefaults in advance. 

See README documentation for further instruction on usage. 

********************************************************************************
Note to developer: Be certain to provide access to s3fs.S3FileSystem that 
is auto-authenticating. See use_S3FileSystem definition, etc. 
********************************************************************************

"""

#  Define parameters of the database.

AWSregion = "us-east-1"
databaseS3bucket = "gnss-ro-data"
float_fill_value = -999.99
defaults_filename = ".awsgnssroutilsrc"

#  Imports.

import os
import datetime
import numpy as np
import s3fs
import json
import re
import time
from tqdm import tqdm
import subprocess
from botocore import UNSIGNED


#  Logging output. 

import logging
LOGGER = logging.getLogger( __name__ )

#  Exception handling.

class Error( Exception ):
    pass

class AWSgnssroutilsError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment


################################################################################
#  Useful utility functions and classes.
################################################################################

def unsigned_S3FileSystem() -> s3fs.S3FileSystem :
    """This is a custom function that contains code to generate an authenticated
    instance of s3fs.S3FileSystem. In this particular case, authentication is
    UNSIGNED."""

    s3 = s3fs.S3FileSystem( client_kwargs={ 'region_name': AWSregion },
                                     config_kwargs={ 'signature_version': UNSIGNED } )

    return s3

class S3FileSystem():
    """This class is a wrapper for s3fs.S3FileSystem that checks for broken
    instances and restores them when necessary."""

    def __init__( self, S3FileSystem_create_function ):
        """Create a wrapper for s3fs.S3FileSystem. The sole argument points to a
        function that returns an instance of s3fs.S3FileSystem that is fully
        authenticated."""

        self._s3fscreate = S3FileSystem_create_function
        message = "Argument to S3FileSystem must be a reference to a function " + \
                "that returns an instance of s3fs.S3FileSystem."

        try:
            self._s3 = self._s3fscreate()
        except:
            raise AWSgnssroutilsError( "IncorrectArgument", message )

        if not isinstance( self._s3, s3fs.S3FileSystem ):
            raise AWSgnssroutilsError( "IncorrectArgument", message )

    def info( self, x ):
        try:
            ret = self._s3.info( x )
        except:
            self._s3 = self._s3fscreate()
            ret = self._s3.info( x )
        return ret

    def download( self, x, y ):
        try:
            ret = self._s3.download( x, y )
        except:
            self._s3 = self._s3fscreate()
            ret = self._s3.download( x, y )
        return ret

    def ls( self, x ):
        try:
            ret = self._s3.ls( x )
        except:
            self._s3 = self._s3fscreate()
            ret = self._s3.ls( x )
        return ret

    def open( self, x ):
        try:
            ret = self._s3.open( x )
        except:
            self._s3 = self._s3fscreate()
            ret = self._s3.open( x )
        return ret


################################################################################
#  IMPORTANT!!!!
#  Define the auto-refresh S3FileSytem that will be used. 
################################################################################

use_S3FileSystem = unsigned_S3FileSystem


################################################################################
#  Useful parameters. Scan the AWS RO data repository for valid versions, valid 
#  processing centers, and valid file types. 
################################################################################

s3 = use_S3FileSystem()

valid_versions = [ entry.split("/")[-1] for entry in s3.ls(f'{databaseS3bucket}/contributed/') ]
valid_processing_centers = {}
valid_file_types = {}

for version in valid_versions: 
    valid_processing_centers.update( { version: list( { entry.split("/")[-1] for entry in 
                    s3.ls(f'{databaseS3bucket}/contributed/{version}/') } ) } )
    fts = []
    for center in valid_processing_centers[version]: 
        missions = [ entry.split("/")[-1] for entry in 
                    s3.ls(f'{databaseS3bucket}/contributed/{version}/{center}/') ]
        for mission in missions: 
            fts += [ entry.split("/")[-1] for entry in 
                    s3.ls(f'{databaseS3bucket}/contributed/{version}/{center}/{mission}/') ]
    valid_file_types.update( { version: list( set( fts ) ) } )


################################################################################
#  Resources utilities: create a defaults file, populate metadata database. 
################################################################################

def setdefaults( repository:str=None, rodata:str=None, version:str=None ) -> dict: 
    """Create a local repository for a history database queries. 

    Create a local repository of prior database queries and record the 
    directory path in a defaults file that can be found in the user's home 
    directory. Also, one can specify a default path for RO data downloads. 

    repository          An absolute path to a directory containing 
                        information from all previous searches. 

    rodata              The default path for downloading RO data. If 
                        it is not provided here, then it must be provided 
                        when the method OccList.download is called. "rodata" 
                        must be an absolute path.

    version             The default RO format version. The available versions 
                        can be found in s3://gnss-ro-data/dynamo. 
    """

    defaults = { 'repository': "", 'rodata': "", 'version': "" }

    #  Be sure the paths are absolute paths. Also, create paths if they 
    #  don't already exist. 

    if repository is not None: 
        if repository != os.path.abspath( repository ): 
            raise AWSgnssroutilsError( "BadPathName", 
                    f'Path to repository ({repository}) must be an absolute path' )
        else: 
            defaults['repository'] = repository
            os.makedirs( repository, exist_ok=True )

    if rodata is not None: 
        if rodata != os.path.abspath( rodata ): 
            raise AWSgnssroutilsError( "BadPathName", 
                    f'Path to rodata ({rodata}) must be an absolute path' )
        else: 
            defaults['rodata'] = rodata
            os.makedirs( rodata, exist_ok=True )


    #  Check for a valid version. 

    if version is not None: 

        if version not in valid_versions: 
            raise AWSgnssroutilsError( "InvalidVersion", f'Version "{version}" is invalid; ' + \
                    'valid versions are ' + ", ".join( valid_versions ) )

        defaults['version'] = version

        if repository is not None: 
            os.makedirs( os.path.join( repository, version ), exist_ok=True )

    #  Record repository and rodata paths and version to resource file. 

    HOME = os.path.expanduser( "~" )
    defaults_file_path = os.path.join( HOME, defaults_filename )

    with open( defaults_file_path, 'w' ) as fp: 
        json.dump( defaults, fp )

    #  Done. 

    return defaults


def populate() -> subprocess.CompletedProcess : 
    """Populate the metadata database in the path established by 
    setdefaults. 

    This function will synchronize the default repository path 
    "{respository}/{version}" with the contents in the AWS S3 path 
    s3://gnss-ro-data/dynamo/{version}/export_subsets. 
    """

    defaults = get_defaults()
    repository, version = defaults['repository'], defaults['version']

    if version is None: 
        raise AWSgnssroutilsError( "InvalidVersion", 'A default version must ' + \
                'be specified using the awsgnssroutils.database.setdefaults function.' )

    #  Check for validity of version. 

    if version not in valid_versions: 
        raise AWSgnssroutilsError( "InvalidVersion", f'Version "{version}" is invalid; ' + \
                'valid versions are ' + ", ".join( valid_versions ) )

    #  Create command to sync the metadata database. 

    command = [ 'aws', 's3', 'sync', 
               f's3://{databaseS3bucket}/dynamo/{version}/export_subsets/', 
               os.path.join(repository,version), '--no-sign-request', '--delete' ]

    #  Synchronize using subprocess.

    ret = subprocess.run( command, capture_output=True )

    return


################################################################################
#  Internal utilities. 
################################################################################

def get_defaults() -> dict: 
    """Read the module defaults from a file in the user's home 
    directory and return contents in a dictionary."""

    #  Define defaults file path. 

    HOME = os.path.expanduser( "~" )
    defaults_file_path = os.path.join( HOME, defaults_filename )

    if not os.path.exists( defaults_file_path ): 
        raise AWSgnssroutilsError( "NoResourcesFile", 
                    f'The defaults file "{defaults_file_path}" could not be found. ' + \
                            'Be sure to create the defaults using the function ' + \
                            'awsgnssroutils.database.setdefaults.' )

    #  Read the defaults. 

    with open( defaults_file_path, 'r' ) as fp: 
        defaults = json.load( fp )

    #  Replace emptry strings with Nones. 

    for key, value in defaults.items(): 
        if value == "": defaults.update( { key: None } )

    #  Done. 

    return defaults


################################################################################
#  Define the OccList class, which defines a list of occultations together with
#  the metadata on each occultation in the list.
################################################################################

class OccList():
    """An instance containing a list of entries/radio occultation soundings
    in the AWS Registry of Open Data for GNSS RO data. It provides methods
    to filter/subset the list, get metadata on the occultations in the list,
    save the list for future use, download relevant database metadata for
    future inquiries directly from the AWS Open Data S3 bucket, and download
    all RO data associated with the entries in the list to the local file
    system."""

    def __init__( self, data:list, s3, version:str ):
        """Create an instance of OccList. 

        Arguments
        =========
        data        A list of items/RO soundings from the RO database. 

        s3          An instance of S3FileSystem that enables access to the 
                    AWS repository of RO data.

        version     The AWS repository version.
        """

        if isinstance( data, list ):
            self._data = data
        else:
            raise AWSgnssroutilsError( "BadInput", "Input argument data must be a list." )

        if isinstance( s3, S3FileSystem ):
            self._s3 = s3
        else:
            raise AWSgnssroutilsError( "BadInput", "Input argument s3 must be an " + \
                    "instance of class s3fs.S3FileSystem" )

        self._version = version

        self.size = len( self._data )


    def filter( self, missions:{str,tuple,list}=None, 
               receivers:{str,tuple,list}=None, 
               transmitters:{str,tuple,list}=None,
               GNSSconstellations:{str,tuple,list}=None, 
               longituderange:{str,tuple,list}=None, 
               latituderange:{str,tuple,list}=None,
               datetimerange:{tuple,list}=None, 
               localtimerange:{tuple,list}=None, 
               geometry:str=None,
               availablefiletypes:{str,tuple,list}=None ):
        """Filter a list of occultations according to various criteria, such as
        mission, receiver, transmitter, etc.  

        Filtering can be done through the following keywords:

        missions            A string or list-like object containing the names of
                            the missions over which to apply the filter.

        receivers           A string or list-like object containing the names of
                            the GNSS RO receivers over which to apply the filter.

        transmitters        A string or list-like object containing the names of
                            the GNSS transmitters over which to apply the filter.

        GNSSconstellations  A string or list-like object containing the names of
                            the GNSS constellations ("G" for GPS, "R" for
                            GLONASS, etc.) over which to apply the filter.

        longituderange      A two-element list-like object containing the
                            longitude bounds of the soundings to retain upon
                            applying the filter. Longitudes are bounded by -180
                            to 180, and the region can wrap around the date line.

        latituderange       A two-element list-list object containing the
                            latitude bounds of the soundings to retain upon
                            applying the filter. Latitudes are bounded by -90 to
                            90.

        datetimerange       A two-element list-like object containing the
                            date-time bounds of the soundings to retain upon
                            applying the filter. Each of the two elements must
                            be a string object with an ISO-format date-time.

        localtimerange      A two-element list-like object containing the
                            local time bounds of the soundings to retain upon
                            applying the filter. Each of the two elements must
                            by a local/solar time in hours, ranging from 0 to
                            24. The range can wrap around midnight (0 hrs).

        geometry            A string defining the occultation geometry over
                            which to apply the filter. Must be "rising" to
                            restrict the list to rising occultations only, or
                            "setting" to restrict the list to setting
                            occultations only.

        availablefiletypes  A list, set, or tuple of strings designating the
                            AWS RO data file types that must be present for
                            the retained soundings. Each file type must be of
                            the format "{center}_{filetype}" where the "center"
                            is one of the valid contributing RO processing
                            centers ("ucar", "jpl", "romsaf", etc.) and the
                            "filetype" is one of the valid AWS RO data file
                            types ("calibratedPhase", "refractivityRetrieval",
                            "atmosphericRetrieval".
        """

        #  Filter by GNSSconstellations or by transmitters, but not by both.

        if GNSSconstellations is not None and transmitters is not None:
            raise AWSgnssroutilsError( "GNSSconstellationsTransmittersClash",
                    "Filtering by both GNSS constellation " + \
                    "and transmitter is not permitted." )

        #  Filter by mission or receivers, but not by both..

        if missions is not None and receivers is not None:
            raise AWSgnssroutilsError( "MissionsReceiversClash", "Filtering by both RO missions " + \
                    "and receiver is not permitted." )

        #  Check GNSSconstellations.

        if GNSSconstellations is not None:
            if isinstance( GNSSconstellations, str ):
                f_GNSSconstellations = [ GNSSconstellations ]
            elif isinstance( GNSSconstellations, tuple ) or \
                isinstance( GNSSconstellations, list ) or \
                isinstance( GNSSconstellations, np.ndarray ):
                    f_GNSSconstellations = list( GNSSconstellations )
            else:
                raise AWSgnssroutilsError( "FaultyGNSSconstellations", "GNSS constellations must be a " + \
                        "string or a list-like object" )
        else:
            f_GNSSconstellations = None

        #  Check transmitters.

        if transmitters is not None:
            if isinstance( transmitters, str ):
                f_transmitters = [ transmitters ]
            elif isinstance( transmitters, tuple ) or \
                isinstance( transmitters, list ) or \
                isinstance( transmitters, np.ndarray ):
                    f_transmitters = list( transmitters )
            else:
                raise AWSgnssroutilsError( "FaultyTransmitters", "transmitters must be a " + \
                        "string or a list-like object" )
        else:
            f_transmitters = None

        #  Check missions.

        if missions is not None:
            if isinstance( missions, str ):
                f_missions = [ missions ]
            elif isinstance( missions, tuple ) or \
                isinstance( missions, list ) or \
                isinstance( missions, np.ndarray ):
                    f_missions = list( missions )
            else:
                raise AWSgnssroutilsError( "FaultyMissions", "missions must be a " + \
                        "string or a list-like object" )
        else:
            f_missions = None

        #  Check receivers.

        if receivers is not None:
            if isinstance( receivers, str ):
                f_receivers = [ receivers ]
            elif isinstance( receivers, tuple ) or \
                isinstance( receivers, list ) or \
                isinstance( receivers, np.ndarray ):
                    f_receivers = list( receivers )
            else:
                raise AWSgnssroutilsError( "FaultyReceivers", "receivers must be a " + \
                        "string or a list-like object" )
        else:
            f_receivers = None

        #  Check longituderange.

        if longituderange is not None:
            if isinstance( longituderange, tuple ) or \
                isinstance( longituderange, list ) or \
                isinstance( longituderange, np.ndarray ):
                    if len( longituderange ) == 2:
                        f_longituderange = np.array( longituderange )
                        if np.logical_and( f_longituderange >= -180, f_longituderange <= 180 ).sum() != 2:
                            raise AWSgnssroutilsError( "FaultyLongitudeRange", "longitudes in " + \
                                    "longituderange must both fall between -180 and 180" )
                    else:
                        raise AWSgnssroutilsError( "FaultyLongitudeRange", "longituderange must have " + \
                                "two elements" )
            else:
                raise AWSgnssroutilsError( "FaultyLongitudeRange", "longituderange must be a tuple/list/ndarray" )
        else:
            f_longituderange = None

        #  Check latituderange.

        if latituderange is not None:
            if isinstance( latituderange, tuple ) or \
                isinstance( latituderange, list ) or \
                isinstance( latituderange, np.ndarray ):
                    if len( latituderange ) == 2:
                        f_latituderange = np.array( latituderange )
                        if np.logical_and( f_latituderange >= -90, f_latituderange <= 90 ).sum() != 2 or \
                            ( f_latituderange[1] <= f_latituderange[0] ):
                            raise AWSgnssroutilsError( "FaultyLatitudeRange", "latitudes in " + \
                                    "latituderange must both fall between -90 and 90 and " + \
                                    "latituderange[1] > latituderange[0]" )
                    else:
                        raise AWSgnssroutilsError( "FaultyLatitudeRange", "latituderange must have " + \
                                "two elements" )
            else:
                raise AWSgnssroutilsError( "FaultyLatitudeRange", "latituderange must be a tuple/list/ndarray" )
        else:
            f_latituderange = None

        #  Check datetimerange.

        if datetimerange is not None:
            if isinstance( datetimerange, tuple ) or \
                isinstance( datetimerange, list ) or \
                isinstance( datetimerange, np.ndarray ):
                    if len( datetimerange ) == 2:
                        try:
                            f_datetimerange = [ datetime.datetime.fromisoformat( datetimerange[0] ),
                                       datetime.datetime.fromisoformat( datetimerange[1] ) ]
                        except:
                            raise AWSgnssroutilsError( "FaultyDatetimerange", "The elements of datetimerange " + \
                                    "must be ISO format times" )
                        if f_datetimerange[0] > f_datetimerange[1]:
                            raise AWSgnssroutilsError( "FaultyDatetimerange", "datetimerange[0] " + \
                                    "must be less than datetimerange[1]" )
                    else:
                        raise AWSgnssroutilsError( "FaultyDatetimerange", "datetimerange must have " + \
                                "two elements" )
            else:
                raise AWSgnssroutilsError( "FaultyDatetimerange", "datetimerange must be a tuple/list" )
        else:
            f_datetimerange = None

        #  Check localtimerange.

        if localtimerange is not None:
            if isinstance( localtimerange, tuple ) or \
                isinstance( localtimerange, list ) or \
                isinstance( localtimerange, np.ndarray ):
                    if len( localtimerange ) == 2:
                        f_localtimerange = np.array( localtimerange )
                        if np.logical_and( f_localtimerange >= 0.0, f_localtimerange < 24.0 ).sum() != 2:
                            raise AWSgnssroutilsError( "FaultySolartimeRange", "localtimes in " + \
                                    "localtimerange must both fall between -180 and 180" )
                    else:
                        raise AWSgnssroutilsError( "FaultySolartimeRange", "localtimerange must have " + \
                                "two elements" )
            else:
                raise AWSgnssroutilsError( "FaultySolartimeRange", "localtimerange must be a tuple/list/ndarray" )
        else:
            f_localtimerange = None

        #  Check geometry.

        if geometry is not None:
            if geometry not in [ "setting", "rising" ]:
                raise AWSgnssroutilsError( "FaultyGeometry", "geometry can only be one of 'setting' or 'rising'" )
        f_geometry = geometry


        #  Check availablefiletypes.

        if availablefiletypes is not None:

            if type( availablefiletypes ) not in [ str, list, set, tuple ]:
                raise AWSgnssroutilsError( "FaultyAvailableFiletypes", 'availablefiletypes must be of class ' + \
                        '"str", "list", "set", or "tuple"' )

            if isinstance( availablefiletypes, str ):
                f_availablefiletypes = { availablefiletypes }
            else:
                f_availablefiletypes = set( availablefiletypes )

            for availablefiletype in f_availablefiletypes:
                m = re.search( "^(\w+)_(\w+)$", availablefiletype )
                if m:
                    center, filetype = m.group(1), m.group(2)
                    if center not in valid_processing_centers[self._version]:
                        raise AWSgnssroutilsError( "InvalidProcessingCenter",
                                f'Processing center "{center}" in availablefiletype {availablefiletype} ' + \
                                        'is not valid.' )
                    if filetype not in valid_file_types[self._version]:
                        raise AWSgnssroutilsError( "InvalidFileType",
                                f'File type "{filetype}" in availablefiletype {availablefiletype} ' + \
                                        'is not valid.' )
                else:
                    raise AWSgnssroutilsError( "InvalidAvailableFiletype",
                                f'availablefiletype {availablefiletype} is not a valid format.' )
        else:
            f_availablefiletypes = None

        # Loop through list of items, each a dictionary.

        keep_list = []

        for item in self._data:

            keep = True

            if f_missions is not None:
                keep &= ( item['mission'] in f_missions )

            if f_receivers is not None:
                keep &= ( item['receiver'] in f_receivers )

            if f_GNSSconstellations is not None:
                keep &= ( item['transmitter'][0] in f_GNSSconstellations )

            if f_transmitters is not None:
                keep &= ( item['transmitter'] in f_transmitters )

            if f_datetimerange is not None:
                dt = datetime.datetime( *[ int(s) for s in item['date-time'].split("-") ] )
                keep &= ( f_datetimerange[0] <= dt and dt <= f_datetimerange[1] )

            if f_longituderange is not None:
                if item['longitude'] == float_fill_value:
                    keep = False
                else:
                    if f_longituderange[0] < f_longituderange[1]:
                        keep &= ( f_longituderange[0] <= item['longitude'] and \
                                item['longitude'] <= f_longituderange[1] )
                    else:
                        keep &= ( f_longituderange[0] <= item['longitude'] or \
                                item['longitude'] <= f_longituderange[1] )

            if f_latituderange is not None:
                if item['latitude'] == float_fill_value:
                    keep = False
                else:
                    keep &= ( f_latituderange[0] <= item['latitude'] and \
                                item['latitude'] <= f_latituderange[1] )

            if f_localtimerange is not None:
                if item['local_time'] == float_fill_value:
                    keep = False
                else:
                    if f_localtimerange[0] < f_localtimerange[1]:
                        keep &= ( f_localtimerange[0] <= item['local_time'] and \
                                item['local_time'] <= f_localtimerange[1] )
                    else:
                        keep &= ( f_localtimerange[0] <= item['local_time'] or \
                                item['local_time'] <= f_localtimerange[1] )

            if f_geometry is not None:
                if item['setting'] is None:
                    keep = False
                else:
                    keep &= ( item['setting'] and ( f_geometry == "setting" ) ) or \
                            ( ( not item['setting'] ) and ( f_geometry == "rising" ) )

            if f_availablefiletypes is not None:
                keep &= f_availablefiletypes.issubset( item.keys() )


            #  Keep or don't keep in list.

            if keep:
                keep_list.append( item )

        #  Generate new OccList based on kept items.

        return OccList( data=keep_list, s3=self._s3, version=self._version )

    def save(self, filename:str):
        """Save instance of OccList to filename in line JSON format. The OccList
        can be restored using RODatabaseClient.restore."""

        with open(filename,'w') as file:
            for item in self._data:
                file.write(json.dumps(item)+'\n')

        LOGGER.debug( f"Search results saved to {filename}." )

    def info( self, param:str ) -> { list, dict }:
        '''Provides information on the following parameters: "mission", "receiver",
        "transmitter", "datetime", "longitude", "latitude", "localtime", "geometry",
        "filetype".

        mission         Return a list of the missions in the OccList instance.

        receiver        Returns a list of the receivers in the OccList instance.

        transmitter     Returns a list of the transmitters in the OccList instance.

        datetime        Returns a dictionary with the "min" and "max" ISO-format
                        date-times in the OccList instance.

        longitude       Returns a dictionary with the "min" and "max" longitudes
                        in the OccList instance.

        latitude        Returns a dictionary with the "min" and "max" latitudes
                        in the OccList instance.

        localtime       Returns a dictionary with the "min" and "max" localtimes
                        in the OccList instance.

        geometry        Returns a dictionary of the counts of "rising" and
                        "setting" occultations in the OccList instance.

        filetype        Returns a dictionary of the counts of various file types,
                        with the names of the file types as the keys and the
                        counts as their values.
        '''

        #  Show which filter key word to use based on the column they choose to show.

        option_list = []

        if param == 'datetime':
            option_list = [ item['date-time'] for item in self._data ]
            display = { 'min': min( option_list ), 'max': max( option_list ) }

        elif param in [ 'longitude', 'latitude', 'localtime' ]:
            xm = np.ma.masked_equal( [ item[param] for item in self._data ], float_fill_value )
            display = { "min":float(xm.min()), "max":float(xm.max()) }

        elif param in [ 'mission', 'receiver', 'transmitter' ]:
            display = sorted( list( { item[param] for item in self._data } ) )

        elif param == 'geometry':
            option_list = [ item['setting'] for item in self._data ]
            display = { 'nsetting':option_list.count(True), 'nrising':option_list.count(False) }

        elif param == "filetype":
            display = {}
            for item in self._data:
                for key in item.keys():
                    m = re.search( "^(\w+)_(\w+)$", key )
                    if m:
                        if m.group(1) in valid_processing_centers[self._version] and m.group(2) in valid_file_types[self._version]:
                            if key not in display.keys():
                                display.update( { key: 0 } )
                            display[key] += 1

        return display

    def download(self, filetype:str, rodata:str=None, 
                 keep_aws_structure:bool=True, silent:bool=False ) -> list :
        """Download RO data files from AWS Registry of Open Data repository of RO 
        data. 

        Download RO data of file type "filetype" from the AWS Registry of Open
        Data. The data are downloaded into the directory "rodata" as specified in 
        the defaults (created by setdefaults) rodata is specified by the keyword. 

        Arguments
        =========

        filetype            The filetype must be one of *_calibratedPhase, 
                            *_refractivityRetrieval, *_atmosphericRetrieval, where 
                            * is one of the valid contributed RO retrieval centers 
                            "ucar", "romsaf", "jpl", etc. 

        rodata              The path to the directory for downloaded RO data files. 
                            It overrides the default download path created by 
                            setdefaults. It can be a relative or absolute path. 

        keep_aws_structure  If true, create a directory hierarchy in the same way 
                            as exists in the RO repository in the AWS Registry of 
                            Open Data. If false, all files are downloaded into the 
                            same directory. Note that all RO files are downloaded 
                            using the AWS hierarchy structure if rodata is not 
                            specified as an argument here. 

        silent              By setting to True, no progress bars are displayed. 
                            Progress bars are shown by default. 
        """

        if rodata is not None: 
            rootdir = rodata

        else: 
            defaults = get_defaults()
            rootdir = defaults['rodata']
            if not keep_aws_structure: 
                raise AWSgnssroutilsError( "BadArgument", 'keep_aws_structure must be ' + \
                        'true if RO data are downloaded into the default directory' )

        m = re.search( "^([a-z]+)_([a-zA-Z]+)$", filetype )
        if m:
            if m.group(1) not in valid_processing_centers[self._version]:
                raise AWSgnssroutilsError( "InvalidInput", 
                        f'Invalid retrieval center "{m.group(1)}" ' + \
                        'requested; must be one of ' + \
                        ', '.join( valid_processing_centers[self._version] ) )
            elif m.group(2) not in valid_file_types[self._version]:
                raise AWSgnssroutilsError( "InvalidInput", 
                        f'Invalid file type "{m.group(2)}" ' + \
                        'requested; must be one of ' + \
                        ', '.join( valid_file_types[self._version] ) )
        else:
            raise AWSgnssroutilsError( "InvalidInput", 
                        f'You must select the "filetype" to download ' + 'as ' + \
                        ', '.join( [ f"*_{ft}" for f in valid_file_types[self._version] ] ) + \
                        ', where * is one of the processing centers ' + \
                        ', '.join( valid_processing_centers[self._version] ) )

        ro_file_list = []
        for item in self._data:
            if filetype in item.keys():
                ro_file_list.append(item[filetype])
        ro_file_list = sorted( set( ro_file_list ) )

        sTime = time.time()
        local_file_list = []
        LOGGER.debug( f"Downloading {len(ro_file_list)} {filetype} files to {rootdir}." )

        #  Progress bar or no progress bar. 

        if silent: 
            iterator = range(len(ro_file_list))
        else: 
            iterator = tqdm( range(len(ro_file_list)), desc="Downloading..." )

        for ifile in iterator: 
            ro_file = ro_file_list[ifile]

            if keep_aws_structure:
                local_path = os.path.join( rootdir, os.path.dirname(ro_file) )
            else:
                local_path = rootdir

            local_file = os.path.join( local_path, os.path.basename(ro_file) )

            #  Make the local directory path if it doesn't already exist.

            os.makedirs(local_path, exist_ok=True)

            #  Download the file if it doesn't already exist locally.

            if not os.path.exists( local_file ):
                self._s3.download( "/".join( [databaseS3bucket,ro_file] ), local_file )
                ret = True
            else:
                ret = False

            local_file_list.append( local_file )

        LOGGER.debug( "Download took {:} seconds.".format( round((time.time()-sTime),1 ) ) )
        return local_file_list

    def values( self, field:str ) -> np.ndarray :
        """Return an ndarray of values of a requested field for the data in the
        OccList. 

        Valid fields are "longitude", "latitude", "datetime", "localtime" and "time".  
        Longitudes and latitudes are in degrees; time is an ISO format time; and 
        local times are in hours."""

        if field == "longitude":
            x = np.ma.masked_equal( [ item['longitude'] for item in self._data ], float_fill_value )

        elif field == "latitude":
            x = np.ma.masked_equal( [ item['latitude'] for item in self._data ], float_fill_value )

        elif field == "localtime":
            x = np.ma.masked_equal( [ item['local_time'] for item in self._data ], float_fill_value )

        elif field == "datetime":
            x = [ item['date-time'] for item in self._data ]

        elif field == "time":
            x = [ item['time'] for item in self._data ]

        else:
            raise AWSgnssroutilsError( "InvalidArgument", "Valid fields are " + \
                    "longitude, latitude, localtime, datetime." )

        return x

    #  Magic methods.

    def __add__(self, occlist2):

        if not isinstance( occlist2, OccList ):
            raise AWSgnssroutilsError( "FaultyAddition", 
                    "Unable to concatenate; both arguments must be instances of OccList." )

        return OccList( data=self._data + occlist2._data, s3=self._s3, version=self._version )

    def __padd__(self, occlist2):

        if not isinstance( occlist2, OccList ):
            raise AWSgnssroutilsError( "FaultyAddition", 
                    "Unable to concatenate; both arguments must be instances of OccList." )

        return OccList( data=self._data + occlist2._data, s3=self._s3, version=self._version )

    def __getitem__(self,items):
        new = self._data[items]
        if isinstance( new, dict ):
            out = OccList( data=[new], s3=self._s3, version=self._version )
        else:
            out = OccList( data=new, s3=self._s3, version=self._version )
        return out

    def __repr__(self):
        return f'OccList({len(self._data)} items)'


################################################################################
#  Define the RODatabaseClient class, which creates a portal to the database
#  of radio occultation data in the AWS Registry of Open Data.
################################################################################

class RODatabaseClient:
    '''Class to initialize filter of dynamo line JSON files named by mission and day.
    An instance of this class initiates a gateway to the database of GNSS radio
    occultation data in the AWS Registry of Open Data. '''

    def __init__( self, repository:str=None, version:str=None, update:bool=False ):
        '''Create an instance of RODatabaseClient. This object serves as a portal
        to the database contents of the AWS Registry of Open Data repository of
        GNSS radio occultation data.

        repository  If set, it is the path to the directory on the local file
                    system where the contents of the RO database are stored
                    locally. If not set, the repository path is read from a 
                    defaults file created by the function setdefaults. 

        version     The version of the contents of the AWS Registry of Open Data
                    that should be accessed. The various versions that are
                    accessible can be found in s3://gnss-ro-data/dynamo/.

        update      If requested, update the contents of the local repository
                    to what currently exists in the AWS repository.
        '''

        #  Instantiate the s3 file system in AWS region AWSregion and with unsigned certificate
        #  authentication.

        self._s3 = S3FileSystem( use_S3FileSystem )

        #  Check version. 

        if version is None: 
            defaults = get_defaults()
            if defaults['version'] is None: 
                raise( "InvalidVersion", 'Either specify a version when instantiating ' + \
                        'RODatabaseClient or specify it when setting defaults with setdefaults.' )
            else: 
                uversion = defaults['version']
        else: 
            uversion = version

        if uversion not in valid_versions: 
            raise AWSgnssroutilsError( "InvalidVersion", f'Version "{uversion}" is invalid; ' + \
                    'valid versions are ' + ", ".join( valid_versions ) )

        self._version = uversion

        #  Get location of local database repository of previous searches. 

        if repository is None: 
            defaults = get_defaults()
            self._repository = defaults['repository']
        else: 
            self._repository = repository

        os.makedirs( os.path.join( self._repository, self._version ), exist_ok=True )

        #  Update the existing repository if requested.

        self._update = update
        if update: 
            self.update_repo()



    def update_repo(self):
        '''This module will check for updated json files on the AWS Registry of Open Data.
        If there has been an update in files that are part of your local repo, they will be
        updated.'''

        if not os.path.exists( self._repository ):
            return

        LOGGER.debug( f"Updating dynamo json files in {self._repository}." )

        sTime = time.time()
        altzone = int( time.altzone / 3600 )     #  Correct for the time zone.
        allfiles = sorted( os.listdir( self._repository ) )

        for filename in allfiles:
            local_json_info = os.stat( os.path.join( self._repository, filename ) )
            local_LastModified_ctime = time.ctime( local_json_info.st_mtime )
            local_LastModified_unaware = datetime.datetime.strptime( local_LastModified_ctime, "%a %b %d %H:%M:%S %Y" )
            local_LastModified = datetime.datetime( local_LastModified_unaware.year,
                        local_LastModified_unaware.month,
                        local_LastModified_unaware.day,
                        local_LastModified_unaware.hour)

            s3_uri = "/".join( [ databaseS3bucket, f'dynamo/{self._version}/export_subsets', filename ] )
            s3_info = self._s3.info( s3_uri )
            s3_LastModified_unaware = s3_info['LastModified']
            s3_LastModified = datetime.datetime( s3_LastModified_unaware.year,
                                s3_LastModified_unaware.month, s3_LastModified_unaware.day,
                                s3_LastModified_unaware.hour-altzone )

            if s3_LastModified > local_LastModified:
                LOGGER.info( f"  Updating {filename}" )
                self._s3.download(s3_uri, os.path.join(self._repository,self._version,filename) )

        LOGGER.debug( "Local repository update took {:} seconds.".format( round((time.time()-sTime),1) ) )

    def query(self, missions:{str,tuple,list}=None, 
              datetimerange:{tuple,list}=None, 
              silent:bool=False, **filterargs ) -> OccList:
        """Execute an query on the RO database for RO soundings. 

        This method obtains database JSON files from the AWS S3 bucket if the 
        requested files are not already available in the local repository. 
        At least one of the keywords "missions" or "datetimerange" must be 
        specified. All other keywords will serve as filters on the query. 
        Return an instance of class OccList. 

        Arguments
        =========
        missions        The names of the missions to query, as defined in the 
                        documentation at github.com/gnss-ro/aws-opendata. 

        datetimerange   A two-element tuple or list specifying the time range 
                        over which to query RO metadata, both elements being 
                        ISO format datetimes.

        silent          Whether or not to run silently, generally pertaining 
                        to progress bars. 
        """

        #  Check input.

        if missions is None and datetimerange is None:
            raise AWSgnssroutilsError( "InvalidInput", 
                    f"Either 'missions' or 'datetimerange' or both must be provided." )

        #  Get listing of all JSON database files.

        initial_file_array = self._s3.ls( "/".join( 
                    [ databaseS3bucket, f'dynamo/{self._version}/export_subsets' ] ) )
        LOGGER.debug( f"Initial file count: {len(initial_file_array)}" )

        # Filter by mission.

        if missions is None:
            file_array = initial_file_array.copy()
        else:
            file_array = []
            for file in initial_file_array:
                basename = os.path.basename(file)
                basename_mission = basename.split('_')[0]
                if basename_mission in missions:
                    file_array.append( file )
            LOGGER.debug( f"File count after filtering by mission: {len(file_array)}" )

        # Filter by date.

        if datetimerange is not None:
            remove_list = []
            try:
                dt = datetime.datetime.fromisoformat( datetimerange[0] )
                rangeStart = datetime.datetime( dt.year, dt.month, dt.day )
                dt = datetime.datetime.fromisoformat( datetimerange[1] )
                rangeEnd = datetime.datetime( dt.year, dt.month, dt.day )
            except:
                raise AWSgnssroutilsError( "FaultyDateFormat", "The datetimerange elements are not ISO format datetimes" )

            for file in file_array:
                m = re.search( "^\w+_(\d{4})-(\d{2})-(\d{2})\.json$", os.path.basename(file) )
                file_datetime = datetime.datetime( int(m.group(1)), int(m.group(2)), int(m.group(3)) )
                if file_datetime < rangeStart or file_datetime > rangeEnd:
                    remove_list.append(file)

            for f in remove_list:
                file_array.remove(f)

            LOGGER.debug( f"File count after filtering by date: {len(file_array)}" )

            # self._repository should be for line JSON files only.

        LOGGER.info( "Updating local database repository..." )

        local_file_array = []
        os.makedirs(self._repository, exist_ok=True)

        #  Progress bar? 

        if silent: 
            iterator = range(len(file_array))
        else: 
            iterator = tqdm( range(len(file_array)), desc="Downloading..." )

        for ifile in iterator: 
            file = file_array[ifile]
            local_path = os.path.join(self._repository,self._version,os.path.basename(file))
            if not os.path.exists(local_path):
                self._s3.download(file, local_path)
            local_file_array.append(local_path)

        # Reset file_array to local path.

        file_array = local_file_array

        LOGGER.debug( "Searching files for RO events..." )

        #  With file array, open up and read files in to query more.

        ret_list = OccList( data=[], s3=self._s3, version=self._version )

        if silent: 
            iterator = range(len(file_array))
        else: 
            iterator = tqdm( range(len(file_array)), desc="Loading..." )

        for ifile in iterator:
            file = file_array[ifile]
            with open(file, 'r') as f:
                df_dict = json.loads( f.readline() )
            df = list( df_dict.values() )

            add_list = OccList( df, s3=self._s3, version=self._version ).filter( 
                    missions=missions, datetimerange=datetimerange, **filterargs )

            ret_list += add_list

        return ret_list

    def restore( self, datafile:str ) -> OccList :
        """Restore a previously saved OccList from datafile, which is a
        JSON format file."""

        if os.path.exists( datafile ):
            LOGGER.debug( f"Restoring previously saved OccList from {datafile}." )
            data = []
            with open( datafile, 'r' ) as f:
                for line in f.readlines():
                    data.append( json.loads(line) )
        else:
            raise AWSgnssroutilsError( "FaultyData", "Argument data must be a list " + \
                    "of RO database items or a path to a previously saved OccList." )

        occlist = OccList( data=data, s3=self._s3, version=self._version )
        return occlist

    def __repr__( self ):

        output_list = []

        output_list.append( f'repository="{self._repository}"' )
        output_list.append( f'version="{self._version}"' )

        if self._update:
            output_list.append( "update=True" )
        else:
            output_list.append( "update=False" )

        ret = "RODatabaseClient({:})".format( ", ".join( output_list ) )
        return ret
