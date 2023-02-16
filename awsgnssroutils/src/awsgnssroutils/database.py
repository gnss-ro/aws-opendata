"""database.py

Authors: Amy McVey (amcvey@aer.com) and Stephen Leroy (sleroy@aer.com)
Date: 19 December 2022

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

See README documentation for further instruction on usage. 

"""

#  Define parameters of the database.

AWSregion = "us-east-1"
databaseS3bucket = "gnss-ro-data"
AWSversion = "v1.1"
float_fill_value = -999.99

#  Imports.

import os
import datetime
import numpy as np
import s3fs
import json
import re
import time
from botocore import UNSIGNED

#  Usefule parameters.

valid_processing_centers = [ "ucar", "romsaf", "jpl" ]
valid_file_types = [ "calibratedPhase", "refractivityRetrieval", "atmosphericRetrieval" ]

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

def unsigned_S3FileSystem():
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

    def __init__( self, data, s3 ):
        """Create an instance of OccList. The data argument is a list of
        items/RO soundings from the RO database.  The s3 argument is an
        instance of S3FileSystem that enables access to the AWS
        repository of RO data."""

        if isinstance( data, list ):
            self._data = data
        else:
            raise AWSgnssroutilsError( "BadInput", "Input argument data must be a list." )

        if isinstance( s3, S3FileSystem ):
            self._s3 = s3
        else:
            raise AWSgnssroutilsError( "BadInput", "Input argument s3 must be an " + \
                    "instance of class s3fs.S3FileSystem" )

        self.size = len( self._data )


    def filter( self, missions=None, receivers=None, transmitters=None,
        GNSSconstellations=None, longituderange=None, latituderange=None,
        datetimerange=None, localtimerange=None, geometry=None,
        availablefiletypes=None ):
        '''Filter a list of occultations according to various criteria, such as
        mission, receiver, transmitter, etc.  df is a list of items in the database,
        and repository points to the local repository of the database data.

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
        '''

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
                    if center not in valid_processing_centers:
                        raise AWSgnssroutilsError( "InvalidProcessingCenter",
                                f'Processing center "{center}" in availablefiletype {availablefiletype} ' + \
                                        'is not valid.' )
                    if filetype not in valid_file_types:
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

        return OccList( data=keep_list, s3=self._s3 )

    def save(self, filename):
        """Save instance of OccList to filename in line JSON format. The OccList
        can be restored using RODatabaseClient.restore."""

        with open(filename,'w') as file:
            for item in self._data:
                file.write(json.dumps(item)+'\n')

        print( f"Search results saved to {filename}." )

    def info( self, param ):
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
                        if m.group(1) in valid_processing_centers and m.group(2) in valid_file_types:
                            if key not in display.keys():
                                display.update( { key: 0 } )
                            display[key] += 1

        return display

    def download(self, filetype, rootdir, keep_aws_structure = False):
        '''Download RO data files of file type "filetype" from the AWS Registry of Open
        Data to into the local directory "rootdir". The "filetype" must be one of
        *_calibratedPhase, *_refractivityRetrieval, *_atmosphericRetrieval, where * is
        one of the valid contributed RO retrieval centers "ucar", "romsaf", "jpl". If
        "keep_aws_structure" is True, then maintain the same directory structure locally
        as in the AWS bucket; if False, download all data files into "rootdir" without
        subdirectories. '''

        m = re.search( "^([a-z]+)_([a-zA-Z]+)$", filetype )
        if m:
            if m.group(1) not in valid_processing_centers:
                raise AWSgnssroutilsError( "InvalidInput", f'Invalid retrieval center "{m.group(1)}" ' + \
                        'requested; must be one of ' + ', '.join( valid_processing_centers ) )
            elif m.group(2) not in valid_file_types:
                raise AWSgnssroutilsError( "InvalidInput", f'Invalid file type "{m.group(2)}" ' + \
                        'requested; must be one of ' + ', '.join( valid_file_types ) )
        else:
            raise AWSgnssroutilsError( "InvalidInput", f'You must select the "filetype" to download ' + \
                'as ' + ', '.join( [ f"*_{ft}" for f in valid_file_types ] ) + ', where * is one of ' + \
                'the processing centers ' + ', '.join( valid_processing_centers ) )

        ro_file_list = []
        for item in self._data:
            if filetype in item.keys():
                ro_file_list.append(item[filetype])
        ro_file_list = sorted( set( ro_file_list ) )

        sTime = time.time()
        local_file_list = []
        print( f"Downloading {len(ro_file_list)} {filetype} files to {rootdir}." )

        for ro_file in ro_file_list:

            if keep_aws_structure:
                local_path = os.path.join( rootdir, os.path.dirname(ro_file) )
            else:
                local_path = rootdir

            local_file = os.path.join( local_path, os.path.basename(ro_file) )

            #  Make the local directory path if it doesn't already exist.

            os.makedirs(local_path, exist_ok=True)

            #  Download the file if it doesn't already exist locally.

            if not os.path.exists( local_file ):
                self._s3.download( os.path.join( databaseS3bucket, ro_file ), local_file )
                ret = True
            else:
                ret = False

            local_file_list.append( local_file )

        print( "Download took {:} seconds.".format( round((time.time()-sTime),1 ) ) )
        return local_file_list

    def values( self, field ):
        """Return an ndarray of values of a requested field for the data in the
        OccList. Valid fields are "longitude", "latitude", "datetime", "localtime".
        Longitudes and latitudes are in degrees; datetime is an ISO format time;
        and local times are in hours."""

        if field == "longitude":
            x = np.ma.masked_equal( [ item['longitude'] for item in self._data ], float_fill_value )

        elif field == "latitude":
            x = np.ma.masked_equal( [ item['latitude'] for item in self._data ], float_fill_value )

        elif field == "localtime":
            x = np.ma.masked_equal( [ item['local_time'] for item in self._data ], float_fill_value )

        elif field == "datetime":
            x = [ item['date-time'] for item in self._data ]

        else:
            raise AWSgnssroutilsError( "InvalidArgument", "Valid fields are " + \
                    "longitude, latitude, localtime, datetime." )

        return x

    #  Magic methods.

    def __add__(self, occlist2):

        if not isinstance( occlist2, OccList ):
            raise AWSgnssroutilsError( "FaultyAddition", "Unable to concatenate; both arguments must be instances of OccList." )

        return OccList( data=self._data + occlist2._data, s3=self._s3 )

    def __padd__(self, occlist2):

        if not isinstance( occlist2, OccList ):
            raise AWSgnssroutilsError( "FaultyAddition", "Unable to concatenate; both arguments must be instances of OccList." )

        return OccList( data=self._data + occlist2._data, s3=self._s3 )

    def __getitem__(self,items):
        new = self._data[items]
        if isinstance( new, dict ):
            out = OccList( data=[new], s3=self._s3 )
        else:
            out = OccList( data=new, s3=self._s3 )
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

    def __init__( self, repository=None, version=AWSversion, update=False ):
        '''Create an instance of RODatabaseClient. This object serves as a portal
        to the database contents of the AWS Registry of Open Data repository of
        GNSS radio occultation data.

        repository  If set, it is the path to the directory on the local file
                    system where the contents of the RO database are stored
                    locally. If not set, the RO database is read directly from
                    the Open Data Registry S3 bucket. It is *highly* recommended
                    that the contents of the database be stored locally, as this
                    will greatly accelerate database inquiries.

        version     The version of the contents of the AWS Registry of Open Data
                    that should be accessed. The various versions that are
                    accessible can be found in s3://gnss-ro-data/dynamo/.

        update      If requested, update the contents of the local repository
                    to what currently exists in the AWS repository.
        '''

        self._version = version
        self._repository = repository

#  Instantiate the s3 file system in AWS region AWSregion and with unsigned certificate
#  authentication.

        self._s3 = S3FileSystem( unsigned_S3FileSystem )

#  Update the existing repository if requested.

        self._update = update
        if update and repository is not None:
            self.update_repo()


    def update_repo(self):
        '''This module will check for updated json files on the AWS Registry of Open Data.
        If there has been an update in files that are part of your local repo, they will be
        updated.'''

        if not os.path.exists( self._repository ):
            return

        print( f"Updating dynamo json files in {self._repository}." )

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

            s3_uri = os.path.join( databaseS3bucket, f'dynamo/{self._version}/export_subsets', filename )
            s3_info = self._s3.info( s3_uri )
            s3_LastModified_unaware = s3_info['LastModified']
            s3_LastModified = datetime.datetime( s3_LastModified_unaware.year,
                                s3_LastModified_unaware.month, s3_LastModified_unaware.day,
                                s3_LastModified_unaware.hour-altzone )

            if s3_LastModified > local_LastModified:
                print( f"  Updating {filename}" )
                self._s3.download(s3_uri, os.path.join(self._repository,filename) )

        print( "Local repository update took {:} seconds.".format( round((time.time()-sTime),1) ) )

    def query(self, missions=None, receivers=None, datetimerange=None, **filterargs ):
        '''Execute an inquiry on the RO database for RO soundings. At least one of
        the keywords "missions" or "datetimerange" must be specified. If accessing
        a database on the local file system, an inquiry will download all relevant
        database files from the AWS repository. All other keywords will serve as
        filters on the inquiry. Return an instance of class OccList.'''

        #  Check input.

        if missions is None and datetimerange is None:
            raise AWSgnssroutilsError( "InvalidInput", f"Either 'missions' or 'datetimerange' or both must be provided." )

        #  Get listing of all JSON database files.

        initial_file_array = self._s3.ls( os.path.join( databaseS3bucket, f'dynamo/{self._version}/export_subsets' ) )
        print( f"Initial file count: {len(initial_file_array)}" )

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
            print( f"File count after filtering by mission: {len(file_array)}" )

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

            print( f"File count after filtering by date: {len(file_array)}" )

            # self._repository should be for line JSON files only.

        if self._repository is not None:

            print( "Updating local database repository..." )

            local_file_array = []
            os.makedirs(self._repository, exist_ok=True)
            for file in file_array:
                local_path = os.path.join(self._repository,os.path.basename(file))
                if not os.path.exists(local_path):
                    self._s3.download(file, local_path)
                local_file_array.append(local_path)

            # Reset file_array to local path.

            file_array = local_file_array

        print( "Searching files for RO events..." )

        #  With file array, open up and read files in to query more.

        ret_list = OccList( data=[], s3=self._s3 )

        for file in file_array:
            if self._repository is None:
                with self._s3.open(file, 'r') as f:
                    df_dict = json.loads( f.readline() )
            else:
                with open(file, 'r') as f:
                    df_dict = json.loads( f.readline() )
            df = list( df_dict.values() )

            add_list = OccList( df, self._s3 ).filter( missions=missions, receivers=receivers,
                    datetimerange=datetimerange, **filterargs )

            ret_list += add_list

        return ret_list

    def restore( self, datafile ):
        """Restore a previously saved OccList from datafile, which is a
        JSON format file."""

        if os.path.exists( datafile ):
            print( f"Restoring previously saved OccList from {datafile}." )
            data = []
            with open( datafile, 'r' ) as f:
                for line in f.readlines():
                    data.append( json.loads(line) )
        else:
            raise AWSgnssroutilsError( "FaultyData", "Argument data must be a list " + \
                    "of RO database items or a path to a previously saved OccList." )

        occlist = OccList( data=data, s3=self._s3 )
        return occlist

    def __repr__( self ):

        output_list = []

        if self._repository is not None:
            output_list.append( f'repository="{self._repository}"' )

        if self._version is not None:
            output_list.append( f'version="{self._version}"' )

        if self._update:
            output_list.append( "update=True" )
        else:
            output_list.append( "update=False" )

        ret = "RODatabaseClient({:})".format( ", ".join( output_list ) )
        return ret
