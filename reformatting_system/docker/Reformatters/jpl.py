"""Library of functions that translate JPL formats to AWS-native formats."""

#  Standard imports.

import os
import re
import json
import numpy as np
from netCDF4 import Dataset

#  Library imports.

from Missions import get_receiver_satellites
from Utilities.TimeStandards import Time, Calendar
from Utilities import tangentpoint_radii

#  Define the archive storage bucket for center data and the bucket containing 
#  the liveupdate incoming stream. 

archiveBucket = "jpl-earth-ro-archive-untarred"
liveupdateBucket = ""

#  Define WMO originating center identifier. 

centerwmo = { 'originating_center_id': 173 }

#  Set other parameters. 

gps0 = Time( gps=0 )

#  Logging.

import logging
LOGGER = logging.getLogger( __name__ )


################################################################################
#  Parameters relevant to JPL.
################################################################################

processing_center = "jpl"

data_use_license = "http://creativecommons.org/licenses/by/4.0/"
optimization_references = []
ionospheric_references = [ "Vorobev, V. V. and Krasilnikova, T. G.: Estimation of " + \
        "the accuracy of the refractive index recovery from Doppler shift " + \
        "measurements at frequencies used in the NAVSTAR system, Phys. Atmos. " + \
        "Ocean, 29, 602-609, 1994" ]
retrieval_references = [ "doi:10.1016/S1364-6826(01)00114-6", 
                        "doi:10.1029/2003JD003909", "doi:10.1029/2008JD010483" ]


################################################################################
#  Utility for parsing UCAR file names. 
################################################################################

def varnames( input_file_path ):
    """This function translates an input_file_path as provided by
    JPL into the mission name, the receiver name, and
    the version that should be used in the definition of a DynamoDB entry.
    Note that it must be a complete path, with at least the mission name
    included as an element in the directory tree. The output is a dictionary 
    with keywords "mission", "transmitter", "receiver", "version", 
    "processing_center", "input_file_type", and "time". Additional keywords 
    "status" (success, fail) tell the status of the function, "messages" a 
    list of output mnemonic messages, and "comments" and list of verbose 
    comments."""

#  Initialization.

    ret = { 'status': None, 'messages': [], 'comments': [] }

#  Parse the directory tree.

    head, tail = os.path.split( input_file_path )
    headsplit = re.split( os.path.sep, head )

#  Parse the file name. It can be any one of the level 1b or level 2 file formats.

    m = re.search( "([a-zA-Z]+)_([a-zA-Z0-9]+)\_([a-z]+)_(.*)_([a-zA-Z0-9]+)-([A-Z]\d{2})-(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})\.nc$", tail )

    if not m:
        ret['status'] = "fail"
        comment = f"Path {input_file_path} is unrecognized"
        ret['messages'].append( "UnrecognizedPath" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    JPLfiletype = m.group(1)
    JPLmission = m.group(2)
    JPLversion = m.group(4)
    JPLreceiver = m.group(5)
    JPLtransmitter = m.group(6)
    JPLyear = int( m.group(7) )
    JPLmonth = int( m.group(8) )
    JPLday = int( m.group(9) )
    JPLhour = int( m.group(10) )
    JPLminute = int( m.group(11) )

    cal = Calendar( year=JPLyear, month=JPLmonth, day=JPLday, hour=JPLhour, minute=JPLminute )

#  Enter definition of mission and receiver names.

    input_file_type = JPLfiletype
    transmitter = JPLtransmitter
    version = JPLversion

    sats = get_receiver_satellites( processing_center, mission=JPLmission, receiver=JPLreceiver )
    if len( sats ) != 1: 
        comment = 'Indeterminant LEO identification: the search for receiver ' + \
                f'"{JPLreceiver}" returned {len(sats)} LEO satellites.'
        ret['status'] = "fail"
        ret['messages'].append( "UnrecognizedLEO" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    sat = sats[0]
    receiver = sat['aws']['receiver']
    mission = sat['aws']['mission']

#  Done.

    ret['status'] = "success"
    ret.update( { 
            'mission': mission,
            'receiver': receiver,
            'transmitter': transmitter,
            'version': version,
            'processing_center': processing_center,
            'input_file_type': input_file_type,
            'time': cal } )

    return ret


################################################################################
#  level1b translator
################################################################################

def level1b2aws( jpl_level1b_file, level1b_file, mission, transmitter, receiver, 
        input_file_type, processing_center_version, processing_center_path, 
        version, **extra ):
    """Convert a JPL level1b file to a level1b file suitable for pushing to the 
    AWS Open Data repository. The mission and leo must correspond to the AWS 
    Open Data definitions. The jpl_level1b_file is the JPL input file and the 
    level1b_file is the AWS file type output file. The variables mission, 
    transmitter, receiver, input_file_type, processing_center_version and 
    processing_center_path are written into the output file. The 
    processing_center_path must be a relative path, with the JPL mission name 
    as the root.

    The returned output is a dictiony, key "status" having a value of 
    "success" or "fail", key "messages" having a value that is a list of 
    mnemonic messages of processing, key "comments" having a value that 
    is a list of comments from processing, and key "metadata" having a 
    value that is a dictionary of occultation metadata extracted from the 
    data file."""

    level = "level1b"

    #  Log run.

    LOGGER.info( "Running level1b2aws: " + \
            json.dumps( { 'jpl_level1b_file': jpl_level1b_file, 'level1b_file': level1b_file,
                'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
                'processing_center_path': processing_center_path } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Check if recognized incoming file type. 

    if input_file_type not in [ "calibratedPhase" ]:
        ret['status'] = "fail"
        comment = f"{input_file_type} not a recognized file type"
        ret['messages'].append( "InvalidFileType" )
        ret['comments'].append( comment )
        LOGGER.warning( f"level1b2aws: {comment}" )
        return ret

    #  Check for the existence of the input file.

    if not os.path.exists( jpl_level1b_file ):
        comment = f"File {jpl_level1b_file} not found"
        ret['status'] = "fail"
        ret['messages'].append( "FileNotFound" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Find the file formatter.

    fileformatter = version[level]
    required_RO_order = version['module'].required_RO_order
    required_met_order = version['module'].required_met_order

    #  Open input file.

    try:
        d = Dataset( jpl_level1b_file, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"File {jpl_level1b_file} is not a NetCDF file"
        ret['messages'].append( "FileNotNetCDF" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    #  Create output file.

    try:
        head, tail = os.path.split( level1b_file )
        if head != '':
            if not os.path.isdir( head ):
                ret['comments'].append( f"Creating directory {head}" )
                LOGGER.info( f"Creating directory {head}" )
                os.makedirs( head, exist_ok=True )
        e = Dataset( level1b_file, 'w', format='NETCDF4', clobber=True )

    except:
        ret['status'] = "fail"
        comment = f"Cannot create output file {level1b_file}"
        ret['messages'].append( "CannotCreateFile" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        d.close()
        return ret

    #  Get dimensions. 

    ntimes = d.variables['time'].size
    nsignals = d.variables['carrierFrequency'].size

    #  Create reference time. 

    cal = Calendar( year=d.getncattr("year"), month=d.getncattr("month"), day=d.getncattr("day"), 
                   hour=d.getncattr("hour"), second=d.getncattr("second") )

    #  Reference sat and reference station. 

    referencesat = d.getncattr( "refGnss" )
    referencestation = d.getncattr( "refStation" )

    #  Get start and stop times. 

    starttime = Time( gps=d.variables['startTime'].getValue() )
    endtime = Time( gps=d.variables['endTime'].getValue() )

    #  Format template. 

    outvars = fileformatter( e,
            processing_center, processing_center_version, processing_center_path,
            data_use_license, retrieval_references, ntimes, nsignals, cal.datetime(), mission,
            transmitter, receiver, referencesat=referencesat, referencestation=referencestation, 
            centerwmo=centerwmo, starttime=starttime-gps0 )

    #  Get start and stop times. 

    starttime = Time( gps=d.variables['startTime'].getValue() )
    endtime = Time( gps=d.variables['endTime'].getValue() )

    #  Time attributes. 

    if { "RangeBeginningDate", "RangeBeginningTime", "RangeEndingDate", "RangeEndingTime" }.issubset( e.ncattrs() ): 
        date0 = starttime.calendar( "utc" ).isoformat()
        date1 = endtime.calendar( "utc" ).isoformat()
        e.setncatts( {
            'RangeBeginningDate': date0[:10], 
            'RangeBeginningTime': date0[11:19], 
            'RangeEndingDate': date1[:10], 
            'RangeEndingTime': date1[11:19] } )

    #  Granule ID. 

    if "GranuleID" in e.ncattrs(): 
        m = re.search( "(^.*)\.nc", os.path.basename( level1b_file ) )
        e.setncatts( { 'GranuleID': m.group(1) } )

    #  Is time a leading or a trailing index? 

    leading_time = ( outvars['positionGNSS'].dimensions[0] == "time" )

    #  Write data.

    for key in outvars.keys(): 
        if key in d.variables.keys(): 
            dims = outvars[key].dimensions
            if "time" in dims and len(dims) > 1 and dims[0] != "time": 
                values = d.variables[key][:].T
            else: 
                values = d.variables[key][:]

            #  Mask for NaNs. 

            dtype = str( d.variables[key].dtype )
            if re.search('^float',dtype) or re.search('^int',dtype): 
                outvars[key][:] = np.ma.masked_where( np.isnan(values), values )
            else: 
                outvars[key][:] = values

    #  Determine rising v. setting. 

    if leading_time: 
        ret_radii = tangentpoint_radii( 
            np.array( [ outvars['positionLEO'][0,:], outvars['positionLEO'][-1,:] ] ), 
            np.array( [ outvars['positionGNSS'][0,:], outvars['positionGNSS'][-1,:] ] ) )  
    else: 
        ret_radii = tangentpoint_radii( 
            np.array( [ outvars['positionLEO'][:,0], outvars['positionLEO'][:,-1] ] ), 
            np.array( [ outvars['positionGNSS'][:,0], outvars['positionGNSS'][:,-1] ] ) )  

    ret['messages'] += ret_radii['messages']
    ret['comments'] += ret_radii['comments']

    if ret_radii['status'] == "fail": 
        ret['status'] = "fail"
        return ret

    setting = ( ret_radii['value'][1] < ret_radii['value'][0] )

    #  Close output files. 

    d.close()
    e.close()

    ret['status'] = "success"
    ret['metadata'].update( { 'gps_seconds': starttime-gps0, 'occ_duration': endtime-starttime, 'setting': setting } )

    LOGGER.info( "Exiting level1b2aws\n" )

    return ret


################################################################################
#  level2a translator
################################################################################

def level2a2aws( jpl_level2a_file, level2a_file, mission, transmitter, receiver, 
        input_file_type, processing_center_version, processing_center_path, 
        version, **extra ):
    """Convert a JPL level2a file to a level2a file suitable for pushing to the 
    AWS Open Data repository. The mission and leo must correspond to the AWS 
    Open Data definitions. The jpl_level2a_file is the JPL input file and the 
    level2a_file is the AWS file type output file. The variables mission, 
    transmitter, receiver, input_file_type, processing_center_version and 
    processing_center_path are written into the output file. The 
    processing_center_path must be a relative path, with the JPL mission name 
    as the root.

    The returned output is a dictiony, key "status" having a value of 
    "success" or "fail", key "messages" having a value that is a list of 
    mnemonic messages of processing, key "comments" having a value that 
    is a list of comments from processing, and key "metadata" having a 
    value that is a dictionary of occultation metadata extracted from the 
    data file."""

    level = "level2a"

    #  Log run.

    LOGGER.info( "Running level2a2aws: " + \
            json.dumps( { 'jpl_level2a_file': jpl_level2a_file, 'level2a_file': level2a_file,
                'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
                'processing_center_path': processing_center_path } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Check if recognized incoming file type. 

    if input_file_type not in [ "refractivityRetrieval" ]:
        ret['status'] = "fail"
        comment = f"{input_file_type} not a recognized file type"
        ret['messages'].append( "InvalidFileType" )
        ret['comments'].append( comment )
        LOGGER.warning( f"level2a2aws: {comment}" )
        return ret

    #  Check for the existence of the input file.

    if not os.path.exists( jpl_level2a_file ):
        comment = f"File {jpl_level2a_file} not found"
        ret['status'] = "fail"
        ret['messages'].append( "FileNotFound" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Find the file formatter.

    fileformatter = version[level]
    required_RO_order = version['module'].required_RO_order
    required_met_order = version['module'].required_met_order

    #  Open input file.

    try:
        d = Dataset( jpl_level2a_file, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"File {jpl_level2a_file} is not a NetCDF file"
        ret['messages'].append( "FileNotNetCDF" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Create output file.

    try:
        head, tail = os.path.split( level2a_file )
        if head != '':
            if not os.path.isdir( head ):
                ret['comments'].append( f"Creating directory {head}" )
                LOGGER.info( f"Creating directory {head}" )
                os.makedirs( head, exist_ok=True )
        e = Dataset( level2a_file, 'w', format='NETCDF4', clobber=True )

    except:
        ret['status'] = "fail"
        comment = f"Cannot create output file {level2a_file}"
        ret['messages'].append( "CannotCreateFile" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        d.close()
        return ret

    #  Get dimensions. 

    nsignals = d.variables['carrierFrequency'].size
    nimpacts = d.variables['impactParameter'].size
    nlevels = d.variables['altitude'].size

    #  Create reference time. 

    cal = Calendar( year=d.getncattr("year"), month=d.getncattr("month"), day=d.getncattr("day"), 
                   hour=d.getncattr("hour"), second=d.getncattr("second") )

    #  Format template. 

    outvars = fileformatter( e,
            processing_center, processing_center_version, processing_center_path,
            data_use_license, optimization_references, ionospheric_references, retrieval_references,
            nimpacts, nlevels, cal.datetime(), mission, transmitter, receiver, centerwmo=centerwmo )

    #  Start time and stop time. 

    if { "gps_seconds", "occ_duration" }.issubset( extra.keys() ) and \
            { "RangeBeginningDate", "RangeBeginningTime", "RangeEndingDate", "RangeEndingTime" }.issubset( e.ncattrs() ): 
        date0 = Time( gps=extra['gps_seconds'] ).calendar( "utc" ).isoformat()
        date1 = Time( gps=extra['gps_seconds']+extra['occ_duration'] ).calendar( "utc" ).isoformat()
        e.setncatts( {
            'RangeBeginningDate': date0[:10], 
            'RangeBeginningTime': date0[11:19], 
            'RangeEndingDate': date1[:10], 
            'RangeEndingTime': date1[11:19] } )

    #  Granule ID. 

    if "GranuleID" in e.ncattrs(): 
        m = re.search( "(^.*)\.nc", os.path.basename( level2a_file ) )
        e.setncatts( { 'GranuleID': m.group(1) } )

    #  Get level sequencing. 

    p = d.variables['impactParameter']
    z = d.variables['altitude']

    flip_RO = ( version['module'].required_RO_order == "descending" )^( p[1] > p[0] )
    flip_met = ( version['module'].required_met_order == "descending" )^( z[1] > z[0] )

    #  Write data. Screen and flip order as necessary. 

    for key in outvars.keys(): 
        if key in d.variables.keys(): 
            indimnames = d.variables[key].get_dims()
            if "impact" in indimnames and flip_RO: 
                iaxis = indimnames.index( "impact" )
                outvars[key][:] = np.flip( d.variables[key][:], iaxis )
            elif "level" in indimnames and flip_met: 
                iaxis = indimnames.index( "level" )
                values = np.flip( d.variables[key][:], iaxis )
            else: 
                values = d.variables[key][:]

            #  Mask for NaNs. 

            dtype = str( d.variables[key].dtype )
            if re.search('^float',dtype) or re.search('^int',dtype): 
                outvars[key][:] = np.ma.masked_where( np.isnan(values), values )
            else: 
                outvars[key][:] = values

    #  Compute local time. 

    refcal = ( gps0 + float( d.variables['refTime'].getValue() ) ).calendar("utc")
    local_time = refcal.hour + ( refcal.minute + refcal.second/60 ) / 60 + d.variables['refLongitude'].getValue() / 15
    x = local_time * np.pi/12
    local_time = ( np.arctan2( -np.sin(x), -np.cos(x) ) + np.pi ) * 12/np.pi

    #  Get metadata. 

    ret['metadata'].update( { "longitude": d.variables['refLongitude'].getValue() } )
    ret['metadata'].update( { "latitude": d.variables['refLatitude'].getValue() } )
    ret['metadata'].update( { "local_time": float( local_time ) } )
    ret['metadata'].update( { "setting": ( d.variables['setting'].getValue() != 0 ) } )

    #  Mean orientation. 

    orientations = d.variables['orientation'][:]
    if not isinstance( orientations.mean(), np.ma.core.MaskedConstant ): 
        ref_orientation = np.median( orientations )
        dx = np.deg2rad( orientations - ref_orientation )
        dx_mean = np.arctan2( np.sin(dx), np.cos(dx) ).mean()
        mean_orientation = np.deg2rad( ref_orientation ) + dx_mean
        mean_orientation = np.rad2deg( np.arctan2( np.sin(mean_orientation), np.cos(mean_orientation) ) )
        ret['metadata'].update( { "orientation": mean_orientation } )

    #  Close output files. 

    d.close()
    e.close()

    ret['status'] = "success"

    LOGGER.info( "Exiting level2a2aws\n" )

    return ret



################################################################################
#  level2b reformatter
################################################################################

def level2b2aws( jpl_level2b_file, level2b_file, mission, transmitter, receiver, 
        input_file_type, processing_center_version, processing_center_path, 
        version, **extra ):
    """Convert a JPL level2b file to a level2b file suitable for pushing to the 
    AWS Open Data repository. The mission and leo must correspond to the AWS 
    Open Data definitions. The jpl_level2b_file is the JPL input file and the 
    level2b_file is the AWS file type output file. The variables mission, 
    transmitter, receiver, input_file_type, processing_center_version and 
    processing_center_path are written into the output file. The 
    processing_center_path must be a relative path, with the JPL mission name 
    as the root.

    The returned output is a dictiony, key "status" having a value of 
    "success" or "fail", key "messages" having a value that is a list of 
    mnemonic messages of processing, key "comments" having a value that 
    is a list of comments from processing, and key "metadata" having a 
    value that is a dictionary of occultation metadata extracted from the 
    data file."""

    level = "level2b"

    #  Log run.

    LOGGER.info( "Running level2b2aws: " + \
            json.dumps( { 'jpl_level2b_file': jpl_level2b_file, 'level2b_file': level2b_file,
                'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
                'processing_center_path': processing_center_path } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Check if recognized incoming file type. 

    if input_file_type not in [ "atmosphericRetrieval" ]:
        ret['status'] = "fail"
        comment = f"{input_file_type} not a recognized file type"
        ret['messages'].append( "InvalidFileType" )
        ret['comments'].append( comment )
        LOGGER.warning( f"level2b2aws: {comment}" )
        return ret

    #  Check for the existence of the input file.

    if not os.path.exists( jpl_level2b_file ):
        comment = f"File {jpl_level2b_file} not found"
        ret['status'] = "fail"
        ret['messages'].append( "FileNotFound" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Find the file formatter.

    fileformatter = version[level]
    required_RO_order = version['module'].required_RO_order
    required_met_order = version['module'].required_met_order

    #  Open input file.

    try:
        d = Dataset( jpl_level2b_file, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"File {jpl_level2b_file} is not a NetCDF file"
        ret['messages'].append( "FileNotNetCDF" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    #  Create output file.

    try:
        head, tail = os.path.split( level2b_file )
        if head != '':
            if not os.path.isdir( head ):
                ret['comments'].append( f"Creating directory {head}" )
                LOGGER.info( f"Creating directory {head}" )
                os.makedirs( head, exist_ok=True )
        e = Dataset( level2b_file, 'w', format='NETCDF4', clobber=True )

    except:
        ret['status'] = "fail"
        comment = f"Cannot create output file {level2b_file}"
        ret['messages'].append( "CannotCreateFile" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        d.close()
        return ret

    #  Get dimensions. 

    nlevels = d.variables['altitude'].size

    #  Create reference time. 

    cal = Calendar( year=d.getncattr("year"), month=d.getncattr("month"), day=d.getncattr("day"), 
                   hour=d.getncattr("hour"), second=d.getncattr("second") )

    #  Format template. 

    outvars = fileformatter( e,
            processing_center, processing_center_version, processing_center_path,
            data_use_license, retrieval_references,
            nlevels, cal.datetime(), mission, transmitter, receiver, centerwmo=centerwmo )

    #  Start time and stop time. 

    if { "gps_seconds", "occ_duration" }.issubset( extra.keys() ) and \
            { "RangeBeginningDate", "RangeBeginningTime", "RangeEndingDate", "RangeEndingTime" }.issubset( e.ncattrs() ): 
        date0 = Time( gps=extra['gps_seconds'] ).calendar( "utc" ).isoformat()
        date1 = Time( gps=extra['gps_seconds']+extra['occ_duration'] ).calendar( "utc" ).isoformat()
        e.setncatts( {
            'RangeBeginningDate': date0[:10], 
            'RangeBeginningTime': date0[11:19], 
            'RangeEndingDate': date1[:10], 
            'RangeEndingTime': date1[11:19] } )

    #  Granule ID. 

    if "GranuleID" in e.ncattrs(): 
        m = re.search( "(^.*)\.nc", os.path.basename( level2b_file ) )
        e.setncatts( { 'GranuleID': m.group(1) } )

    #  Get level sequencing. 

    z = d.variables['altitude']
    flip_met = ( version['module'].required_met_order == "descending" )^( z[1] > z[0] )

    #  Write data. Screen and flip order as necessary. 

    for key in outvars.keys(): 
        if key in d.variables.keys(): 
            indimnames = d.variables[key].get_dims()
            if "level" in indimnames and flip_met: 
                iaxis = indimnames.index( "level" )
                values = np.flip( d.variables[key][:], iaxis )
            else: 
                values = d.variables[key][:]

            #  Mask for NaNs. 

            dtype = str( d.variables[key].dtype )
            if re.search('^float',dtype) or re.search('^int',dtype): 
                outvars[key][:] = np.ma.masked_where( np.isnan(values), values )
            else: 
                outvars[key][:] = values

    #  Compute local time. 

    refcal = ( gps0 + float( d.variables['refTime'].getValue() ) ).calendar("utc")
    local_time = refcal.hour + ( refcal.minute + refcal.second/60 ) / 60 + d.variables['refLongitude'].getValue() / 15
    x = local_time * np.pi/12
    local_time = ( np.arctan2( -np.sin(x), -np.cos(x) ) + np.pi ) * 12/np.pi

    #  Get metadata. 

    ret['metadata'].update( { "longitude": d.variables['refLongitude'].getValue() } )
    ret['metadata'].update( { "latitude": d.variables['refLatitude'].getValue() } )
    ret['metadata'].update( { "local_time": float( local_time ) } )
    ret['metadata'].update( { "setting": ( d.variables['setting'].getValue() != 0 ) } )

    #  Mean orientation. 

    if "orientation" in d.variables.keys(): 
        orientations = d.variables['orientation'][:]
        if not isinstance( orientations.mean(), np.ma.core.MaskedConstant ): 
            ref_orientation = np.median( orientations )
            dx = np.deg2rad( orientations - ref_orientation )
            dx_mean = np.arctan2( np.sin(dx), np.cos(dx) ).mean()
            mean_orientation = np.deg2rad( ref_orientation ) + dx_mean
            mean_orientation = np.rad2deg( np.arctan2( np.sin(mean_orientation), np.cos(mean_orientation) ) )
            ret['metadata'].update( { "orientation": mean_orientation } )

    #  Close output files. 

    d.close()
    e.close()

    ret['status'] = "success"

    LOGGER.info( "Exiting level2b2aws\n" )

    return ret


