"""Library of functions that translate EUMETSAT formats (GRAS) to AWS formats
(level1b)."""

#  Standard imports.

import os
import re
import json
import numpy as np
from netCDF4 import Dataset
from scipy.interpolate import interp1d
from datetime import datetime
from astropy.coordinates import TEME, ITRS

#  Library imports.

from GNSSsatellites import carrierfrequency, get_transmitter_satellite
from Missions import get_receiver_satellites, receiversignals, valid_missions
from Utilities.TimeStandards import Time, Calendar
from Utilities import LagrangePolynomialInterpolate, transformcoordinates, screen, \
        tangentpoint_radii

#  Define the archive storage bucket for center data and the bucket containing 
#  the liveupdate incoming stream. 

archiveBucket = ""
liveupdateBucket = "eumetsat-earth-ro-archive-liveupdate"

#  Define WMO originating center identifier. 

wmo = { 'originating_center_id': 254 }

#  Set other parameters. 

gps0 = Time( gps=0 )
fill_value = -9.99e20

#  Logging.

import logging
LOGGER = logging.getLogger( __name__ )


################################################################################
#  Parameters relevant to EUMETSAT.
################################################################################

processing_center = "eumetsat"

#  EUMETSAT parameter settings. 

#  Order of output profiles of radio occultation variables, such as
#  bending angle vs. impact parameter, and of atmospheric variables,
#  such as refractivity, dry pressure, dry temperature, temperature,
#  water vapor vs. height.

# required_RO_order = "descending"
# required_met_order = "ascending"

#  Imports.


################################################################################
#  Utility for parsing EUMETSAT file names.
################################################################################

def varnames( input_file_path ):
    """This function translates an input_file_path as provided by
    EUMETSAT into the mission name, the receiver name, and
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

#   EUMETSAT has information in both their mission name, which is found in the directory
#   hierarchy, and in the satellite name, which is found in their definition of the
#   occultation ID. For single satellite missions, generally speaking, the relationship
#   is trivial

#  Parse the directory tree.

    head, tail = os.path.split( input_file_path )

#  Parse the file name. It is level 1b file formats.

    m = re.search( "^(\S+)_(\S+)_(\S+)_(\d{14})Z_(\d{14})Z_R_O_(\d{14})Z_([A-Z]\d\d)_(N[ND])_(\d+)\.nc$", os.path.basename( tail ) )

    if not m: 
        comment = f"Cannot parse input filename {tail}"
        ret['status'] = "fail"
        ret['messages'].append( "IndecipherableFilename" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Instrument, level, receiver, transmitter, version...

    EUMETSAT_instrument, EUMETSAT_level, EUMETSAT_receiver = m.group(1), m.group(2), m.group(3)
    EUMETSAT_transmitter, EUMETSAT_status, EUMETSAT_version = m.group(7), m.group(8), m.group(9)

    #  Start time of measurement. 

    ds = m.group(4)
    cal = Calendar( year=int(ds[0:4]), month=int(ds[4:6]), day=int(ds[6:8]), hour=int(ds[8:10]), minute=int(ds[10:12]), second=int(ds[12:14]) )
    EUMETSAT_start_time = Time( utc=cal )

    #  Stop/end time of measurement. 

    ds = m.group(5)
    cal = Calendar( year=int(ds[0:4]), month=int(ds[4:6]), day=int(ds[6:8]), hour=int(ds[8:10]), minute=int(ds[10:12]), second=int(ds[12:14]) )
    EUMETSAT_end_time = Time( utc=cal )

    #  Processing time. 

    ds = m.group(6)
    cal = Calendar( year=int(ds[0:4]), month=int(ds[4:6]), day=int(ds[6:8]), hour=int(ds[8:10]), minute=int(ds[10:12]), second=int(ds[12:14]) )
    processing_time = Time( utc=cal )
    processing_time_cal = processing_time.calendar("utc")

    #  The time of the occultation is determined as the mid-time of the measurement. 

    EUMETSAT_time = EUMETSAT_start_time + 0.5 * ( EUMETSAT_end_time - EUMETSAT_start_time )
    EUMETSAT_cal = EUMETSAT_time.calendar( "utc" )

    #  Identify the satellite. 

    sats = get_receiver_satellites( processing_center, receiver=EUMETSAT_receiver )
    if len( sats ) != 1:
        comment = 'Indeterminant LEO identification: the search for receiver ' + \
                f'"{EUMETSAT_receiver}" from processing center "{processing_center}" ' 
        ret['status'] = "fail"
        ret['messages'].append( "UnrecognizedLEO" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    sat = sats[0]
    receiver = sat['aws']['receiver']
    mission = sat['aws']['mission']

#  Done.

    if EUMETSAT_status == "NN": 
        ret['status'] = "success"
    else: 
        ret['status'] = "fail"
        comment = f'Occultation {input_file_path} has degraded status and is eliminated.'
        ret['messages'].append( "BadRetrieval" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )

    ret.update( {
            'mission': mission,
            'receiver': receiver,
            'transmitter': EUMETSAT_transmitter,
            'version': EUMETSAT_version,
            'processing_center': processing_center,
            'input_file_type': EUMETSAT_level,
            'time': EUMETSAT_cal, 
            'processing_time': processing_time_cal
            } )

    return ret


################################################################################
#  level1b translator
################################################################################

def level1b2aws( input_file, level1b_file, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, **extra ):
    """Convert EUMETSAT GRAS or BJxx file to a level1b file suitable
    for pushing to the AWS Open Data repository. The mission and leo must
    correspond to the AWS Open Data definitions.
    The variables mission, transmitter, receiver, input_file_type,
    processing_center_version and processing_center_path are written into the
    output file. The processing_center_path must be a relative path, with the
    EUMETSAT mission name as the root.

    The returned output is a dictionary, key "status" having a value of
    "success" or "fail", key "messages" having a value that is a list of
    mnemonic messages of processing, key "comments" having a value that
    is a list of comments from processing, and key "metadata" having a
    value that is a dictionary of occultation metadata extracted from the
    data file."""

    level = "level1b"

    #  Log run.

    LOGGER.info( "Running level1b2aws: " + \
            json.dumps( { 'input_file': input_file, 'level1b_file': level1b_file,
                'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
                'processing_center_path': processing_center_path } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Check for the existence of the input file.

    if not os.path.exists( input_file ):
        comment = f"File {input_file} not found"
        ret['status'] = "fail"
        ret['messages'].append( "FileNotFound" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Find the file formatter.

    fileformatter = version[level]

    #  Open input file.

    try:
        d = Dataset( input_file, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"File {input_file} is not a NetCDF file"
        ret['messages'].append( "FileNotNetCDF" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    #  Check for overall quality. 

    if "quality" in d.groups.keys(): 
        if "overall_quality_ok" in d.groups['quality'].variables.keys(): 
            overall_quality_ok = d.groups['quality'].variables['overall_quality_ok'].getValue()
            if overall_quality_ok == 0: 
                ret['status'] = "fail"
                comment = f'Overall quality of {input_file} is not OK'
                ret['messages'].append( "BadRetrieval" )
                ret['comments'].append( comment )
                LOGGER.warning( comment )
                d.close()
                return ret
        else: 
            ret['status'] = "fail"
            comment = f'Overall quality variable not in {input_file}'
            ret['messages'] += [ "MissingQualityControlVariable", "BadRetrieval" ]
            ret['comments'].append( comment )
            LOGGER.warning( comment )
            d.close()
            return ret
    else: 
        ret['status'] = "fail"
        comment = f'Quality group not in {input_file}'
        ret['messages'] += [ "MissingQualityControlGroup", "BadRetrieval" ]
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        d.close()
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

    #  Level 1a data group in input. 

    l1a = d.groups['data'].groups['level_1a']

    #  Get number of high-rate times, number of tracked signals. 

    ntimes = l1a.groups['combined'].dimensions['t'].size
    nsignals = l1a.groups['combined'].dimensions['codes'].size

    #  Get reference station and reference transmitter. 

    reference_station = None
    reference_transmitter = None

    #  Get measurement epoch. 

    days = l1a.variables['gps_start_absdate'].getValue()
    seconds = l1a.variables['gps_start_abstime'].getValue()
    epoch = Time( gps=Calendar(year=2000,month=1,day=1) ) + float( days*86400 + seconds )

    #  Read in the time variable.

    time = l1a.groups['combined'].variables['dtime'][:]

    #  Get starttime and stoptime. Both are instances of Time.

    starttime = epoch + float( time[0] )
    stoptime = epoch + float( time[-1] )
    cal = ( epoch + time.mean() ).calendar("utc")

    #  Get references. 

    if "doi" in d.ncattrs(): 
        references = d.getncattr( "doi" )
        if isinstance( references, list ): 
            retrieval_references = [ "doi:"+r for r in references ]
        elif references == "": 
            retrieval_references = []
        else: 
            retrieval_references = [ "doi:"+references ]
    else: 
        retrieval_references = []

    #  Define the data use license. 

    ret_varnames = varnames( input_file )
    if ret_varnames['status'] == "fail": 
        ret['status'] = "fail"
        ret['messages'] += ret_varnames['messages']
        ret['comments'] += ret_varnames['comments']
        return ret

    data_use_license = "This file contains modified EUMETSAT bending angle data, " + \
            "a Core Data product of EUMETSAT ({:04d}); ".format( ret_varnames['processing_time'].year ) + \
            "see https://www.eumetsat.int/eumetsat-data-licensing"

    #  Create the output file template.

    outvars = fileformatter( e,
            processing_center, processing_center_version, processing_center_path,
            data_use_license, retrieval_references, ntimes, nsignals, cal.datetime(), mission,
            transmitter, receiver, referencesat=reference_transmitter, 
            referencestation=reference_station, centerwmo=wmo, starttime=starttime-gps0 )

    outvarsnames = sorted( list( outvars.keys() ) )

    #  What signals are in the input file?

    input_signals = list( l1a.groups['combined'].variables['codes'][:] )

    #  Start time and stop time. 

    if { "RangeBeginningDate", "RangeBeginningTime", "RangeEndingDate", "RangeEndingTime" }.issubset( e.ncattrs() ): 
        date0 = starttime.calendar( "utc" ).isoformat()
        date1 = stoptime.calendar( "utc" ).isoformat()
        e.setncatts( {
            'RangeBeginningDate': date0[:10], 
            'RangeBeginningTime': date0[11:19], 
            'RangeEndingDate': date1[:10], 
            'RangeEndingTime': date1[11:19] } )

    #  Granule ID. 

    if "GranuleID" in e.ncattrs(): 
        m = re.search( "(^.*)\.nc", os.path.basename( level1b_file ) )
        e.setncatts( { 'GranuleID': m.group(1) } )

    #  Write data.

    if "startTime" in outvarsnames:
        outvars['startTime'].assignValue( starttime - gps0 )
    if "endTime" in outvarsnames:
        outvars['endTime'].assignValue( starttime - gps0 + time[-1] )
    if "time" in outvarsnames:
        outvars['time'][:] = time.data

    #  Is time a leading or a trailing index? 

    leading_time = ( outvars['positionGNSS'].dimensions[0] == "time" )

    #  Navigation bits present in data? 

    if "open_loop" in l1a.groups.keys(): 
        if "external_navbits_ok" in d.groups['quality'].variables.keys(): 
            navBitsPresent = ( d.groups['quality'].variables['external_navbits_ok'].getValue() == 0 )
        else: 
            navBitsPresent = True
    else: 
        navBitsPresent = False

    #  Reference to pseudo_range group. 

    if "pseudo_range" in l1a.groups.keys(): 
        pseudo_range = l1a.groups['pseudo_range']
    else: 
        pseudo_range = None
        comment = f"pseudo_range group does not exist in {input_file}"
        ret['comments'].append( comment )
        LOGGER.warning( comment )

    #  Loop over signals. 

    for isignal in range( nsignals ): 

        #  RINEX-3 observation codes and carrier frequencies.

        if "snrCode" in outvarsnames:
            outvars['snrCode'][isignal,:] = "S" + input_signals[isignal].upper()
        if "phaseCode" in outvarsnames:
            outvars['phaseCode'][isignal,:] = "L" + input_signals[isignal].upper()
        if "carrierFrequency" in outvarsnames:
            outvars['carrierFrequency'][isignal] = l1a.groups['combined'].variables['frequencies'][isignal]

        if "navBitsPresent" in outvarsnames:
            if navBitsPresent: 
                outvars['navBitsPresent'][isignal] = 1
            else: 
                outvars['navBitsPresent'][isignal] = 0

        #  SNR.

        var = "snr_" + input_signals[isignal]
        if var in l1a.groups['combined'].variables.keys(): 
            x = l1a.groups['combined'].variables[var][:]
            if leading_time: 
                outvars['snr'][:,isignal] = x[:]
            else: 
                outvars['snr'][isignal,:] = x[:]
        else: 
            comment = f'SNR variable "{var}" not in file'
            ret['comments'].append( comment )
            LOGGER.warning( comment )

        #  Excess phase. 

        var = "exphase_" + input_signals[isignal] + "_nco"
        if var in l1a.groups['combined'].variables.keys(): 
            x = l1a.groups['combined'].variables[var][:]
            if leading_time: 
                outvars['excessPhase'][:,isignal] = x[:]
            else: 
                outvars['excessPhase'][isignal,:] = x[:]
        else: 
            comment = f'Excess phase variable "{var}" not in file'
            ret['comments'].append( comment )
            LOGGER.warning( comment )

        #  Pseudo-range model. 

        if pseudo_range is not None: 

            ptimes = pseudo_range.variables['dtime'][:]

            var = "pseudorange_" + input_signals[isignal] 
            if var in pseudo_range.variables.keys(): 
                x = pseudo_range.variables[var][:]
                xintp = interp1d( ptimes, x, kind='cubic', bounds_error=False, fill_value=fill_value )
                y = xintp( time.data )
                yma = np.ma.masked_where( y==fill_value, y )
                if leading_time: 
                    outvars['rangeModel'][:,isignal] = yma
                else: 
                    outvars['rangeModel'][isignal,:] = yma
            else: 
                comment = f'Range model variable "{var}" not in {input_file}'
                ret['comments'].append( comment )
                LOGGER.warning( comment )

    #  Convert LEO orbits from ECI to ECF.

    eci = l1a.groups['combined'].variables['r_receiver'][:]
    ecf = transformcoordinates( eci, time, starttime, direction='eci2ecf', ecisystem="J2000" )

    if "positionLEO" in outvarsnames:
        if leading_time: 
            outvars['positionLEO'][:] = ecf[:]
        else: 
            outvars['positionLEO'][:] = ecf[:].T

    #  Convert GNSS orbits from ECI to ECF.

    eci = l1a.groups['combined'].variables['r_transmitter'][:]
    ecf = transformcoordinates( eci, time, starttime, direction='eci2ecf', ecisystem="J2000" )

    if "positionGNSS" in outvarsnames:
        if leading_time: 
            outvars['positionGNSS'][:] = ecf[:]
        else: 
            outvars['positionGNSS'][:] = ecf[:].T

    #  Determine whether this is a rising or a setting occultation.

    if "setting" in extra.keys(): 
        setting = extra['setting']
    else: 
        setting = None

    if setting is None and "positionLEO" in outvarsnames and "positionGNSS" in outvarsnames:

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
            d.close()
            e.close()
            return ret

        setting = ( ret_radii['value'][1] < ret_radii['value'][0] )

    #  Close output files.

    d.close()
    e.close()

    ret['status'] = "success"
    ret['metadata'].update( { 
            "gps_seconds": starttime-gps0, 
            "occ_duration": stoptime-starttime } )

    if setting is not None:
        ret['metadata'].update( { "setting": setting } )

    LOGGER.info( "Exiting level1b2aws\n" )

    return ret

