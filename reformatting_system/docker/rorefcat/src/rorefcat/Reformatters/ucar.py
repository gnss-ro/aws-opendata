"""Library of functions that translate UCAR formats (conPhs/atmPhs,
atmPrf, wetPrf/wetPf2) to AWS formats (level1b, level2a,
level2b."""

#  Standard imports.

import os
import re
import json
import numpy as np
from netCDF4 import Dataset
from datetime import datetime
from astropy.coordinates import SkyCoord
import warnings

warnings.filterwarnings( "ignore" )

#  Library imports.

from ..GNSSsatellites import carrierfrequency, get_transmitter_satellite
from ..Missions import get_receiver_satellites, receiversignals, valid_missions
from ..Utilities.TimeStandards import Time, Calendar
from ..Utilities import LagrangePolynomialInterpolate, transformcoordinates, screen, \
        tangentpoint_radii

#  Suppress warnings. 

import warnings
warnings.filterwarnings("ignore")

#  Define the archive storage bucket for center data and the bucket containing 
#  the liveupdate incoming stream. 

archiveBucket = "ucar-earth-ro-archive-untarred"
liveupdateBucket = "ucar-earth-ro-archive-liveupdate"

#  Define WMO originating center identifier. 

centerwmo = { 'originating_center_id': 60 }

#  UCAR defaults. Define what signals UCAR denotes as CA, L1 and L2 in its 
#  conPhs/atmPhs files. All of these values must correspond directly to the 
#  standardNames defined in the function "signals" defined in the Missions 
#  modules. 

CA_standardNames = [ "C/A", "E1Ca" ]
L1_standardNames = [ "L1" ]
L2_standardNames = [ "L2", "E5b(Q)", "B2a(Pilot)" ]

#  Set other parameters. 

gps0 = Time( gps=0 )

#  Logging.

import logging
LOGGER = logging.getLogger( __name__ )


################################################################################
#  Parameters relevant to UCAR.
################################################################################

processing_center = "ucar"

#  UCAR references.

ionospheric_references = \
    [ "doi:10.1029/1999RS002199", "doi:10.5194/amt-9-335-2016" ]

optimization_references = \
    [ "doi:10.1029/2000RS002370" ]

retrieval_references = \
    [ "doi:10.2151/jmsj.2004.507" ]

data_use_license = "https://www.ucar.edu/terms-of-use/data"

#  UCAR parameter settings relevant to Earth's shape and gravity field.

semi_major_axis, semi_minor_axis = 6378.1370e3, 6356.7523142e3  # m
grav_equator, grav_polar = 9.7803253359, 9.8321849378           # m/s**2
grav_constant = 3.986004418e14                                  # m**3/s**2
earth_omega = 7.292115e-5                                       # rads/s
grav = 9.80665                                                  # J/kg/m
eccentricity = 0.081819
freezing_point = 273.15                                         # K

#  Order of output profiles of radio occultation variables, such as
#  bending angle vs. impact parameter, and of atmospheric variables,
#  such as refractivity, dry pressure, dry temperature, temperature,
#  water vapor vs. height.

required_RO_order = "descending"
required_met_order = "ascending"

#  Imports.


################################################################################
#  Utility functions.
################################################################################

def alt2geop( latitudes, alts, height=True ):
    """Compute geopotential height given geodetic latitude(s) and a numpy
    array of altitudes (MSL_alt) as alts. The units of latitudes, which can
    be a scalar or an array of the same dimensions as alts, are degrees
    north and the units of alts is meters. This is how UCAR converts mean
    sea level altitude to geopotential height. The value of gravity used to
    convert geopotential to geopotential height is the WMO standard, given
    as the variable "grav" below.

    If the keyword height is True, then the returned array is
    "geopotential height" in units of geopotential meters. If it is False,
    however, then the returned array is "geopotential" in units of J/kg. In
    both cases, the output array is a numpy masked array."""

#  Useful intermediate parameters.

    flattening = ( semi_major_axis - semi_minor_axis ) / semi_major_axis
    somigliana = ( semi_minor_axis / semi_major_axis ) * ( grav_polar / grav_equator ) - 1
    grav_ratio = ( earth_omega * earth_omega * semi_major_axis * semi_major_axis * semi_minor_axis ) / grav_constant

#  Make a numpy array.

    zm = np.array( alts )

#  Calculations.

    sin2 = np.sin( np.deg2rad( latitudes ) )**2
    termg = grav_equator * ( 1 + somigliana * sin2 ) / np.sqrt( 1 - eccentricity**2 * sin2 )
    termr = semi_major_axis / ( 1 + flattening + grav_ratio - 2 * flattening * sin2 )

#  Geopotential.

    geop = termg * termr * zm / ( termr + zm )

#  Mask this array.

    geop = np.ma.masked_where( zm < -900, geop )

#  Geopotential heights, in geopotential meters.

    h = geop / grav

#  Done.

    if height:
        return h
    else:
        return geop


################################################################################
#  Utility for parsing UCAR file names.
################################################################################

def varnames( input_file_path ):
    """This function translates an input_file_path as provided by
    UCAR into the mission name, the receiver name, and
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

#   UCAR has information in both their mission name, which is found in the directory
#   hierarchy, and in the satellite name, which is found in their definition of the
#   occultation ID. For single satellite missions, generally speaking, the relationship
#   is trivial, except for GPS/MET.

#  Parse the directory tree.

    head, tail = os.path.split( input_file_path )
    headsplit = re.split( os.path.sep, head )

#  Parse the file name. It can be any one of the level 1b or level 2 file formats.

    m = re.search( r"([a-zA-Z0-9]{6})_([a-zA-Z0-9]+)\.(\d{4})\.(\d{3})\.(\d{2})\.(\d{2})\.(\w\d{2})\_(\S+)\_", tail )

    if not m:
        ret['status'] = "fail"
        comment = f"Path {input_file_path} is unrecognized"
        ret['messages'].append( "UnrecognizedPath" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    UCARfiletype = m.group(1)
    UCARsatellite = m.group(2)
    UCARyear = int( m.group(3) )
    UCARdoy = int( m.group(4) )
    UCARhour = int( m.group(5) )
    UCARminute = int( m.group(6) )
    UCARtransmitter = m.group(7)
    UCARversion = m.group(8)

    cal = Calendar( year=UCARyear, month=1, day=1, hour=UCARhour, minute=UCARminute )
    cal += ( UCARdoy - 1 ) * 86400

#  Search for UCAR mission name.

    UCARmission = None
    for m in valid_missions[processing_center]:
        if m in head:
            UCARmission = m
            break

#  Enter definition of mission and receiver names.

    input_file_type = UCARfiletype
    transmitter = UCARtransmitter
    version = UCARversion

    sats = get_receiver_satellites( processing_center, mission=UCARmission, receiver=UCARsatellite )
    if len( sats ) != 1:
        comment = 'Indeterminant LEO identification: the search for receiver ' + \
                f'"{UCARsatellite}" from processing center "{processing_center}" ' + \
                f'returned {len(sats)} LEO satellites.'
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

def level1b2aws( atmPhs_file, level1b_file, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, **extra ):
    """Convert a UCAR atmPhs/conPhs file to a level1b file suitable
    for pushing to the AWS Open Data repository. The mission and leo must
    correspond to the AWS Open Data definitions. The atmPhs_file is the UCAR
    input file and the level1b_file is the AWS file type output file.
    The variables mission, transmitter, receiver, input_file_type,
    processing_center_version and processing_center_path are written into the
    output file. The processing_center_path must be a relative path, with the
    UCAR mission name as the root.

    The returned output is a dictiony, key "status" having a value of
    "success" or "fail", key "messages" having a value that is a list of
    mnemonic messages of processing, key "comments" having a value that
    is a list of comments from processing, and key "metadata" having a
    value that is a dictionary of occultation metadata extracted from the
    data file."""

    level = "level1b"

    #  Log run.

    LOGGER.info( "Running level1b2aws: " + \
            json.dumps( { 'atmPhs_file': atmPhs_file, 'level1b_file': level1b_file,
                'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
                'processing_center_path': processing_center_path } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Check if atmPhs or conPhs.

    if input_file_type not in [ "atmPhs", "conPhs" ]:
        ret['status'] = "fail"
        comment = f"{input_file_type} not a recognized file type"
        ret['messages'].append( "InvalidFileType" )
        ret['comments'].append( comment )
        LOGGER.warning( f"level1b2aws: {comment}" )
        return ret

    #  Check for the existence of the input file.

    if not os.path.exists( atmPhs_file ):
        comment = f"File {atmPhs_file} not found"
        ret['status'] = "fail"
        ret['messages'].append( "FileNotFound" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Find the file formatter.

    fileformatter = version[level]

    #  Open input file.

    try:
        d = Dataset( atmPhs_file, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"File {atmPhs_file} is not a NetCDF file"
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

    #  Get metadata.

    ncattrs = list( d.ncattrs() )
    ncattrs.sort()
    if len( ncattrs ) == 0:
        ret['status'] = "fail"
        comment = f"No global attributes in source file"
        ret['messages'].append( "GlobalAttributesAbsent" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        d.close()
        e.close()
        return ret

    inputfile_variables = set( d.variables.keys() )

    #  If it is an atmPhs file, check for version.

    if "txmitLR" in inputfile_variables:
        input_phs_type = "2"
    else:
        input_phs_type = "1"

    LOGGER.info( f"atmPhs/conPhs file type {input_phs_type}" )

    #  Read in the time variable.

    if 'time' in inputfile_variables:
        time = d.variables['time'][:]
    elif 'dTime' in inputfile_variables:
        time = d.variables['dTime'][:]
    else:
        ret['status'] = "fail"
        ret['messages'].append( "MissingInputData" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        d.close()
        e.close()
        return ret

    ntimes = time.size
    setting = None

    #  Get fiducial station name.

    if "fidName" in ncattrs:
        referencestation = d.getncattr( "fidName" )
    else:
        referencestation = ""

#----------------------------------------
#  Input file type 1.
#----------------------------------------

    if input_phs_type == "1":

        #  Get starttime and stoptime. Both are instances of Time.

        if 'startTime' in ncattrs:

            starttime = Time( gps=d.getncattr( "startTime" ) )
            stoptime = Time( gps=d.getncattr( "stopTime" ) )
            cal = starttime.calendar("gps")

        else:

            try:
                units = d.variables['time'].getncattr( "units" )
            except:
                ret['status'] = "fail"
                comment = 'No "units" atrribute for time in source file'
                ret['messages'].append( "VariableAttributeAbsent" )
                ret['comments'].append( comment )
                LOGGER.warning( comment )
                d.close()
                e.close()
                return ret

            m = re.search( r"(\d{4})-(\d{2})-(\d{2})\w+(\d{2}):(\d{2}):([0-9.]+)", units )
            if m:
                year, month, day = int( m.group(1) ), int( m.group(2) ), int( m.group(3) )
                hour, minute, second = int( m.group(4) ), int( m.group(5) ), float( m.group(6) )
            else:
                year, month, day = int( d.getncattr("year") ), int( d.getncattr("month") ), int( d.getncattr("day") )
                hour, minute, second = int( d.getncattr("hour") ), int( d.getncattr("minute") ), float( d.getncattr("second") )
            cal = Calendar( year, month, day, hour, minute, second )

            starttime = Time( gps=cal )
            stoptime = starttime + time[-1]

        date = cal.datetime()

        doy = int( d.getncattr( "dayOfYear" ) )

        LOGGER.info(f"calendar: {cal.datetimestring}")

        #  Transmitter and receiver.

        if "conId" in ncattrs:
            constellation = d.getncattr( "conId" )
            if isinstance( constellation, np.int32 ):
                if constellation == 1:
                    constellation = "G"
                elif constellation == 2:
                    constellation = "R"
                elif constellation == 3:
                    constellation = "E"
                elif constellation == 4:
                    constellation = "C"
                elif constellation == 5:
                    constellation = "J"         #  Unsure whether this is true
        else:
            constellation = "G"

        transmitter = "{:}{:02d}".format( constellation, int( d.getncattr("occsatId") ) )
        transmitter_sat = get_transmitter_satellite( transmitter, cal.datetime() )

        referencesat = ""
        if "refsatId" in ncattrs:
            refsatId = int( d.getncattr("refsatId") )
            if refsatId > 0:
                referencesat = "{:}{:02d}".format( "G", refsatId )

        #  What signals are in the input file?

        signals = receiversignals( transmitter, receiver, cal.datetime() )
        if len( signals ) == 0:
            ret['status'] = "fail"
            comment = f"No signals defined for {transmitter=}, {receiver=} at {cal.isoformat()}"
            ret['messages'].append( "NoReceiverSignalsDefined" )
            ret['comments'].append( comment )
            return ret

        LOGGER.info( "signals: {:}".format( json.dumps( signals ) ) )

        if signals is not None:
            nsignals = len( signals )
        else:
            nsignals = 2

        #  Create the output file template.

        outvars = fileformatter( e,
                processing_center, processing_center_version, processing_center_path,
                data_use_license, retrieval_references, ntimes, nsignals, cal.datetime(), mission,
                transmitter, receiver, referencesat=referencesat, referencestation=referencestation, 
                centerwmo=centerwmo, starttime=starttime-gps0 )

        outvarsnames = sorted( list( outvars.keys() ) )

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
            m = re.search( r"(^.*)\.nc", os.path.basename( level1b_file ) )
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

        for isignal, signal in enumerate(signals):

            #  RINEX-3 observation codes and carrier frequencies.

            if "snrCode" in outvarsnames:
                outvars['snrCode'][isignal,:] = "{:}{:}".format( "S", signal['rinex3name'][1:3] )
            if "phaseCode" in outvarsnames:
                outvars['phaseCode'][isignal,:] = "{:}{:}".format( "L", signal['rinex3name'][1:3] )
            if "carrierFrequency" in outvarsnames:
                outvars['carrierFrequency'][isignal] = carrierfrequency( transmitter, date, signal['rinex3name'] )

            if "navBitsPresent" in outvarsnames:
                if input_file_type == "atmPhs" and signal['loop'] == "open" and \
                        signal['rinex3name'] in transmitter_sat['data_tones']:
                    outvars['navBitsPresent'][isignal] = 1
                else:
                    outvars['navBitsPresent'][isignal] = 0

            #  SNR and excess phase.

            if signal['standardName'] in CA_standardNames: 
                x = d.variables['caL1Snr']
                good = screen( x )
                if len(good) > 0 and "snr" in outvarsnames:
                    if leading_time: 
                        outvars['snr'][good,isignal] = x[good].data * 0.1
                    else: 
                        outvars['snr'][isignal,good] = x[good].data * 0.1
                if len(good) > 0 and "excessPhase" in outvarsnames:
                    if leading_time: 
                        outvars['excessPhase'][good,isignal] = d.variables['exL1'][good].data
                    else: 
                        outvars['excessPhase'][isignal,good] = d.variables['exL1'][good].data

            elif signal['standardName'] in L1_standardNames: 
                x = d.variables['pL1Snr']
                good = screen( x )
                if len(good) > 0 and "snr" in outvarsnames:
                    if leading_time: 
                        outvars['snr'][good,isignal] = x[good].data * 0.1
                    else: 
                        outvars['snr'][isignal,good] = x[good].data * 0.1

            elif signal['standardName'] in L2_standardNames: 
                x = d.variables['pL2Snr']
                good = screen( x )
                if len(good) > 0 and "snr" in outvarsnames:
                    if leading_time: 
                        outvars['snr'][good,isignal] = x[good].data * 0.1
                    else: 
                        outvars['snr'][isignal,good] = x[good].data * 0.1
                x = d.variables['exL2']
                good = screen( x )
                if len(good) > 0 and "excessPhase" in outvarsnames:
                    if leading_time: 
                        outvars['excessPhase'][good,isignal] = x[good].data
                    else: 
                        outvars['excessPhase'][isignal,good] = x[good].data

            if 'xrng' in inputfile_variables:
                x = d.variables['xrng']
                good = screen( x )
                if len(good) > 0 and "rangeModel" in outvarsnames:
                    if leading_time: 
                        outvars['rangeModel'][good,isignal] = x[good].data
                    else: 
                        outvars['rangeModel'][isignal,good] = x[good].data

            if "xmdldd" in inputfile_variables:
                x = d.variables['xmdldd']
                good = screen( x )
                if len(good) > 0 and "phaseModel" in outvarsnames:
                    if leading_time: 
                        outvars['phaseModel'][good,isignal] = x[good].data
                    else: 
                        outvars['phaseModel'][isignal,good] = x[good].data

        #  Convert LEO orbits from ECI to ECF.

        eci = np.array( [ d.variables['xLeo'][:], d.variables['yLeo'][:], d.variables['zLeo'][:] ] ).T * 1000.0
        if np.any( np.abs( eci ) > 50.e6 ):
            ret['status'] = "fail"
            comment = "LEO orbit exceeds valid range (> 50,000 km)"
            ret['messages'].append( "InvalidOrbit" )
            ret['comments'].append( comment )
            LOGGER.warning( comment )
            d.close()
            e.close()
            return ret

        ecf = transformcoordinates( eci, time, starttime, direction='eci2ecf' )
        if "positionLEO" in outvarsnames:
            if leading_time: 
                outvars['positionLEO'][:] = ecf[:]
            else: 
                outvars['positionLEO'][:] = ecf[:].T

        #  Convert GNSS orbits from ECI to ECF.

        eci = np.array( [ d.variables['xGps'][:], d.variables['yGps'][:], d.variables['zGps'][:] ] ).T * 1000.0
        if np.any( np.abs( eci ) > 50.e6 ):
            ret['status'] = "fail"
            comment = "Transmitter orbit exceeds valid range (> 50,000 km)"
            ret['messages'].append( "InvalidOrbit" )
            ret['comments'].append( comment )
            LOGGER.warning( comment )
            d.close()
            e.close()
            return ret

        ecf = transformcoordinates( eci, time, starttime, direction='eci2ecf' )
        if "positionGNSS" in outvarsnames:
            if leading_time: 
                outvars['positionGNSS'][:] = ecf[:]
            else: 
                outvars['positionGNSS'][:] = ecf[:].T

#----------------------------------------
#  Input file type 2.
#----------------------------------------

    if input_phs_type == "2":

        #  Get starttime and stoptime. Both are instances of Time.

        try:
            starttime = gps0 + float( d.variables['startTime'][0] )
        except:
            ret['status'] = "fail"
            comment = 'No variable "startTime" in source file'
            ret['messages'].append( "VariableAbsent" )
            ret['comments'].append( comment )
            LOGGER.warning( comment )
            d.close()
            e.close()
            return ret

        try:
            stoptime = gps0 + float( d.variables['stopTime'][0] )
        except:
            ret['status'] = "fail"
            comment = 'No variable "stopTime" in source file'
            ret['messages'].append( "VariableAbsent" )
            ret['comments'].append( comment )
            LOGGER.warning( comment )
            d.close()
            e.close()
            return ret

        cal = starttime.calendar("gps")
        date = starttime.calendar("utc").datetime()
        doy = int( d.getncattr( "dayOfYear" ) )

        LOGGER.info(f"calendar: {cal.datetimestring}")

        #  Transmitter and receiver.

        constellation = d.getncattr( "conId" )
        if isinstance( constellation, np.int32 ):
            if constellation == 1:
                constellation = "G"
            elif constellation == 2:
                constellation = "R"
            elif constellation == 3:
                constellation = "E"
            elif constellation == 4:
                constellation = "C"
            elif constellation == 5:
                constellation = "J"         #  Unsure if this is true

        transmitter = "{:}{:02d}".format( constellation, int( d.getncattr("occsatId") ) )

        referencesat = ""
        if "refsatId" in ncattrs:
            refsatId = int( d.getncattr("refsatId") )
            if refsatId > 0:
                referencesat = "{:}{:02d}".format( "G", refsatId )

        #  What signals are in the input file?

        signals = receiversignals( transmitter, receiver, cal.datetime() )
        if len( signals ) == 0:
            ret['status'] = "fail"
            comment = f"No signals defined for {transmitter=}, {receiver=} at {cal.isoformat()}"
            ret['messages'].append( "NoReceiverSignalsDefined" )
            ret['comments'].append( comment )
            return ret

        if signals is not None:
            nsignals = len( signals )
        else:
            nsignals = 2

        #  Create the output file template.

        outvars = fileformatter( e,
                processing_center, processing_center_version, processing_center_path,
                data_use_license, retrieval_references, ntimes, nsignals, cal.datetime(), mission,
                transmitter, receiver, referencesat=referencesat, referencestation=referencestation, 
                centerwmo=centerwmo, starttime=starttime-gps0 )

        outvarsnames = sorted( list( outvars.keys() ) )

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
            m = re.search( r"(^.*)\.nc", os.path.basename( level1b_file ) )
            e.setncatts( { 'GranuleID': m.group(1) } )

        #  Is time a leading or a trailing index? 

        leading_time = ( outvars['positionGNSS'].dimensions[0] == "time" )

        #  Write data.

        if "startTime" in outvarsnames:
            outvars['startTime'].assignValue( starttime - gps0 )
        if "endTime" in outvarsnames:
            outvars['endTime'].assignValue( stoptime - gps0 )
        if "time" in outvarsnames:
            outvars['time'][:] = d.variables['time'][:].data

        for isignal, signal in enumerate(signals):

            if "snrCode" in outvarsnames:
                outvars['snrCode'][isignal,:] = "{:}{:}".format( "S", signal['rinex3name'][1:3] )
            if "phaseCode" in outvarsnames:
                outvars['phaseCode'][isignal,:] = "{:}{:}".format( "L", signal['rinex3name'][1:3] )
            if "carrierFrequency" in outvarsnames:
                outvars['carrierFrequency'][isignal] = carrierfrequency( transmitter, date, signal['rinex3name'] )

            #  Navigation bits present?

            if "navBitsPresent" in outvarsnames:
                if input_file_type == "atmPhs" and signal['rinex3name'][2] in [ "Y", "C" ] \
                        and signal['loop'] == "open":
                    outvars['navBitsPresent'][isignal] = 1
                else:
                    outvars['navBitsPresent'][isignal] = 0

            #  Write SNR and excess phase.

            if signal['standardName'] in CA_standardNames: 

                x = d.variables['caL1Snr']
                good = screen( x )
                if len(good) > 0 and "snr" in outvarsnames:
                    if leading_time: 
                        outvars['snr'][good,isignal] = x[good].data * 0.1
                    else: 
                        outvars['snr'][isignal,good] = x[good].data * 0.1

                x = d.variables['exL1']
                good = screen( x )
                if len(good) > 0 and "excessPhase" in outvarsnames:
                    if leading_time: 
                        outvars['excessPhase'][good,isignal] = x[good].data
                    else: 
                        outvars['excessPhase'][isignal,good] = x[good].data

            elif signal['standardName'] in L1_standardNames: 

                x = d.variables['exL1']
                good = screen( x )
                if len(good) > 0 and "excessPhase" in outvarsnames:
                    if leading_time: 
                        outvars['excessPhase'][good,isignal] = x[good].data
                    else: 
                        outvars['excessPhase'][isignal,good] = x[good].data

            elif signal['standardName'] in L2_standardNames: 

                x = d.variables['pL2Snr']
                good = screen( x )
                if len(good) > 0 and "snr" in outvarsnames:
                    if leading_time: 
                        outvars['snr'][good,isignal] = x[good].data * 0.1
                    else: 
                        outvars['snr'][isignal,good] = x[good].data * 0.1

                x = d.variables['exL2']
                good = screen( x )
                if len(good) > 0 and "excessPhase" in outvarsnames:
                    if leading_time: 
                        outvars['excessPhase'][good,isignal] = x[good].data
                    else: 
                        outvars['excessPhase'][isignal,good] = x[good].data

            if "xrng" in inputfile_variables:
                x = d.variables['xrng']
                good = screen( x )
                if len(good) > 0 and "rangeModel" in outvarsnames:
                    if leading_time: 
                        outvars['rangeModel'][good,isignal] = x[good].data
                    else: 
                        outvars['rangeModel'][isignal,good] = x[good].data

            if "xmdldd" in inputfile_variables:
                x = d.variables['xmdldd']
                good = screen( x )
                if len(good) > 0 and "phaseModel" in outvarsnames:
                    if leading_time: 
                        outvars['phaseModel'][good,isignal] = x[good].data
                    else: 
                        outvars['phaseModel'][isignal,good] = x[good].data

        #  Get low-rate and high-rate times for signal reception and transmission.
        #  The epoch for all times is starttime.

        epoch = starttime

        receiverLRtime = d.variables['orbtime'][:].data + ( gps0 - epoch )
        receiverHRtime = d.variables['time'][:].data + ( starttime - epoch )

        transmitterLRtime = d.variables['txmitLR'][:].data + ( gps0 - epoch )
        transmitterHRtime = np.interp( receiverHRtime, receiverLRtime, transmitterLRtime )

        #  Receiver: first polynomial interpolate the ECI orbit to high-rate, then
        #  transform to ECF coordinates.

        eciLR = np.array( [ d.variables['xLeoLR'][:], d.variables['yLeoLR'][:], d.variables['zLeoLR'][:] ] )
        f = LagrangePolynomialInterpolate( receiverLRtime, eciLR )
        eciHR = f( receiverHRtime, n=3 ).T
        HRorbit = transformcoordinates( eciHR, receiverHRtime, epoch, direction='eci2ecf' )
        if "positionLEO" in outvarsnames:
            if leading_time: 
                outvars['positionLEO'][:,:] = HRorbit * 1000.0
            else: 
                outvars['positionLEO'][:,:] = HRorbit.T * 1000.0

        #  Transmitter: first polynomial interpolate the ECI orbit to high-rate, then
        #  transform to ECF coordinates.

        eciLR = np.array( [ d.variables['xGnssLR'][:], d.variables['yGnssLR'][:], d.variables['zGnssLR'][:] ] )
        f = LagrangePolynomialInterpolate( transmitterLRtime, eciLR )
        eciHR = f( transmitterHRtime, n=3 ).T
        HRorbit = transformcoordinates( eciHR, transmitterHRtime, epoch, direction='eci2ecf' )
        if "positionGNSS" in outvarsnames:
            if leading_time: 
                outvars['positionGNSS'][:,:] = HRorbit * 1000.0
            else: 
                outvars['positionGNSS'][:,:] = HRorbit.T * 1000.0

    #  Determine whether this is a rising or a setting occultation.

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


################################################################################
#  atmPrf translator
################################################################################

def level2a2aws( atmPrf_file, level2a_file, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, setting=None, **extra ):
    """Translate a UCAR atmPrf file into an AWS Open Data Registry
    level2a file format. atmPrf_file is the path to the input
    UCAR atmPrf file; level2a_file is the path to the
    output level2a file; mission is the name of the mission; receiver
    is the name of the receiving satellite; and transmitter is the name of the
    transmitter in 3-character RINEX-3 format. processing_center_version is the
    version as returned by _varnames, and processing_center_path is the path to
    the file contributed by UCAR that is converted.

    The returned output is a dictiony, key "status" having a value of
    "success" or "fail", key "messages" having a value that is a list of
    mnemonic messages of processing, key "comments" having a value that
    is a list of comments from processing, and key "metadata" having a
    value that is a dictionary of occultation metadata extracted from the
    data file."""

    level = "level2a"

    #  Log run.

    LOGGER.info( "Running level2a2aws" )
    LOGGER.info( f"atmPrf_file={atmPrf_file}, level2a_file={level2a_file}" )
    LOGGER.info( f"mission={mission}, transmitter={transmitter}, receiver={receiver}, processing_center_path={processing_center_path}" )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Check for the existence of the input file.

    if not os.path.exists( atmPrf_file ):
        ret['status'] = "fail"
        comment = f"File {atmPrf_file} not found"
        ret['messages'].append( "FileNotFound" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Find the file formatter.

    fileformatter = version[level]
    required_RO_order = version['module'].required_RO_order

    #  Open input file.

    try:
        d = Dataset( atmPrf_file, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"File {atmPrf_file} is not a NetCDF file"
        ret['messages'].append( "FileNotNetCDF" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    #  Is the bad flag set?

    if "bad" in d.ncattrs():
        bad_flag = d.getncattr( "bad" )
        m = re.search( r"^\w*(\d+)", str( bad_flag ) )
        bad = ( int( m.group(1) ) != 0 )

        if bad:
            ret['status'] = "fail"
            comment = f"{atmPrf_file} has the bad flag set to true"
            ret['messages'].append( "BadRetrieval" )
            ret['comments'].append( comment )
            LOGGER.warning( comment )
            d.close()
            return ret

    #  Get metadata.

    nlevels = d.variables['MSL_alt'][:].size
    ncattrs = d.ncattrs()

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

    #  Get time information.

    year, month, day = d.getncattr('year'), d.getncattr('month'), d.getncattr('day')
    hour, minute, second = d.getncattr('hour'), d.getncattr('minute'), d.getncattr('second')
    cal = Calendar( year=year, month=month, day=day, hour=hour, minute=minute, second=second )
    date = cal.datetime()

    #  Define output file format.

    outvars = fileformatter( e,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, optimization_references, ionospheric_references, retrieval_references,
        nlevels, nlevels, cal.datetime(), mission, transmitter, receiver, centerwmo=centerwmo )

    outvarsnames = sorted( list( outvars.keys() ) )

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
        m = re.search( r"(^.*)\.nc", os.path.basename( level2a_file ) )
        e.setncatts( { 'GranuleID': m.group(1) } )

    #  Justify longitude so that it falls between -180 and +180.

    lon = np.deg2rad( d.getncattr( "lon" ) )
    lon = np.rad2deg( np.arctan2( np.sin(lon), np.cos(lon) ) )
    lat = np.deg2rad( d.getncattr( "lat" ) )

    #  Radius of curvature.

    radiusOfCurvature = d.getncattr( "rfict" ) * 1000.0

    curv = d.getncattr( "curv" ) * 1000.0
    centerOfCurvature_eci = SkyCoord( curv[0], curv[1], curv[2], frame="teme", obstime=cal.isoformat() ) 
    centerOfCurvature_ecf = centerOfCurvature_eci.transform_to("itrs") 
    c = centerOfCurvature_ecf.represent_as("cartesian").xyz
    centerOfCurvature = np.array( c )

    #  Determine order of impact parameter (RO) variables.

    RO_ascending = False        #  UCAR default

    if "Impact_height" in d.variables.keys():
        x = d.variables['Impact_height']
        good = screen( x )
        RO_ascending = ( x[good[1]] > x[good[0]] )
    elif "Impact_parm" in d.variables.keys():
        x = d.variables['Impact_parm']
        good = screen( x )
        RO_ascending = ( x[good[1]] > x[good[0]] )
    else:
        LOGGING.warning( "Could not find impact parameter" )

    #  Determine the order of met variables.

    met_ascending = False       #  UCAR default

    if "MSL_alt" in d.variables.keys():
        x = d.variables['MSL_alt']
        good = screen( x )
        met_ascending = ( x[good[1]] > x[good[0]] )
    else:
        LOGGING.warning( "Could not find MSL_alt" )

    #  Reverse RO and met variables?

    flip_RO = ( bool(RO_ascending) == bool( required_RO_order == "descending" ) )
    flip_met = ( bool(met_ascending) == bool( required_met_order == "descending" ) )

    #  Write variables.

    if "refTime" in outvarsnames:
        outvars['refTime'].assignValue( d.getncattr( "start_time" ) )
    if "refLongitude" in outvarsnames:
        outvars['refLongitude'].assignValue( lon )
    if "refLatitude" in outvarsnames:
        outvars['refLatitude'].assignValue( d.getncattr( "lat" ) )
    if "equatorialRadius" in outvarsnames:
        outvars['equatorialRadius'].assignValue( semi_major_axis )
    if "polarRadius" in outvarsnames:
        outvars['polarRadius'].assignValue( semi_minor_axis )
    if "undulation" in outvarsnames:
        outvars['undulation'].assignValue( d.getncattr( "rgeoid" ) * 1000.0 )
    if "centerOfCurvature" in outvarsnames:
        outvars['centerOfCurvature'][:] = centerOfCurvature
    if "radiusOfCurvature" in outvarsnames:
        outvars['radiusOfCurvature'].assignValue( radiusOfCurvature )

    #  Carrier frequency: first get signals.

    signalList = receiversignals( transmitter, receiver, cal.datetime() )
    if len( signalList ) == 0:
        ret['status'] = "fail"
        comment = f"No signals defined for {transmitter=}, {receiver=} at {date.isoformat()}"
        ret['messages'].append( "NoReceiverSignalsDefined" )
        ret['comments'].append( comment )
        return ret

    #  Carrier frequency: L1

    for signal in signalList:
        if signal['standardName'] in CA_standardNames + L1_standardNames: 
            outvars['carrierFrequency'][0] = carrierfrequency( transmitter, date, signal['rinex3name'] )
            break

    #  Carrier frequency: L2

    for signal in signalList:
        if signal['standardName'] in L2_standardNames and "carrierFrequency" in outvarsnames:
            outvars['carrierFrequency'][1] = carrierfrequency( transmitter, date, signal['rinex3name'] )
            break

    #  Write profile variables.

    rfict = d.getncattr( "rfict" ) * 1000.0
    rgeoid = d.getncattr( "rgeoid" ) * 1000.0
    rcurv = rfict + rgeoid          #  Check: Ask UCAR about definition of radius of curvature.
                                    #  Does it include the geoid undulation or not?

    #  Impact parameter.

    if "impactParameter" in outvarsnames:
        if "Impact_height" in d.variables.keys():
            x = d.variables['Impact_height']
            good = screen( x )
            if len(good) > 0:
                if flip_RO:
                    iout = np.flip( nlevels - 1 - good )
                    outvars['impactParameter'][iout] = np.flip( x[good].data ) * 1000.0 + rcurv
                else:
                    outvars['impactParameter'][good] = x[good].data * 1000.0 + rcurv
        elif "Impact_parm" in d.variables.keys():
            x = d.variables['Impact_parm']
            good = screen( x )
            if len(good) > 0:
                if flip_RO:
                    iout = np.flip( nlevels - 1 - good )
                    outvars['impactParameter'][iout] = np.flip( x[good].data ) * 1000.0
                else:
                    outvars['impactParameter'][good] = x[good].data * 1000.0
        else:
            LOGGING.warning( "Could not generate impact parameter" )

    #  Bending angles.

    if "Bend_ang1" in d.variables.keys():
        x = d.variables['Bend_ang1']
        good = screen( x )
        if len(good) > 0 and "rawBendingAngle" in outvarsnames:
            if flip_RO:
                iout = np.flip( nlevels - 1 - good )
                outvars['rawBendingAngle'][iout,0] = np.flip( x[good].data )
            else:
                outvars['rawBendingAngle'][good,0] = x[good].data

    if "Bend_ang2" in d.variables.keys():
        x = d.variables['Bend_ang2']
        good = screen( x )
        if len(good) > 0 and "rawBendingAngle" in outvarsnames:
            if flip_RO:
                iout = np.flip( nlevels - 1 - good )
                outvars['rawBendingAngle'][iout,1] = np.flip( x[good].data )
            else:
                outvars['rawBendingAngle'][good,1] = x[good].data

    if "Bend_ang" in d.variables.keys():
        x = d.variables['Bend_ang']
        good = screen( x )
        if len(good) > 0 and "bendingAngle" in outvarsnames:
            if flip_RO:
                iout = np.flip( nlevels - 1 - good )
                outvars['bendingAngle'][iout] = np.flip( x[good].data )
            else:
                outvars['bendingAngle'][good] = x[good].data

    if "Opt_bend_ang" in d.variables.keys():
        x = d.variables['Opt_bend_ang']
        good = screen( x )
        if len(good) > 0 and "optimizedBendingAngle" in outvarsnames:
            if flip_RO:
                iout = np.flip( nlevels - 1 - good )
                outvars['optimizedBendingAngle'][iout] = np.flip( x[good].data )
            else:
                outvars['optimizedBendingAngle'][good] = x[good].data

    if "Bend_ang_stdv" in d.variables.keys():
        x = d.variables['Bend_ang_stdv']
        good = screen( x )
        if len(good) > 0 and "bendingAngleUncertainty" in outvarsnames:
            if flip_RO:
                iout = np.flip( nlevels - 1 - good )
                outvars['bendingAngleUncertainty'][iout] = np.flip( x[good].data )
            else:
                outvars['bendingAngleUncertainty'][good] = x[good].data

    #  Altitude and geopotential.

    var = "MSL_alt"

    if var in d.variables.keys():

        #  Altitude:

        x = d.variables[var]
        good = screen( x )
        if len(good) > 0 and "altitude" in outvarsnames:
            if flip_met:
                iout = np.flip( nlevels - 1 - good )
                outvars['altitude'][iout] = np.flip( x[good].data ) * 1000.0
            else:
                outvars['altitude'][good] = x[good].data * 1000.0

        #  Geopotential:

        if len(good) > 0 and "geopotential" in outvarsnames:
            if flip_met:
                iout = np.flip( nlevels - 1 - good )
                outvars['geopotential'][iout] = alt2geop( d.getncattr( "lat" ),
                        np.flip( x[good].data ) * 1000, height=False )
            else:
                outvars['geopotential'][good] = alt2geop( d.getncattr( "lat" ),
                        x[good].data * 1000, height=False )

    else:

        LOGGER.warning( f"Variable {var} not in file {atmPrf_file}." )

    #  Longitude.

    var = "Lon"

    if var in d.variables.keys():
        x = d.variables[var]
        good = screen( x )
        if len(good) > 0 and "longitude" in outvarsnames:
            if flip_met:
                iout = np.flip( nlevels - 1 - good )
                x = np.deg2rad( np.flip( x[good].data ) )
                x = np.rad2deg( np.arctan2( np.sin(x), np.cos(x) ) )
                outvars['longitude'][iout] = x
            else:
                x = np.deg2rad( x[good].data )
                x = np.rad2deg( np.arctan2( np.sin(x), np.cos(x) ) )
                outvars['longitude'][good] = x
    else:
        LOGGER.warning( f"Variable {var} not in file {atmPrf_file}." )

    #  Latitude.

    var = "Lat"

    if var in d.variables.keys():
        x = d.variables[var]
        good = screen( x )
        if len(good) > 0 and "latitude" in outvarsnames:
            if flip_met:
                iout = np.flip( nlevels - 1 - good )
                outvars['latitude'][iout] = np.flip( x[good].data )
            else:
                outvars['latitude'][good] = x[good].data
    else:
        LOGGER.warning( f"Variable {var} not in file {atmPrf_file}." )

    #  Orientation.

    var = "Azim"

    if var in d.variables.keys():
        x = d.variables[var]
        good = screen( x )
        if len(good) > 0 and "orientation" in outvarsnames:
            if flip_met:
                iout = np.flip( nlevels - 1 - good )
                x = np.deg2rad( np.flip( x[good].data ) )
                outvars['orientation'][iout] = np.rad2deg( np.arctan2( np.sin(x), np.cos(x) ) ) + 180.0
            else:
                x = np.deg2rad( x[good].data )
                outvars['orientation'][good] = np.rad2deg( np.arctan2( np.sin(x), np.cos(x) ) ) + 180.0

    else:
        LOGGER.warning( f"Variable {var} not in file {atmPrf_file}." )

    #  Refractivity.

    var = "Ref"

    if var in d.variables.keys():
        x = d.variables[var]
        good = screen( x )
        if len(good) > 0 and "refractivity" in outvarsnames:
            if flip_met:
                iout = np.flip( nlevels - 1 - good )
                outvars['refractivity'][iout] = np.flip( x[good].data )
            else:
                outvars['refractivity'][good] = x[good].data
    else:
        LOGGER.warning( f"Variable {var} not in file {atmPrf_file}." )

    #  Dry pressure, in Pa.

    var = "Pres"

    if var in d.variables.keys():
        x = d.variables[var]
        good = screen( x )
        if len(good) > 0 and "dryPressure" in outvarsnames:
            if flip_met:
                iout = np.flip( nlevels - 1 - good )
                outvars['dryPressure'][iout] = np.flip( x[good].data ) * 100.0
            else:
                outvars['dryPressure'][good] = x[good].data * 100.0
    else:
        LOGGER.warning( f"Variable {var} not in file {atmPrf_file}." )

    #  Setting or rising occultation? First search for this information in the
    #  input file. Then take the value in the database if it is provided. If
    #  no determination of geometry, then return dsetting = None.

    dsetting = None
    if "irs" in ncattrs:
        irs = str( d.getncattr( "irs" ) )
        m = re.search( r"^\w*([+-]*[0-9]+)", irs )
        if m is not None:
            iirs = int( m.group(1) )
            if iirs == +1:
                dsetting = True
            if iirs == -1:
                dsetting = False

    if dsetting is None and setting is not None:
        dsetting = setting

    if dsetting is not None and "setting" in outvarsnames:
        if dsetting:
            outvars['setting'].assignValue( 1 )
        else:
            outvars['setting'].assignValue( 0 )

    #  Mean orientation.

    if "orientation" in outvarsnames:
        orientations = outvars['orientation'][:]
        dx = np.deg2rad( orientations - orientations.mean() )
        meanOrientation = orientations.mean() + np.arctan2( np.sin(dx), np.cos(dx) ).mean()
        meanOrientation = np.rad2deg( np.arctan2( np.sin(meanOrientation), np.cos(meanOrientation) ) )
    else:
        meanOrientation = None

    #  Done writing.

    e.close()

    LOGGER.info( "Completed atmPrf2level2a\n" )

    #  Update output dictionary.

    ret['status'] = "success"

    if "lon" in ncattrs:
        longitude = d.getncattr( "lon" )
        ret['metadata'].update( { "longitude": d.getncattr( "lon" ) } )

    if "lat" in ncattrs:
        ret['metadata'].update( { "latitude": d.getncattr( "lat" ) } )

    if "timloc" in ncattrs:
        local_time = d.getncattr( "timloc" )
        ret['metadata'].update( { "local_time": local_time } )
    elif "lon" in ncattrs:
        local_time = cal.hour + ( cal.minute + cal.second/60.0 ) / 60.0 + longitude / 15.0
        x = local_time * np.pi/12
        local_time = np.arctan2( -np.sin(x), -np.cos(x) ) * 12/np.pi + 12
        ret['metadata'].update( { "local_time": local_time } )

    if "start_time" in ncattrs:
        ret['metadata'].update( { "gps_seconds": d.getncattr( "start_time" ) } )

    if dsetting is not None:
        ret['metadata'].update( { "setting": dsetting } )

    if meanOrientation is not None:
        ret['metadata'].update( { 'orientation': meanOrientation } )

    #  Done reading.

    d.close()

    #  Execution complete.

    LOGGER.info( "Exiting level2a2aws\n" )
    return ret


################################################################################
#  wetPrf/wetPf2 reformatter
################################################################################

def level2b2aws( wetPrf_file, level2b_file, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, setting=None, **extra ):
    """Convert a UCAR wetPrf/wetPf2 file to an level2b file
    suitable for pushing to the AWS Open Data repository. The mission and
    leo must correspond to the AWS Open Data definitions. The wetPrf_file
    is the UCAR input file and the wetPrf_file is the AWS file
    type output file. The variables mission, transmitter, receiver,
    input_file_type, and processing_center_path are written into the
    output file. The processing_center_path must be a relative path, with
    the UCAR mission name as the root.

    The returned output is a dictiony, key "status" having a value of
    "success" or "fail", key "messages" having a value that is a list of
    mnemonic messages of processing, key "comments" having a value that
    is a list of comments from processing, and key "metadata" having a
    value that is a dictionary of occultation metadata extracted from the
    data file."""

    level = "level2b"

    #  Log run.

    LOGGER.info( "Running level2b2aws" )
    LOGGER.info( f"wetPrf_file={wetPrf_file}, level2b_file={level2b_file}" )
    LOGGER.info( f"mission={mission}, transmitter={transmitter}, receiver={receiver}" )
    LOGGER.info( f"input_file_type={input_file_type}, processing_center_path={processing_center_path}" )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Check if wetPrf or wetPf2.

    if input_file_type not in [ "wetPrf", "wetPf2" ]:
        ret['status'] = "fail"
        coment = "level1b2aws: {input_file_type} not a recognized file type"
        ret['messages'].append( "InvalidFileType" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Check for the existence of the input file.

    if not os.path.exists( wetPrf_file ):
        ret['status'] = "fail"
        comment = f"File {wetPrf_file} not found"
        ret['messages'].append( "FileNotFound" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Find the file formatter.

    fileformatter = version[level]
    required_met_order = version['module'].required_met_order

    #  Open input file.

    try:
        d = Dataset( wetPrf_file, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"File {wetPrf_file} is not a NetCDF file"
        ret['messages'].append( "FileNotNetCDF" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    #  Is the bad flag set?

    if "bad" in d.ncattrs():
        bad_flag = d.getncattr( "bad" )
        m = re.search( r"^\w*(\d+)", str( bad_flag ) )
        bad = ( int( m.group(1) ) != 0 )

        if bad:
            ret['status'] = "fail"
            comment = f"{wetPrf_file} has the bad flag set to true"
            ret['messages'].append( "BadRetrieval" )
            ret['comments'].append( comment )
            LOGGER.warning( comment )
            d.close()
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

    #  Get metadata.

    ncattrs = list( d.ncattrs() )
    ncattrs.sort()
    inputfile_variables = set( d.variables.keys() )

    ############################################################
    # wetPrf and wetPf2
    ############################################################

    #  Date information.

    if "date" in ncattrs:
        fileDate = d.getncattr('date')
        regex_search = re.search( r"(\d{4})\-(\d{2})\-(\d{2})\_(\d{2})\:(\d{2})\:(\d{2})\.", fileDate )
        year = int( regex_search.group(1) )
        month = int( regex_search.group(2) )
        day = int( regex_search.group(3) )
        hour = int( regex_search.group(4) )
        minute = int( regex_search.group(5) )
        second = float( regex_search.group(6) )
        cal = Calendar(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

    elif "start_time" in ncattrs:
        cal = Time( gps=d.getncattr('start_time') ).calendar("gps")

    else:
        year = d.getncattr( "year" )
        month = d.getncattr( "month" )
        day = d.getncattr( "day" )
        hour = d.getncattr( "hour" )
        minute = d.getncattr( "minute" )
        second = d.getncattr( "second" )
        cal = Calendar(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

    gps_seconds = Time(gps=cal) - gps0

    #  Determine longitude and latitude.

    m = re.search( r"^([-.0-9]+)", str( d.getncattr( "lon" ) ) )
    if m:
        ret['metadata'].update( { 'longitude': float( m.group(1) ) } )
    else:
        ret['status'] = "fail"
        comment = f"Cannot determine a reference longitude"
        ret['messages'].append( "CannotDetermineLongitude" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    m = re.search( r"^([-.0-9]+)", str( d.getncattr( "lat" ) ) )
    if m:
        ret['metadata'].update( { 'latitude': float( m.group(1) ) } )
    else:
        ret['status'] = "fail"
        comment = f"Cannot determine a reference latitude"
        ret['messages'].append( "CannotDetermineLatitude" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Dimension of data.

    nlevels = d.variables['MSL_alt'].size

    #  Define output template.

    outvars = fileformatter( e,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, retrieval_references,
        nlevels, cal.datetime(), mission, transmitter, receiver, centerwmo=centerwmo )

    outvarsnames = sorted( list( outvars.keys() ) )

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
        m = re.search( r"(^.*)\.nc", os.path.basename( level2b_file ) )
        e.setncatts( { 'GranuleID': m.group(1) } )

    #  Write to output.

    if "refTime" in outvarsnames:
        outvars['refTime'].assignValue( gps_seconds )
    if "refLongitude" in outvarsnames:
        outvars['refLongitude'].assignValue( ret['metadata']['longitude'] )
    if "refLatitude" in outvarsnames:
        outvars['refLatitude'].assignValue( ret['metadata']['latitude'] )

    x = d['MSL_alt']
    good = screen( x )
    if len(good) > 0:
        if "altitude" in outvarsnames:
            outvars['altitude'][good] = x[good].data * 1000
        if "geopotential" in outvarsnames:
            outvars['geopotential'][good] = alt2geop( ret['metadata']['latitude'], x[good].data * 1000, height=False )

    variable = list( inputfile_variables.intersection( { "ref", "Ref" } ) )[0]
    x = d[variable]
    good = screen( x )
    if len(good) > 0 and "refractivity" in outvarsnames:
        outvars['refractivity'][good] = x[good].data

    x = d['Pres']
    good = screen( x )
    if len(good) > 0 and "pressure" in outvarsnames:
        outvars['pressure'][good] = x[good].data * 100

    x = d['Temp']
    good = screen( x )
    if len(good) > 0 and "temperature" in outvarsnames:
        outvars['temperature'][good] = x[good].data + freezing_point

    x = d['Vp']
    good = screen( x )
    if len(good) > 0 and "waterVaporPressure" in outvarsnames:
        outvars['waterVaporPressure'][good] = x[good].data * 100

    #  Setting or rising occultation? First search for this information in the
    #  input file. Then take the value in the database if it is provided. If
    #  no determination of geometry, then return dsetting = None.

    dsetting = None
    if "irs" in ncattrs:
        irs = str( d.getncattr( "irs" ) )
        m = re.search( r"^\w*([+-]*[0-9]+)", irs )
        if m is not None:
            iirs = int( m.group(1) )
            if iirs == +1:
                dsetting = True
            if iirs == -1:
                dsetting = False

    if dsetting is None and setting is not None:
        dsetting = setting

    if dsetting is not None and "setting" in outvarsnames:
        if dsetting:
            outvars['setting'].assignValue( 1 )
        else:
            outvars['setting'].assignValue( 0 )

    #  Close input and output files.

    d.close()
    e.close()


    LOGGER.info(f"translated file path: {level2b_file}")

    #  Compute local time.

    # local_time = cal.hour + ( cal.minute + cal.second/60.0 ) / 60.0
    # x = local_time * np.pi/12
    # local_time = np.arctan2( -np.sin(x), -np.cos(x) ) * 12/np.pi + 12

    ret['status'] = "success"
    ret['metadata'].update( { 'time': cal, 'gps_seconds': gps_seconds } )
    # ret['metadata'].update( { 'local_time': local_time } )

    LOGGER.info( "Exiting level2b2aws\n" )
    return ret
