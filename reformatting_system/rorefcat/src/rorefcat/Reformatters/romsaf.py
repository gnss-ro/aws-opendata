#!/usr/env/python3

"""
module romsaf2aws - conversion methods for ROMSAF GNSS RO products
(Level 1B, 2A and 2B) to AWS Open Data Registry formats (level 2a
and 2b).

     Methods:
     --------
     level2a2aws()
         converts ROPP Level 1B and 2A data to AWS level 2a.

     level2b2aws()
        converts ROPP Level 2B and 2C data to AWS level 2b.

"""

#  Imports.
#  Standard library imports.

import copy
from datetime import datetime
import os
import re
import json
from ..Missions import get_receiver_satellites

#  Define the archive storage bucket for center data and the bucket containing 
#  the liveupdate incoming stream. 

archiveBucket = "romsaf-earth-ro-archive-untarred"
liveupdateBucket = "romsaf-earth-ro-archive-liveupdate"

#  Logger and debugging.

import logging
LOGGER = logging.getLogger(__name__)

#  Related third party imports.

from netCDF4 import Dataset
import numpy as np
from scipy.interpolate import interp1d

#  Local application/library specific imports.

from ..GNSSsatellites import carrierfrequency
from ..Missions import receiversignals, valid_missions
from ..Utilities.TimeStandards import Time, Calendar
from ..Utilities.gravitymodel import geopotential as JGM3geopotential
from ..Utilities import LagrangePolynomialInterpolate, screen

#  Define WMO originating center identifier. 

centerwmo = { 'originating_center_id': 94 }

#  Set other parameters. 

gps0 = Time( gps=0 )

#  ROMSAF resources.

processing_center = "romsaf"

#  Gas values.

freezing_point = 273.15                     # K
Rgas = 8.314                                # Ideal gas constant [J/K/mole]
mudry = 28.964e-3                           # Molecular weight of dry air [kg/mole]
muvap = 18.015e-3                           # Molecular weight of water vapor [kg/mole]

#  These values are for refractivity that accounts fully for compressibility. See
#  Aparicio and Laroche, doi:10.1029/2010JD015214.

a1 = 222.682                                # [ N-units m**3 / kg ]
a2 = 0.069                                  # [ N-units m**3 / kg ]
a3 = 6701.605                               # [ N-units m**3 / kg ]
a4 = 6385.886                               # [ N-units m**3 / kg ]

#  k1, k2, k3 based on a1, a2, a3, a4

k1 = a1 * mudry / Rgas
k2 = a2 * mudry / Rgas
k3 = a3 * muvap / Rgas
k4 = a4 * muvap / Rgas

#  Gravity and the mean seal level geoid ellipse.

gravity = 9.80665                           # WMO standard (??) -- double check ROMSAF's gravity!
semi_major_axis = 6378.1370e3               # m
semi_minor_axis = 6356.7523142e3            # m

#  Parameters relevant to ROM SAF.

processing_center = "romsaf"
processing_center_version = "8.1"

#  ROM SAF references.

ionospheric_references = \
    [ "doi:10.1029/1999RS002199" ]          #  Syndergaard 2000

optimization_references = \
    [ "doi:10.1029/2000RS002370" ]          #  Gorbunov 2002

retrieval_references = [ 
        { 'mission': "metop", 'product': "cdr", 'datetimerange': ( datetime(2006,10,1), datetime(2017,1,1) ), 
            'references': [ "doi:10.15770/EUM_SAF_GRM_0002" ] }, 
        { 'mission': "cosmic1", 'product': "cdr", 'datetimerange': ( datetime(2006,4,1), datetime(2017,1,1) ), 
            'references': [ "doi:10.15770/EUM_SAF_GRM_0003" ] }, 
        { 'mission': "champ", 'product': "cdr", 'datetimerange': ( datetime(2001,9,1), datetime(2008,10,1) ), 
            'references': [ "doi:10.15770/EUM_SAF_GRM_0004" ] }, 
        { 'mission': "grace", 'product': "cdr", 'datetimerange': ( datetime(2007,3,1), datetime(2017,1,1) ), 
            'references': [ "doi:10.15770/EUM_SAF_GRM_0005" ] }, 
        { 'mission': "metop", 'product': "icdr", 'datetimerange': ( datetime(2017,1,1), None ), 
            'references': [ "doi:10.15770/EUM_SAF_GRM_0006" ] } 
        ]

data_use_license = "https://www.eumetsat.int/eumetsat-data-licensing"


################################################################################
#  Utility to parse the ROMSAF file name.
################################################################################

def varnames( input_file_path ):
    """This function translates an input_file_path as provided by
    the ROMSAF into the mission name, the receiver name, and
    the version that should be used in the definition of a DynamoDB entry.
    Note that it must be a complete path, with at least the mission name
    included as an element in the directory tree.  The output is a dictionary
    with keywords "mission", "transmitter", "receiver", "version",
    "processing_center", "input_file_type", and "time". Additional keywords
    "status" (success, fail) tell the status of the function, "messages" a
    list of output mnemonic messages, and "comments" and list of verbose
    comments."""

#  Initialization.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

#  Naming conventions split by processing center.

#  The ROM SAF has information in both their mission name, which is found in the directory
#  hierarchy, and in the satellite name, which is found in their definition of the
#  occultation ID. For single satellite missions, generally speaking, the relationship
#  is trivial, except for GPS/MET.

#  Parse the directory tree.

    head, tail = os.path.split( input_file_path )
    headsplit = re.split( os.path.sep, head )

#  Parse the file name. It can be any one of the level 1b or level 2 file formats.

    m = re.search( r"^([a-z]{3})_(\d{4})(\d{2})(\d{2})_(\d+)_([a-zA-Z0-9]+)_([a-zA-Z0-9]+)_([a-zA-Z]+)_(\d+)_(\d+)\.nc$", tail )

    if not m:
        ret['status'] = "fail"
        comment = f"Path {input_file_path} is unrecognized"
        ret['messages'].append( "UnrecognizedPath" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    ROMSAFfiletype = m.group(1)
    ROMSAFyear = int( m.group(2) )
    ROMSAFmonth = int( m.group(3) )
    ROMSAFday = int( m.group(4) )
    ROMSAFhour = int( m.group(5)[0:2] )
    ROMSAFminute = int( m.group(5)[2:4] )
    ROMSAFsecond = int( m.group(5)[4:6] )
    ROMSAFreceiver = m.group(6)
    ROMSAFtransmitter = m.group(7)

    if ROMSAFfiletype not in [ "atm", "wet" ]:
        ret['status'] = "fail"
        comment = f"File type in ROM SAF file {ROMSAFfiletype} not recognized"
        ret['messages'].append( "UnrecognizeFileType" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    if '0' in ROMSAFtransmitter:
        if 'G' in ROMSAFtransmitter:
            ROMSAFtransmitter = 'G' + ROMSAFtransmitter[2:4]
        if 'R' in ROMSAFtransmitter:
            ROMSAFtransmitter = 'R' + ROMSAFtransmitter[2:4]

    ROMSAFcode = m.group(8)
    ROMSAFversion = m.group(9)
    ROMSAFsubversion = m.group(10)
    cal = Calendar(year=ROMSAFyear, month=ROMSAFmonth, day=ROMSAFday, hour=ROMSAFhour,
             minute=ROMSAFminute, second=ROMSAFsecond)

    ROMSAFdoy = cal.doy

#  Search for ROMSAF mission name.

    ROMSAFmission = None
    for m in valid_missions[processing_center]:
        if m in head:
            ROMSAFmission = m
            break

#  Enter definition of mission and receiver names.

    input_file_type = ROMSAFfiletype
    transmitter = ROMSAFtransmitter
    version = f"{ROMSAFversion}.{ROMSAFsubversion}"

#  Single satellite RO missions. YET TO BE IMPLEMENTED.

    sats = get_receiver_satellites( processing_center, mission=ROMSAFmission, receiver=ROMSAFreceiver )
    if len( sats ) != 1:
        comment = 'Indeterminant LEO identification: the search for receiver ' + \
                f'"{ROMSAFreceiver}" returned {len(sats)} LEO satellites.'
        ret['status'] = "fail"
        ret['messages'].append( "UnrecognizedLEO" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    sat = sats[0]
    receiver = sat['aws']['receiver']
    mission = sat['aws']['mission']

#  Done.

    ret.update( { 'status': "success",
            'mission': mission,
            'receiver': receiver,
            'transmitter': transmitter,
            'version': version,
            'processing_center': processing_center,
            'input_file_type': input_file_type,
            'time': cal } )

    return ret


################################################################################
#  ROMSAF "atm" reformatter.
################################################################################

def level2a2aws(inputfile, outputfile, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, setting=None, **extra ):
    """Translate a ROMSAF atm file into an AWS Open Data Registry
    refractivityRetrieval file format. inputfile is the path to the input
    ROMSAF atm file; outputfile is the path to the output refractivityRetrieval
    file; mission is the name of the mission; receiver is the name of the
    receiving satellite; and transmitter is the name of the transmitter in
    3-character RINEX-3 format. processing_center_version is the version as
    returned by _varnames, and processing_center_path is the path to the file
    contributed by UCAR that is converted; version is an element of the 
    Versions.versions list as returned by Versions.get_version(); setting is a 
    Boolean that gives the occultation geometry, setting=True for a setting 
    occultation, setting=False for a rising occultation. 

    The returned output is a dictiony, key "status" having a value of
    "success" or "fail", key "messages" having a value that is a list of
    mnemonic messages of processing, key "comments" having a value that
    is a list of comments from processing, and key "metadata" having a
    value that is a dictionary of occultation metadata extracted from the
    data file."""

    level = "level2a"

    #  Log run.

    LOGGER.info( "Running level2a2aws: " + json.dumps( {
        'inputfile': inputfile, 'outputfile': outputfile,
        'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
        'processing_center_path': processing_center_path } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Find the file formatter.

    fileformatter = version[level]
    required_RO_order = version['module'].required_RO_order
    required_met_order = version['module'].required_met_order

    #  Open input file.

    try:
        d = Dataset( inputfile, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"Cannot open {inputfile} for input"
        ret['messages'].append( "CannotOpenInputFile" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    #  Get lists of global attribute names and variable names.

    ncattrs = list( d.ncattrs() )
    ncattrs.sort()

    inputfile_variables = list( d.variables.keys() )
    inputfile_variables.sort()

    #  Read in the reference time of the occultation.

    refTime = Time( utc=Calendar(year=2000,month=1,day=1) ) + d.variables['time'][:].data[0]
    cal = refTime.calendar( "utc" )

    #  Get reference longitude, latitude, local_time, gps_seconds, setting.
    #  Note: no information on rising v setting occultation.

    x = d.variables['time'][0] / 3600.0  + d.variables['lon'][0] / 15.0 # local time in hours
    x *= np.pi / 12                                                     # convert to radians
    local_time = np.arctan2( -np.sin(x), -np.cos(x) ) * 12/np.pi + 12   # back to hours

    ret['metadata'].update( {
            'longitude': d.variables['lon'][0].data,
            'latitude': d.variables['lat'][0].data,
            'local_time': local_time
            } )

    #  Create output file.

    try:
        head, tail = os.path.split( outputfile )
        if head != '':
            if not os.path.isdir( head ):
                ret['comments'].append( f"Creating directory {head}" )
                LOGGER.info( f"Creating directory {head}" )
                os.makedirs( head, exist_ok=True )        
        e = Dataset( outputfile, 'w', clobber=True, format='NETCDF4' )
    except:
        ret['status'] = "fail"
        comment = f"Cannot create {outputfile} for output"
        ret['messages'].append( "CannotCreateOutputFile" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        d.close()
        return ret

    #  What signals are in the input file?

    signals = receiversignals(transmitter, receiver, refTime.calendar("utc").datetime())
    if len( signals ) == 0:
        ret['status'] = "fail"
        comment = f"No signals defined for {transmitter=}, {receiver=} at {refTime.calendar('utc').isoformat()}"
        ret['messages'].append( "NoReceiverSignalsDefined" )
        ret['comments'].append( comment )
        return ret

    #  Make a dictionary that defines the dimensions of the output
    #  netCDF file variables.

    for dim in [ 'dim_lev1b', 'dim_lev2a' ]:
        if dim not in d.dimensions.keys():
            ret['status'] = "fail"
            comment = f"{dim} not a dimension in input file {inputfile}"
            ret['messages'].append( "InvalidInputFile" )
            ret['comments'].append( comment )
            LOGGER.warning( comment )
            d.close()
            e.close()
            return ret

    nimpacts = d.dimensions['dim_lev1b'].size
    nlevels = d.dimensions['dim_lev2a'].size

    #  Select the references. 

    references = None
    cal_datetime = cal.datetime()

    for rec in retrieval_references: 
        if rec['mission'] == mission: 
            if rec['datetimerange'][1] is None: 
                if rec['datetimerange'][0] <= cal_datetime: 
                    references = rec['references']
                    break
            else: 
                if rec['datetimerange'][0] <= cal_datetime and cal_datetime < rec['datetimerange'][1]: 
                    references = rec['references']
                    break

    if references is None: 
        ret['status'] = "fail"
        comment = "No valid references found"
        ret['messages'].append( "NoValidReferences" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        d.close()
        e.close()
        return ret

    #  Output file template.

    outvars = fileformatter( e,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, optimization_references, ionospheric_references,
        references, nimpacts, nlevels, cal.datetime(), mission, transmitter, receiver, centerwmo=centerwmo )

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
        m = re.search( r"(^.*)\.nc", os.path.basename( outputfile ) )
        e.setncatts( { 'GranuleID': m.group(1) } )

    #  Screen calibration data and determine whether to flip or not.

    impact_parameter = d.variables['impact']
    impact_good = screen( impact_parameter )
    RO_ascending = ( impact_parameter[0,impact_good[1]] > impact_parameter[0,impact_good[0]] )
    flip_RO = ( bool(RO_ascending) == bool( required_RO_order == "descending" ) )

    if flip_RO:
        impact_iout = np.flip( nimpacts - 1 - impact_good )
    else:
        impact_iout = impact_good

    #  Screen met data and determine whether to flip or not.

    geop = d.variables['geop_refrac']
    level_good = screen( geop )
    met_ascending = ( geop[0,level_good[1]] > geop[0,level_good[0]] )
    flip_met = ( bool(met_ascending) == bool( required_met_order == "descending" ) )

    if flip_met:
        level_iout = np.flip( nlevels - 1 - level_good )
    else:
        level_iout = level_good

    #  Calculate dry pressure.

    dryTemperature = d.variables['dry_temp'][0,:]
    refractivity = d.variables['refrac'][0,:]

    #  Use Aparicio and Laroche formulation for refractivity: doi:10.1029/2010JD015214.
    #  First, evaluate compressibility, assuming that dry temperature is
    #  approximately the same as kinetic temperature.

    tau = ( freezing_point / dryTemperature - 1 )
    dryDensity = refractivity / ( a1 + a2 * tau )
    dryPressure = dryDensity * dryTemperature * Rgas / mudry

    #  Fill the output variables with data from the input ROMSAF
    #  data structures.

    if "refTime" in outvarsnames:
        outvars['refTime'].assignValue( refTime - gps0 )
    if "refLongitude" in outvarsnames:
        outvars['refLongitude'].assignValue( d.variables['lon'][0].data )
    if "refLatitude" in outvarsnames:
        outvars['refLatitude'].assignValue( d.variables['lat'][0].data )
    if "equatorialRadius" in outvarsnames:
        outvars['equatorialRadius'].assignValue( semi_major_axis )
    if "polarRadius" in outvarsnames:
        outvars['polarRadius'].assignValue( semi_minor_axis )
    if "undulation" in outvarsnames:
        outvars['undulation'].assignValue( d.variables['undulation'][0].data )
    if "centerOfCurvature" in outvarsnames:
        outvars['centerOfCurvature'][:] = d.variables['r_coc'][:].data.squeeze()
    if "radiusOfCurvature" in outvarsnames:
        outvars['radiusOfCurvature'].assignValue( d.variables['roc'][0].data )

    #  Occultation geometry. First try to obtain information on occultation
    #  geometry from the input data file itself. If the information is not
    #  present, then try to get that information from the keyword "setting".
    #  If that is not present, then leave the "setting" variable unfilled in
    #  the output file.

    try:
        dsetting = bool( ( d.variables['pcd'][0].data & 4 ) == 0 )
    except:
        dsetting = None

    if dsetting is None and setting is not None:
        dsetting = bool( setting )

    if dsetting is not None and "setting" in outvarsnames:
        ret['metadata'].update( { 'setting': dsetting } )
        if dsetting:
            outvars['setting'].assignValue( 1 )
        else:
            outvars['setting'].assignValue( 0 )

    if "carrierFrequency" in outvarsnames:

        #  Carrier frequency: L1

        for signal in signals:
            if signal['standardName'] in [ 'C/A', 'L1' ]:
                outvars['carrierFrequency'][0] = carrierfrequency( transmitter, cal.datetime(), signal['rinex3name'] )
                break

        #  Carrier frequency: L2

        for signal in signals:
            if signal['standardName'] in [ 'L2' ]:
                outvars['carrierFrequency'][1] = carrierfrequency( transmitter, cal.datetime(), signal['rinex3name'] )
                break

    #  Bending angle profile variables.

    if flip_RO:
        if "rawBendingAngle" in outvarsnames:
            outvars['rawBendingAngle'][impact_iout,0] = np.flip( d.variables['bangle_L1'][:].data.squeeze()[impact_good] )
            outvars['rawBendingAngle'][impact_iout,1] = np.flip( d.variables['bangle_L2'][:].data.squeeze()[impact_good] )
        if "impactParameter" in outvarsnames:
            outvars['impactParameter'][impact_iout] = np.flip( d.variables['impact_opt'][:].data.squeeze()[impact_good] )
        if "bendingAngle" in outvarsnames:
            outvars['bendingAngle'][impact_iout] = np.flip( d.variables['bangle'][:].data.squeeze()[impact_good] )
        if "optimizedBendingAngle" in outvarsnames:
            outvars['optimizedBendingAngle'][impact_iout] = np.flip( d.variables['bangle_opt'][:].data.squeeze()[impact_good] )
        if "bendingAngleUncertainty" in outvarsnames:
            outvars['bendingAngleUncertainty'][impact_iout] = np.flip( d.variables['bangle_sigma'][:].data.squeeze()[impact_good] )

    else:
        if "rawBendingAngle" in outvarsnames:
            outvars['rawBendingAngle'][impact_iout,0] = d.variables['bangle_L1'][:].data.squeeze()[impact_good]
            outvars['rawBendingAngle'][impact_iout,1] = d.variables['bangle_L2'][:].data.squeeze()[impact_good]
        if "impactParameter" in outvarsnames:
            outvars['impactParameter'][impact_iout] = d.variables['impact_opt'][:].data.squeeze()[impact_good]
        if "optimizedBendingAngle" in outvarsnames:
            outvars['optimizedBendingAngle'][impact_iout] = d.variables['bangle_opt'][:].data.squeeze()[impact_good]
        if "bendingAngleUncertainty" in outvarsnames:
            outvars['bendingAngleUncertainty'][impact_iout] = d.variables['bangle_sigma'][:].data.squeeze()[impact_good]

    #  Atmospheric profile variables.

    if flip_met:
        if "altitude" in outvarsnames:
            outvars['altitude'][level_iout] = np.flip( d.variables['alt_refrac'][:].data.squeeze()[level_good] )
        if "longitude" in outvarsnames:
            outvars['longitude'][level_iout] = np.flip( d.variables['lon_tp'][:].data.squeeze()[level_good] )
        if "latitude" in outvarsnames:
            outvars['latitude'][level_iout] = np.flip( d.variables['lat_tp'][:].data.squeeze()[level_good] )
        if "orientation" in outvarsnames:
            outvars['orientation'][level_iout] = np.flip( d.variables['azimuth_tp'][:].data.squeeze()[level_good] )
        if "geopotential" in outvarsnames:
            outvars['geopotential'][level_iout] = np.flip( d.variables['geop_refrac'][:].data.squeeze()[level_good] * gravity )
        if "refractivity" in outvarsnames:
            outvars['refractivity'][level_iout] = np.flip( d.variables['refrac'][:].data.squeeze()[level_good] )
        if "dryPressure" in outvarsnames:
            outvars['dryPressure'][impact_iout] = np.flip( dryPressure[impact_good] )
        if "quality" in outvarsnames:
            outvars['quality'][level_iout] = np.flip( d.variables['refrac_qual'][0,level_good] )

    else:
        if "altitude" in outvarsnames:
            outvars['altitude'][level_iout] = d.variables['alt_refrac'][:].data.squeeze()[level_good]
        if "longitude" in outvarsnames:
            outvars['longitude'][level_iout] = d.variables['lon_tp'][:].data.squeeze()[level_good]
        if "latitude" in outvarsnames:
            outvars['latitude'][level_iout] = d.variables['lat_tp'][:].data.squeeze()[level_good]
        if "orientation" in outvarsnames:
            outvars['orientation'][level_iout] = d.variables['azimuth_tp'][:].data.squeeze()[level_good]
        if "geopotential" in outvarsnames:
            outvars['geopotential'][level_iout] = d.variables['geop_refrac'][:].data.squeeze()[level_good] * gravity
        if "refractivity" in outvarsnames:
            outvars['refractivity'][level_iout] = d.variables['refrac'][:].data.squeeze()[level_good]
        if "dryPressure" in outvarsnames:
            outvars['dryPressure'][impact_iout] = dryPressure[impact_good]
        if "quality" in outvarsnames:
            outvars['quality'][level_iout] = d.variables['refrac_qual'][0,level_good]

    #  Mean orientation.

    if "orientation" in outvarsnames:
        orientations = outvars['orientation'][:]
        dx = np.deg2rad( orientations - orientations.mean() )
        meanOrientation = orientations.mean() + np.arctan2( np.sin(dx), np.cos(dx) ).mean()
        meanOrientation = np.rad2deg( np.arctan2( np.sin(meanOrientation), np.cos(meanOrientation) ) )
        ret['metadata'].update( { 'orientation': meanOrientation } )

    #  Done.

    d.close()
    e.close()

    ret['status'] = "success"

    return ret


################################################################################
#  ROMSAF "wet" reformatter.
################################################################################

def level2b2aws( inputfile, outputfile, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, setting=None, **extra ):
    """Translate a ROMSAF wet file into an AWS Open Data Registry
    atmosphericRetrieval file format.  inputfile is the path to the input
    ROMSAF wet file; outputfile is the path to the output atmosphericRetrieval
    file; mission is the name of the mission; transmitter is the name of the 
    transmitter in 3-character RINEX-3 format; receiver is the name of the
    receiving satellite; processing_center_version is the version as returned 
    by _varnames; processing_center_path is the path to the file contributed 
    by the ROMSAF that is converted; version is a member of the 
    Versions.versions list, usually as returned by Versions.get_version(); 
    setting is a Boolean that gives the occultation geometry, setting=True 
    for a setting occultation, setting=False for a rising occultation.

    The returned output potentially contains several pieces of data contained
    in the input file that would be useful for the DynamoDB database:
    longitude, latitude, local_time, gps_seconds, setting."""

    level = "level2b"

    #  Log run.

    LOGGER.info( "Running level2b2aws: " + json.dumps( {
        'inputfile': inputfile, 'outputfile': outputfile,
        'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
        'processing_center_path': processing_center_path } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Find the file formatter.

    fileformatter = version[level]
    required_RO_order = version['module'].required_RO_order
    required_met_order = version['module'].required_met_order

    #  Open input file.

    try:
        d = Dataset( inputfile, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"{inputfile} could not be found or is not a NetCDF file"
        ret['messages'].append( "InvalidInputFile" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Get lists of global attribute names and variable names.

    ncattrs = list( d.ncattrs() )
    ncattrs.sort()

    inputfile_variables = list( d.variables.keys() )
    inputfile_variables.sort()

    #  Read in the reference time of the occultation.

    refTime = Time( utc=Calendar(year=2000,month=1,day=1) ) + d.variables['time'][:].data[0]
    cal = refTime.calendar( "utc" )

    #  Get reference longitude, latitude, local_time, gps_seconds, setting.
    #  Note: no information on rising v setting occultation.

    x = d.variables['time'][0] / 3600.0  + d.variables['lon'][0] / 15.0 # local time in hours
    x *= np.pi / 12                                                     # convert to radians
    local_time = np.arctan2( -np.sin(x), -np.cos(x) ) * 12/np.pi + 12    # back to hours

    ret['metadata'].update( {
            'longitude': d.variables['lon'][0].data,
            'latitude': d.variables['lat'][0].data,
            'local_time': local_time
            } )

    #  Create output file.

    try:
        head, tail = os.path.split( outputfile )
        if head != '':
            if not os.path.isdir( head ):
                ret['comments'].append( f"Creating directory {head}" )
                LOGGER.info( f"Creating directory {head}" )
                os.makedirs( head, exist_ok=True )          
        e = Dataset( outputfile, 'w', clobber=True, format='NETCDF4' )
    except:
        ret['status'] = "fail"
        comment = f"Cannot create output file {outputfile}"
        ret['messages'].append( "InvalidOutputFile" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    #  Define the output dimension.

    nlevels = d.dimensions['dim_lev2b'].size

    #  Screen the input data.

    good = screen( d.variables['shum'] )
    if len(good) == 0:
        ret['status'] = "fail"
        comment = "No valid data in profile"
        ret['messages'].append( "NoValidData" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        d.close()
        e.close()
        return ret

    #  Flip the output profile?

    met_ascending = ( d.variables['geop'][0,1] > d.variables['geop'][0,0] )
    flip = ( bool(met_ascending) == bool( required_met_order == "descending" ) )

    if flip:
        iout = np.flip( nlevels - 1 - good )
    else:
        iout = good

    #  Select the references. 

    references = None
    cal_datetime = cal.datetime()

    for rec in retrieval_references: 
        if rec['mission'] == mission: 
            if rec['datetimerange'][1] is None: 
                if rec['datetimerange'][0] <= cal_datetime: 
                    references = rec['references']
                    break
            else: 
                if rec['datetimerange'][0] <= cal_datetime and cal_datetime < rec['datetimerange'][1]: 
                    references = rec['references']
                    break

    if references is None: 
        ret['status'] = "fail"
        comment = "No valid references found"
        ret['messages'].append( "NoValidReferences" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        d.close()
        e.close()
        return ret

    #  Output file template.

    outvars = fileformatter( e,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, references, 
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
        m = re.search( r"(^.*)\.nc", os.path.basename( outputfile ) )
        e.setncatts( { 'GranuleID': m.group(1) } )

    #  Compute water vapor pressure (hPa) and refractivity.

    temperature = d.variables['temp'][0,good].data                  #  K
    pressure = d.variables['press'][0,good].data * 100.0            #  Convert to Pa
    specificHumidity = d.variables['shum'][0,good].data / 1000.0    #  Convert to kg/kg

    waterVaporPressure = ( specificHumidity / muvap ) \
        / ( specificHumidity / muvap + ( 1 - specificHumidity ) / mudry ) \
        * pressure

    #  Aparicio and Laroche again.

    tau = ( freezing_point / temperature - 1 )
    dryDensity = mudry * ( pressure - waterVaporPressure ) / ( Rgas * temperature )
    vaporDensity = muvap * ( waterVaporPressure ) / ( Rgas * temperature )
    refractivity = dryDensity * ( a1 + a2 * tau ) + vaporDensity * ( a3 + a4 * tau )

    #  Altitude is complicated. It must be inverted from geopotential.

    #  Generate geopotential vs. altitude using a full gravity model, JGM3.

    lon, lat = d.variables['lon'][0], d.variables['lat'][0]
    profile_altitude = np.arange( -1.0, 120.001, 0.1 )                                      # km
    profile_geopotential = JGM3geopotential( lon, lat, profile_altitude, geoidref=True )    # J/kg

    #  Generate a cubic spline interpolation function based on this profile.

    getalt = interp1d( profile_geopotential, profile_altitude )

    #  Interpolate altitude in the wet file using the cubic spline interpolator.

    wet_geopotential = d.variables['geop'][:].data[0,good] * gravity
    wet_altitude = getalt( wet_geopotential ) * 1000.0      #  Convert to m.

    #  Scalar variables.

    if "refTime" in outvarsnames:
        outvars['refTime'].assignValue( refTime - gps0 )
    if "refLongitude" in outvarsnames:
        outvars['refLongitude'].assignValue( d.variables['lon'][0].data )
    if "refLatitude" in outvarsnames:
        outvars['refLatitude'].assignValue( d.variables['lat'][0].data )

    #  Occultation geometry. First try to obtain information on occultation
    #  geometry from the input data file itself. If the information is not
    #  present, then try to get that information from the keyword "setting".
    #  If that is not present, then leave the "setting" variable unfilled in
    #  the output file.

    try:
        dsetting = bool( ( d.variables['pcd'][0].data & 4 ) == 0 )
    except:
        dsetting = None

    if dsetting is None and setting is not None:
        dsetting = bool( setting )

    if dsetting is not None:
        ret['metadata'].update( { 'setting': dsetting } )
        if dsetting:
            outvars['setting'].assignValue( 1 )
        else:
            outvars['setting'].assignValue( 0 )

    #  Profile variables.

    if flip:
        if "altitude" in outvarsnames:
            outvars['altitude'][iout] = np.flip( wet_altitude )
        if "geopotential" in outvarsnames:
            outvars['geopotential'][iout] = np.flip( wet_geopotential )
        if "refractivity" in outvarsnames:
            outvars['refractivity'][iout] = np.flip( refractivity )
        if "pressure" in outvarsnames:
            outvars['pressure'][iout] = np.flip( pressure ) * 100
        if "temperature" in outvarsnames:
            outvars['temperature'][iout] = np.flip( temperature )
        if "waterVaporPressure" in outvarsnames:
            outvars['waterVaporPressure'][iout] = np.flip( waterVaporPressure )
        if "quality" in outvarsnames:
            outvars['quality'][iout] = np.flip( d.variables['meteo_qual'][0,good] )

    else:
        if "altitude" in outvarsnames:
            outvars['altitude'][iout] = wet_altitude
        if "geopotential" in outvarsnames:
            outvars['geopotential'][iout] = wet_geopotential
        if "refractivity" in outvarsnames:
            outvars['refractivity'][iout] = refractivity
        if "pressure" in outvarsnames:
            outvars['pressure'][iout] = pressure
        if "temperature" in outvarsnames:
            outvars['temperature'][iout] = temperature
        if "waterVaporPressure" in outvarsnames:
            outvars['waterVaporPressure'][iout] = waterVaporPressure
        if "quality" in outvarsnames:
            outvars['quality'][iout] = d.variables['meteo_qual'][:][0,good]

    #  Done.

    d.close()
    e.close()

    ret['status'] = "success"

    return ret
