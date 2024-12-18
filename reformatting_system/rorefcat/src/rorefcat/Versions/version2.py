"""This package contains a Library of functions that create dimensions, variable
definition, and attributes for the level1b, level2a, and level2b file formats.
Its exports are

Parameters: AWSversion

format_level1b
==============================
Create dimensions, global attributes, variables and their attributes for the
AWS-native level1b file, which contains calibrated/excess phase and precise
orbit data.

format_level2a
==============================
Create dimensions, global attributes, variables and their attributes for the
AWS-native level2a file, which contains bending angle and refractivity data.

format_level2b
==============================
Create dimensions, global attributes, variables and their attributes for the
AWS-native level2b file, which contains atmospheric data such as pressure,
temperature, water vapor.

file_indexing
==============================
This is a dictionary containing the indexing terms that define the file 
type used in the database. 

"""


#  Standard imports.

import os
import json
import numpy as np
from datetime import datetime
from ..Missions import get_receiver_satellites
from ..Utilities.TimeStandards import Time

#  Logging.

import logging
LOGGER = logging.getLogger( __name__ )


################################################################################
#  Parameters.
################################################################################

#  AWS processing version.

AWSversion = "2.0"

#  This is the required ordering of RO variables (bending angle, impact
#  parameter) in height and the ordering of meteorological variables
#  (refractivity, pressure, temperature, water vapor pressure, altitude)
#  in altitude. Valid values are "ascending" and "descending" for each.

required_RO_order = "descending"
required_met_order = "ascending"

#  File indexing conventions.

file_indexing = {
        'level1b': "gnssrol1bv2",
        'level2a': "gnssrol2av2",
        'level2b': "gnssrol2bv2" }

#  Database variables to be saved in DynamoDB table. This is not the
#  only information to be saved in the table, though. Other information
#  is contained in the partition key and sort key. Still more is
#  contained in the pointers to the data files by processing center and
#  file type.

database_variables = {
        'longitude': float,             # degrees east, -180 - 180
        'latitude': float,              # degrees north, -90 - 90
        'local_time': float,            # hours, 0-24
        'orientation': float,           # degrees east of north, -180 - 180
        'setting': bool,                # True for setting, False for rising
        'mission': str,                 # as defined in documentation
        'transmitter': str,             # 3-char GNSS prn; e.g., "G03"
        'receiver': str,                # as defined in documentation
        'gps_seconds': float,           # start time of the occultation in GPS seconds
        'occ_duration': float }         # duration of the occultation in seconds

#  Fill values.

_FillValue_byte = np.byte( -128 )
_FillValue_char = np.byte( 0 ).astype('c')
_FillValue_int = np.int32( -999 )
_FillValue_float = np.float32( -9.99e20 )
_FillValue_double = np.float64( -9.99e20 )

#  For testing...

TEST = os.getenv( "TEST" )

#  S3 buckets and DynamoDB table.

if TEST is None:
    from ..Utilities.resources import stagingBucket
    loggingRoot = f"logs/{AWSversion.replace('.','_')}/{datetime.now().strftime('%Y%m%d')}"
    definitionsBucket = "gnss-ro-processing-definitions"
    dynamodbTable = f"gnss-ro-data-stagingv{AWSversion.replace('.','_')}"
    batchJobsRoot = "batchprocess-jobs"
else: 
    stagingBucket = "gnss-ro-data-test"
    loggingRoot = f"logs/{AWSversion.replace('.','_')}/{datetime.now().strftime('%Y%m%d')}"
    definitionsBucket = "gnss-ro-data-test"
    dynamodbTable = f"gnss-ro-test-v2"
    batchJobsRoot = "batchprocess-jobs"


################################################################################
#  Output file/path definition. 
################################################################################

def ShortName( level, mission, processing_center ): 
    """Define the "short name" for the file. level should be the level 
    designation, such as "1b", "2a", etc. The mission should be the AWS-convention 
    name of the occultation mission. The processing_center should be one of the 
    processing centers defined in Reformatters."""

    return f"gnssro_{mission}_{processing_center}_l{level.lower()}"


def defpath( file_type, processing_center, mission, transmitter, receiver, 
             time, occid, center_version ):
    """Defines relative path to processed file according to the AWS file_type 
    ("level1b", "level2a", "level2b"), processing_center, mission, 
    transmitter, receiver, time (instance of Utilities.TimeStandards.Calendar), 
    center processing version (center_version). The mission and receiver must 
    be AWS definitions of those quantities. The output path has the mission 
    name as its root.
    """

    LOGGER.debug( "Running _defpath: " +
            json.dumps( { 'file_type': file_type, 'processing_center': processing_center,
                'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
                'time': time.isoformat() } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [] }

#  Define the directory hierarchy.

    path_naming = file_indexing 

    path = os.path.join( "contributed", "v"+AWSversion, processing_center, 
            mission, path_naming[file_type], "{:4d}".format( time.year ), 
            "{:02d}".format( time.month ), "{:02d}".format( time.day ) )

#  Define the file name.

    level = file_type[5:]
    shortname = ShortName( level, mission, processing_center )
    path = os.path.join( path, f"{shortname}_{center_version}_{occid}.nc4" )

#  Done.

    ret.update( { 'status': "success", 'value': path } )

    return ret


################################################################################
#  File formatters.
################################################################################

def format_level1b( output,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, retrieval_references, ntimes, nsignals, time, mission,
        transmitter, receiver, **optional ): 

    """Define dimensions, create variables and their attributes, and create
    global attributes for the level1b file format. "output" is a
    netCDF4.Dataset object or group.

    processing_center           ucar, romsaf, jpl, etc.
    processing_center_version   Version of retrieval provided by the processing center
    processing_center_path      Relative path to the file being translated
    data_use_license            URL for the data use license designated by the processing center
    retrieval_references        List of DOI ("doi:???") of references for the retrieval system
    ntimes                      The number of high-rate times
    nsignals                    The number of occultation signals to be translated
    time                        datetime.datetime instance reference time of occultation
    mission                     The AWS name of the mission
    transmitter                 The 3-character definition of the transmitter
    receiver                    The AWS name of the LEO receiver
    referencesat                The 3-character definition of the reference satellite
    referencestation            The name of the double-difference reference ground station
    optional                    A dictionary containing optional arguments, such as 
                                referencesat, referencestation, centerwmo, starttime 
                                (in GPS seconds), etc. 
    """

    #  Define the level. 

    level = "1b"

    #  Global attributes.

    output.setncatts( {
            'title': 'This file contains calibrated excess phase and amplitude ' + \
                    'data vs. time for one radio occultation sounding.',
            'ShortName': ShortName( level, mission, processing_center ), 
            'LongName': "GNSS Radio Occultation L1B Calibrated Phase for " + \
                    "{:} with {:} retrieval algorithm {:}".format( mission.upper(), processing_center.upper(), 
                    processing_center_version ),
            'VersionID': "2.0",
            'Format': "NetCDF4",
            'IdentifierProductDOIAuthority': "https://dx.doi.org/???",
            'IdentifierProductDOI': "10.5067/????",
            'ProductionDateTime': datetime.utcnow().isoformat(timespec="seconds")+"Z",
            'ProcessingLevel': level.upper(), 
            'Conventions': "CF-1.10",
            'source': "GNSS radio occultation",
            'institution': processing_center,
            'institution_version': processing_center_version,
            'institution_path': processing_center_path,
            'data_use_license': data_use_license,
            'references': retrieval_references, 
            'DataSetQuality': "GNSS radio occultation has been extensively validated against " + \
                    "radiosonde soundings (doi:10.1029/2004GL021443) and is routinely assimilated " + \
                    "in numerical weather prediction", 
            'ShortName': "gnssro_{:}_{:}_{:}".format( mission, processing_center, "l1b" ), 
            'LongName': "GNSS radio occultation L1B excess phase and amplitude for " + \
                    "{:} as contributed by {:}, version 2.0".format( mission.upper(), processing_center.upper() ), 
            'RangeBeginningDate': "", 
            'RangeBeginningTime': "", 
            'RangeEndingDate': "", 
            'RangeEndingTime': "", 
            'GranuleID': "" 
        } )

    #  Global attributes: time.

    doy = ( time - datetime(year=time.year,month=1,day=1) ).days + 1
    output.setncatts( {
        'year': np.int32( time.year ),
        'month': np.int32( time.month ),
        'day': np.int32( time.day ),
        'hour': np.int32( time.hour ),
        'minute': np.int32( time.minute ),
        'second': float( time.second ),
        'doy': np.int32( doy ) } )

    #  Global attributes: satellites and reference stations (if available).

    output.setncatts( {
        'mission': mission,
        'receiver': receiver,
        'transmitter': transmitter } )

    if 'referencesat' in optional.keys(): 
        if optional['referencesat'] is not None: 
            output.setncatts( { 'reference_transmitter': optional['referencesat'] } )
        else: 
            output.setncatts( { 'reference_transmitter': "" } )
    else:
        output.setncatts( { 'reference_transmitter': "" } )

    if 'referencestation' in optional.keys():
        if optional['referencestation'] is not None: 
            output.setncatts( { 'reference_ground_station': optional['referencestation'] } )
        else: 
            output.setncatts( { 'reference_ground_station': "" } )
    else:
        output.setncatts( { 'reference_ground_station': "" } )

    #  Get the receiver satellite object, write the satellite ID, the instrument ID, 
    #  and the satellite sub-identifier to global attributes. 

    receiver_sat = get_receiver_satellites( "aws", receiver=receiver )[0]

    if "wmo" in receiver_sat.keys(): 
        receiver_wmo = receiver_sat['wmo']
        if 'satellite_subid' in receiver_wmo.keys(): 
            output.setncatts( { 'wmo_satellite_identifier': np.int32(receiver_wmo['satellite_id']), 
                           'wmo_satellite_sub_identifier': np.int32(receiver_wmo['satellite_subid']), 
                           'wmo_instrument_identifier': np.int32(receiver_wmo['instrument_id']) } )
        else: 
            output.setncatts( { 'wmo_satellite_identifier': np.int32(receiver_wmo['satellite_id']), 
                           'wmo_satellite_sub_identifier': np.int32(0), 
                           'wmo_instrument_identifier': np.int32(receiver_wmo['instrument_id']) } )
    else: 
        output.setncatts( { 'wmo_satellite_identifier': np.int32(0), 
                           'wmo_satellite_sub_identifier': np.int32(0), 
                           'wmo_instrument_identifier': np.int32(0) } )

    #  Write WMO identifiers of originating center. 

    if 'centerwmo' in optional.keys(): 

        opt = optional['centerwmo']

        if 'originating_center_id' in opt.keys(): 
            output.setncatts( { 'wmo_originating_center_identifier': np.int32(opt['originating_center_id']) } )
        else: 
            output.setncatts( { 'wmo_originating_center_identifier': np.int32(0) } )

        if 'originating_subcenter_id' in opt.keys(): 
            output.setncatts( { 'wmo_originating_center_sub_identifier': np.int32(opt['originating_subcenter_id']) } )
        else: 
            output.setncatts( { 'wmo_originating_center_sub_identifier': np.int32(0) } )

    else: 

        output.setncatts( { 'wmo_originating_center_identifier': np.int32(0), 
                           'wmo_originating_center_sub_identifier': np.int32(0) } )

    #  Conventions, history, comment. 

    output.setncatts( { 'Conventions': "CF-1.10", 
            'history': "27 November 2023", 
            'comment': "Radio occultation data using the satellites of the " + \
                    "Global Navigation Satellite Systems (GNSS), hosted in " + \
                    "the Registry of Open Data of Amazon Web Services" } )

    #  Create NetCDF dimensions.

    output.createDimension( 'time', ntimes )
    output.createDimension( 'obscode', 3 )
    output.createDimension( 'cartesian', 3 )
    output.createDimension( 'signal', nsignals )

    #  Create NetCDF variables.

    outvars = {}

    varname = 'start_time'
    var = output.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
            'long_name': "start time of occultation", 
            'comment': 'Reference start time of the occultation at ' + \
                            'the receiver in GPS time; i.e., time elapsed since ' + \
                            'January 6, 1980, 00:00Z',
            'units': "seconds since 1980-01-06 00:00:00 UTC", 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'startTime': var } )

    varname = 'end_time'
    var = output.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
            'long_name': "end time of occultation", 
            'comment': 'Time of last data point at ' + \
                            'the receiver in GPS time; i.e., time elapsed since ' + \
                            'January 6, 1980, 00:00Z',
            'units': "seconds since 1980-01-06 00:00:00 UTC", 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'endTime': var } )

    varname = 'obscode'
    var = output.createVariable( varname, np.dtype('b'), dimensions=('obscode',) )
    var.setncatts( {
            'long_name': "observation code string index", 
            'comment': 'dummy variable: allocate three characters for RINEX 3 observation codes',
            'units': "1", 
            '_FillValue': _FillValue_byte } )
    var[:] = np.arange(3)

    varname = 'cartesian'
    var = output.createVariable( varname, np.dtype('c'), dimensions=('cartesian',) )
    var.setncatts( {
            'long_name': "cartesian coodinate label", 
            'comment': 'Cartesian coordinate system axes label',
            'units': "1", 
            '_FillValue': _FillValue_char } )
    var[:] = "xyz"

    varname = 'signal'
    var = output.createVariable( varname, np.dtype('b'), dimensions=('signal',) )
    var.setncatts( {
            'long_name': "signal string index", 
            'comment': 'dummy variable: allocate dimension for tracked GNSS signals',
            'units': "1", 
            '_FillValue': _FillValue_byte } )
    var[:] = np.arange( nsignals )

    varname = 'nav_bits_present'
    var = output.createVariable( varname, np.dtype('b'), dimensions=('signal',) )
    var.setncatts( {
            'long_name': "presence of navigation bits", 
            'comment': 'This logical variable declares whether the ' + \
                    'bit-stream of a navigation message was removed in the ' + \
                    'generation of excess phase. If non-zero, then the ' + \
                    'navigation message bits have been removed; if zero, ' + \
                    'then the navigation message bits have not been removed. ' + \
                    'If the signal is a data-less "pilot" tone, meaning there is ' + \
                    'no navigation message on the signal, the variable is ' + \
                    'meaningless but is set to True nonetheless.',
            'units': "1", 
            '_FillValue': _FillValue_byte } )
    outvars.update( { 'navBitsPresent': var } )

    varname = 'snr_observation_code'
    var = output.createVariable( varname, 'c', dimensions=('signal','obscode') )
    var.setncatts( {
            'long_name': "observation code for SNR", 
            'comment': 'RINEX 3 observation code for the signal-to-noise ' + \
                    'ratio observation',
            'units': "1", 
            '_FillValue': _FillValue_char } )
    outvars.update( { 'snrCode': var } )

    varname = 'phase_observation_code'
    var = output.createVariable( varname, 'c', dimensions=('signal','obscode') )
    var.setncatts( {
            'long_name': "observation code for phase", 
            'comment': 'RINEX 3 observation code for the excess phase ' + \
                    'observation',
            'units': "1", 
            '_FillValue': _FillValue_char } )
    outvars.update( { 'phaseCode': var } )

    varname = 'carrier_frequency'
    var = output.createVariable( varname, np.dtype('f8'), dimensions=('signal',) )
    var.setncatts( {
            'long_name': "carrier frequency", 
            'comment': "Carrier frequency of the tracked GNSS signal",
            'units': "Hz", 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'carrierFrequency': var } )

    if "starttime" in optional.keys(): 
        start_time = Time( gps=optional['starttime'] )
        epochstr = start_time.calendar("utc").datetime().strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    else: 
        epochstr = "start_time"

    varname = 'time'
    var = output.createVariable( varname, np.dtype('f8'), dimensions=('time',) )
    var.setncatts( {
            'standard_name': "time",
            'comment': "Observation times, the independent coordinate, " + \
                    "with respect to start_time", 
            'axis': "T", 
            'units': "seconds since {:}".format( epochstr ) } )
    outvars.update( { 'time': var } )

    varname = 'snr'
    var = output.createVariable( varname, np.dtype('f8'), dimensions=('signal','time') )
    var.setncatts( {
            'long_name': "signal-to-noise ratio", 
            'comment': "Signal-to-noise ratio, in volts per volt at 1 Hz",
            'units': "1", 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'snr': var } )

    varname = 'excess_phase'
    var = output.createVariable( varname, np.dtype('f8'), dimensions=('signal','time') )
    var.setncatts( {
            'long_name': "excess phase", 
            'comment': "Excess phase, or phase in excess of the vacuum " + \
                    "optical path",
            'units': "meter", 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'excessPhase': var } )

    varname = 'pseudorange_model'
    var = output.createVariable( varname, np.dtype('f8'), dimensions=('signal','time') )
    var.setncatts( {
            'long_name': "pseudorange model",
            'comment': "The model for pseudorange used in open-loop " + \
                    "tracking less transmitter and receiver clock biases",
            'units': 'meter', 
            'valid_range': np.array( [ -1.0e9, 1.0e9 ], dtype=np.double ), 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'rangeModel': var } )

    varname = 'phase_model'
    var = output.createVariable( varname, np.dtype('f8'), dimensions=('signal','time') )
    var.setncatts( {
            'long_name': "phase model",
            'comment': "The model for phase used in open-loop tracking",
            'units': "meter", 
            'valid_range': np.array( [ -1.0e9, 1.0e9 ], dtype=np.double ), 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'phaseModel': var } )

    varname = 'receiver_orbit'
    var = output.createVariable( varname, np.dtype('f8'), dimensions=('cartesian','time') )
    var.setncatts( {
            'long_name': "receiver orbit (ECF)", 
            'comment': "Receiver (low-Earth orbiter -- LEO) satellite " + \
                    "position in Cartesian Earth-centered fixed (ECF) " + \
                    "coordinates",
            'units': "meter", 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'positionLEO': var } )

    varname = 'transmitter_orbit'
    var = output.createVariable( varname, np.dtype('f8'), dimensions=('cartesian','time') )
    var.setncatts( {
            'long_name': "transmitter orbit (ECF)", 
            'comment': "GNSS transmitter position in Cartesian " + \
                    "Earth-centered fixed (ECF) coordinates at the " + \
                    "time of transmission of the signal",
            'units': "meter", 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'positionGNSS': var } )

    #  Done.

    return outvars


def format_level2a( output,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, optimization_references, ionospheric_references, retrieval_references,
        nimpacts, nlevels, time, mission, transmitter, receiver, **optional ):

    """Define dimensions, create variables and their attributes, and create
    global attributes for the level2a file format. "output" is a
    netCDF4.Dataset object or group.

    processing_center           ucar, romsaf, jpl, etc.
    processing_center_version   Version of retrieval provided by the processing center
    processing_center_path      Relative path to the file being translated
    data_use_license            URL for the data use license designated by the processing center
    retrieval_references        List of DOI ("doi:???") of references for the retrieval system
    optimization_references     List of DOI references for statistical optimization approach
    ionospheric_references      List of DOI references for ionospheric calibration approach
    nimpacts                    The number of impact parameters retrieved
    nlevels                     The number of atmospheric levels retrieved
    time                        datetime.datetime instance reference time of occultation
    mission                     The AWS name of the mission
    transmitter                 The 3-character definition of the transmitter
    receiver                    The AWS name of the LEO receiver
    optional                    A dictionary containing optional arguments, such as 
                                referencesat, referencestation, centerwmo, etc. 

    """

    #  Define the level. 

    level = "2a"

    #  Global attributes.

    output.setncatts( {
            'title': 'This file contains retrievals of radio occultation bending angle ' + \
                    'vs. impact parameter and retrievals of microwave refractivity and ' + \
                    'dry pressure vs. geopotential for one radio occultation sounding. ' + \
                    'The "pre-Abel" group contains all quantities relevant to radio ' + \
                    'occultation processing before the Abel integral inversion is ' + \
                    'performed; and the "post-Abel" group contains all quantities ' + \
                    'relevant to radio occultation processing after the Abel integral ' + \
                    'inversion.',
            'ShortName': ShortName( level, mission, processing_center ), 
            'LongName': "GNSS Radio Occultation L2A Bending Angle/Refractivity/Dry Retrieval",
            'VersionID': "2.0",
            'Format': "NetCDF4",
            'IdentifierProductDOIAuthority': "https://dx.doi.org/???",
            'IdentifierProductDOI': "10.5067/????",
            'ProductionDateTime': datetime.utcnow().isoformat(timespec="seconds")+"Z",
            'ProcessingLevel': level.upper(), 
            'Conventions': "CF-1.10",
            'source': "GNSS radio occultation",
            'institution': processing_center,
            'institution_version': processing_center_version,
            'institution_path': processing_center_path,
            'data_use_license': data_use_license,
            'references': retrieval_references + optimization_references + ionospheric_references, 
            'DataSetQuality': "GNSS radio occultation has been extensively validated against " + \
                    "radiosonde soundings (doi:10.1029/2004GL021443) and is routinely assimilated " + \
                    "in numerical weather prediction", 
            'ShortName': "gnssro_{:}_{:}_{:}".format( mission, processing_center, "l2a" ), 
            'LongName': "GNSS radio occultation L2A bending angle, refractivity, and dry pressure for " + \
                    "{:} as contributed by {:}, version 2.0".format( mission.upper(), processing_center.upper() ), 
            'RangeBeginningDate': "", 
            'RangeBeginningTime': "", 
            'RangeEndingDate': "", 
            'RangeEndingTime': "", 
            'GranuleID': "" 
        } )

    #  Global attributes: time.

    doy = ( time - datetime(year=time.year,month=1,day=1) ).days + 1
    output.setncatts( {
        'year': np.int32( time.year ),
        'month': np.int32( time.month ),
        'day': np.int32( time.day ),
        'hour': np.int32( time.hour ),
        'minute': np.int32( time.minute ),
        'second': float( time.second ),
        'doy': np.int32( doy ) } )

    #  Global attributes: satellites and reference stations (if available).

    output.setncatts( {
        'mission': mission,
        'receiver': receiver,
        'transmitter': transmitter } )

    #  Get the receiver satellite object, write the satellite ID, the instrument ID, 
    #  and the satellite sub-identifier to global attributes. 

    receiver_sat = get_receiver_satellites( "aws", receiver=receiver )[0]

    if "wmo" in receiver_sat.keys(): 
        receiver_wmo = receiver_sat['wmo']
        if 'satellite_subid' in receiver_wmo.keys(): 
            output.setncatts( { 'wmo_satellite_identifier': np.int32(receiver_wmo['satellite_id']), 
                           'wmo_satellite_sub_identifier': np.int32(receiver_wmo['satellite_subid']), 
                           'wmo_instrument_identifier': np.int32(receiver_wmo['instrument_id']) } )
        else: 
            output.setncatts( { 'wmo_satellite_identifier': np.int32(receiver_wmo['satellite_id']), 
                           'wmo_satellite_sub_identifier': np.int32(0), 
                           'wmo_instrument_identifier': np.int32(receiver_wmo['instrument_id']) } )
    else: 
        output.setncatts( { 'wmo_satellite_identifier': np.int32(0), 
                           'wmo_satellite_sub_identifier': np.int32(0), 
                           'wmo_instrument_identifier': np.int32(0) } )

    #  Write WMO identifiers of originating center. 

    if 'centerwmo' in optional.keys(): 

        opt = optional['centerwmo']

        if 'originating_center_id' in opt.keys(): 
            output.setncatts( { 'wmo_originating_center_identifier': np.int32(opt['originating_center_id']) } )
        else: 
            output.setncatts( { 'wmo_originating_center_identifier': np.int32(0) } )

        if 'originating_subcenter_id' in opt.keys(): 
            output.setncatts( { 'wmo_originating_center_sub_identifier': np.int32(opt['originating_subcenter_id']) } )
        else: 
            output.setncatts( { 'wmo_originating_center_sub_identifier': np.int32(0) } )

    else: 

        output.setncatts( { 'wmo_originating_center_identifier': np.int32(0), 
                           'wmo_originating_center_sub_identifier': np.int32(0) } )

    #  Conventions, history, comment. 

    output.setncatts( { 'Conventions': "CF-1.10", 
            'history': "27 November 2023", 
            'comment': "Radio occultation data using the satellites of the " + \
                    "Global Navigation Satellite Systems (GNSS), hosted in " + \
                    "the Registry of Open Data of Amazon Web Services" } )

    #  Create bending angle and refractivity groups.

    preAbel = output.createGroup( "pre_Abel" )
    postAbel = output.createGroup( "post_Abel" )

    #  Create dimensions.

    nsignals = 2
    preAbel.createDimension( "impact_parameter", nimpacts )
    preAbel.createDimension( "cartesian", 3 )
    preAbel.createDimension( "signal", nsignals )
    postAbel.createDimension( "altitude", nlevels )

    #  Create variables.

    outvars = {}

    varname = "time"
    var = output.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
        'standard_name': "time",
        'comment': "The reference time of the occultation in GPS time, " + \
                "or time elapsed since January 6, 1980 at 00Z",
        'units': "seconds since 1980-01-06 00:00:00 UTC", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'refTime': var } )

    varname = "reference_longitude"
    var = output.createVariable( varname, np.dtype('f4') )
    var.setncatts( {
        'standard_name': "longitude",
        'comment': "The reference longitude of the occultation",
        'units': "degrees_east", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'refLongitude': var } )

    varname = "reference_latitude"
    var = output.createVariable( varname, np.dtype('f4') )
    var.setncatts( {
        'standard_name': "latitude",
        'comment': "The reference latitude of the occultation",
        'units': "degrees_north", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'refLatitude': var } )

    varname = "setting"
    var = output.createVariable( varname, np.dtype('b') )
    var.setncatts( {
        'long_name': "geometry of occultation", 
        'comment': "A flag that states whether this is a rising or a " + \
            "setting occultation. If the value is 1, it is a setting " + \
            "occultation. If the value is 0, it is a rising occultation. " + \
            "If left unfilled, then no determination of occultation geometry " + \
            "is possible.",
        'units': "1",
        '_FillValue': _FillValue_byte } )
    outvars.update( { 'setting': var } )

    varname = "equatorial_radius"
    var = preAbel.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
        'long_name': "Earth equatorial radius", 
        'comment': "The equatorial radius that describes the ellipsoid " + \
            "that approximates the Earth's mean sea-level",
        'units': "meter", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'equatorialRadius': var } )

    varname = "polar_radius"
    var = preAbel.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
        'long_name': "Earth polar radius", 
        'comment': "The polar radius that describes the ellipsoid " + \
            "that approximates the Earth's mean sea-level",
        'units': "meter", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'polarRadius': var } )

    varname = "geoid_undulation"
    var = preAbel.createVariable( varname, np.dtype('f8') )
    var.setncatts( { 
        'long_name': "Earth geoid undulation", 
        'comment': "The geoid undulation at the location of the RO " + \
            "sounding; add this quantity to the mean sea-level ellipsoid " + \
            "described by equatorial_radius and polar_radius to determine the " + \
            "position of the mean sea-level geoid",
        'units': "meter", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'undulation': var } )

    varname = "center_of_curvature"
    var = preAbel.createVariable( varname, np.dtype('f8'), dimensions=("cartesian",) )
    var.setncatts( {
        'long_name': "center of curvature of occultation", 
        'comment': "The reference center of curvature for the occultation",
        'reference_frame': "ECEF",
        'units': "meter", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'centerOfCurvature': var } )

    varname = "radius_of_curvature"
    var = preAbel.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
        'long_name': "radius of curvature of occultation", 
        'comment': "The effective radius of curvature of the occultation",
        'units': "meter", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'radiusOfCurvature': var } )

    varname = "impact_parameter"
    var = preAbel.createVariable( varname, np.dtype('f8'), dimensions=("impact_parameter",) )
    var.setncatts( {
        'long_name': "ray impact parameter", 
        'comment': "The impact parameter is the independent coordinate of " + \
            "retrievals of bending angle",
        'units': "meter", 
        'axis': "Z", 
        'positive': "up" } )
    outvars.update( { 'impactParameter': var } )

    varname = 'carrier_frequency'
    var = preAbel.createVariable( varname, np.dtype('f8'), dimensions=('signal',) )
    var.setncatts( {
            'long_name': "carrier frequency", 
            'comment': "Carrier frequency of the tracked GNSS signal",
            'units': "Hz", 
            '_FillValue': _FillValue_double } )
    outvars.update( { 'carrierFrequency': var } )

    varname = "raw_bending_angle"
    var = preAbel.createVariable( varname, np.dtype('f8'), dimensions=("impact_parameter","signal") )
    var.setncatts( {
        'long_name': "ray raw bending angle", 
        'comment': "The bending angle for each signal, before ionospheric " + \
                "correction and statistical optimization/fusion with a model",
        'units': "radians", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'rawBendingAngle': var } )

    varname = "bending_angle"
    var = preAbel.createVariable( varname, np.dtype('f8'), dimensions=("impact_parameter",) )
    var.setncatts( { 
        'long_name': "ionosphere-corrected bending angle", 
        'comment': "The bending angle, ionosphere corrected " + \
                "but before statistical optimization/fusion with a model",
        'units': "radians", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'bendingAngle': var } )

    varname = "optimized_bending_angle"
    var = preAbel.createVariable( varname, np.dtype('f8'), dimensions=("impact_parameter",) )
    var.setncatts( {
        'long_name': "statistically optimized bending angle", 
        'comment': "The bending angle, ionosphere corrected and " + \
                "statistically optimized/fused with a model",
        'units': "radians", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'optimizedBendingAngle': var } )

    varname = "bending_angle_uncertainty"
    var = preAbel.createVariable( varname, np.dtype('f8'), dimensions=("impact_parameter",) )
    var.setncatts( {
        'long_name': "bending angle uncertainty", 
        'comment': "Uncertainty in the determination of bending_angle, " + \
                "ionospheric corrected but unoptimized", 
        'units': "radians", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'bendingAngleUncertainty': var } )

    varname = 'cartesian'
    var = preAbel.createVariable( varname, 'c', dimensions=('cartesian',) )
    var.setncatts( {
            'long_name': "cartesian coodinate label", 
            'comment': 'Cartesian coordinate system axes label',
            'units': '1', 
            '_FillValue': _FillValue_char } )
    var[:] = "xyz"

    varname = 'signal'
    var = preAbel.createVariable( varname, np.dtype('b'), dimensions=('signal',) )
    var.setncatts( {
            'long_name': "signal index", 
            'comment': 'dummy variable: allocate dimension for tracked GNSS signals',
            'units': '1', 
            '_FillValue': _FillValue_byte } )
    var[:] = np.arange( nsignals )

    varname = "geopotential"
    var = postAbel.createVariable( varname, np.dtype('f8'), dimensions=("altitude",) )
    var.setncatts( {
        'standard_name': "geopotential",
        'comment': "Geopotential energy per unit mass at the " + \
            "occultation tangent point; divide by the WMO standard " + \
            "constant for gravity (J/kg/m) to obtain geopotential " + \
            "height (gpm)",
        'units': "J/kg", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'geopotential': var } )

    varname = "altitude"
    var = postAbel.createVariable( varname, np.dtype('f4'), dimensions=("altitude",) )
    var.setncatts( {
        'standard_name': "altitude",
        'comment': "Altitude above the mean sea-level geoid",
        'units': "meter", 
        'axis': "Z", 
        'positive': "up" } )
    outvars.update( { 'altitude': var } )

    varname = "longitude"
    var = postAbel.createVariable( varname, np.dtype('f4'), dimensions=("altitude",) )
    var.setncatts( { \
        'standard_name': "longitude",
        'comment': "Longitude of the occultation tangent point",
        'units': "degrees_east", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'longitude': var } )

    varname = "latitude"
    var = postAbel.createVariable( varname, np.dtype('f4'), dimensions=("altitude",) )
    var.setncatts( { \
        'standard_name': "latitude",
        'comment': "Latitude of the occultation tangent point",
        'units': "degrees_north", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'latitude': var } )

    varname = "orientation"
    var = postAbel.createVariable( varname, np.dtype('f4'), dimensions=("altitude",) )
    var.setncatts( {
        'long_name': "ray orientation at tangent point", 
        'comment': "The direction of the occultation ray, " + \
            "transmitter to receiver, at the occultation tangent " + \
            "point, measured eastward from north",
        'units': "degrees", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'orientation': var } )

    varname = "refractivity"
    var = postAbel.createVariable( varname, np.dtype('f8'), dimensions=("altitude",) )
    var.setncatts( {
        'long_name': "microwave refractivity", 
        'comment': "Microwave refractivity at the occultation tangent point, " + \
                "in N-units, or the index of refraction less one in parts per million",
        'units': "1", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'refractivity': var } )

    varname = "dry_pressure"
    var = postAbel.createVariable( varname, np.dtype('f8'), dimensions=("altitude",) )
    var.setncatts( { \
        'long_name': "dry pressure", 
        'comment': "Dry pressure at the occultation tangent point; " + \
            "it is the pressure retrieved when ignoring the contribution " + \
            "of water vapor to microwave refractivity, in the equation of " + \
            "state, and in the hydrostatic equation; see " + \
            "doi:10.5194/amt-7-2883-2014",
        'units': "Pa", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'dryPressure': var } )

    varname = "quality"
    var = postAbel.createVariable( varname, np.dtype('f4'), dimensions=("altitude",) )
    var.setncatts( { \
        'long_name': "quality", 
        'comment': "Quality of the retrieval, with 0 for a bad " + \
            "retrieval and 1 for a good retrieval",
        'units': "1", 
        'valid_range': np.array( [ -0.1, 100.1 ], dtype=np.single ), 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'quality': var } )

    varname = "superrefraction_altitude"
    var = postAbel.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
        'long_name': "altitude of super-refraction layer", 
        'comment': "The altitude above the mean sea-level geoid of " + \
            "the highest super-refracting layer. If super-refraction is " + \
            "not analyzed, leave as fill values; if no super-refraction " + \
            "is found, set to -1000.0.",
        'units': "meter", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'superRefractionAltitude': var } )

    #  Done.

    return outvars


def format_level2b( output,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, retrieval_references,
        nlevels, time, mission, transmitter, receiver, **optional ):

    """Define dimensions, create variables and their attributes, and create
    global attributes for the level2b file format. "output" is a
    netCDF4.Dataset object or group.

    processing_center           ucar, romsaf, jpl, etc.
    processing_center_version   Version of retrieval provided by the processing center
    processing_center_path      Relative path to the file being translated
    data_use_license            URL for the data use license designated by the processing center
    retrieval_references        List of DOI ("doi:???") of references for the retrieval system
    nlevels                     The number of atmospheric levels retrieved
    time                        datetime.datetime instance reference time of occultation
    mission                     The AWS name of the mission
    transmitter                 The 3-character definition of the transmitter
    receiver                    The AWS name of the LEO receiver
    optional                    A dictionary containing optional arguments, such as 
                                referencesat, referencestation, centerwmo, etc. 

    """

    #  Define the level. 

    level = "2b"

    #  Global attributes.

    output.setncatts( {
            'title': 'This file contains retrievals of temperature, pressure, ' + \
                    'and humidity as functions of geopotential for one radio ' + \
                    'occultation sounding.',
            'ShortName': ShortName( level, mission, processing_center ), 
            'LongName': "GNSS Radio Occultation L2B Atmospheric Retrieval",
            'VersionID': "2.0",
            'Format': "NetCDF4",
            'IdentifierProductDOIAuthority': "https://dx.doi.org/???",
            'IdentifierProductDOI': "10.5067/????",
            'ProductionDateTime': datetime.utcnow().isoformat(timespec="seconds")+"Z",
            'ProcessingLevel': level.upper(), 
            'Conventions': "CF-1.10",
            'source': "GNSS radio occultation",
            'institution': processing_center,
            'institution_version': processing_center_version,
            'institution_path': processing_center_path,
            'data_use_license': data_use_license,
            'DataSetQuality': "GNSS radio occultation has been extensively validated against " + \
                    "radiosonde soundings (doi:10.1029/2004GL021443) and is routinely assimilated " + \
                    "in numerical weather prediction", 
            'ShortName': "gnssro_{:}_{:}_{:}".format( mission, processing_center, "l2b" ), 
            'LongName': "GNSS radio occultation L2B temperature, pressure, and water vapor retrieval for " + \
                    "{:} as contributed by {:}, version 2.0".format( mission.upper(), processing_center.upper() ), 
            'references': retrieval_references, 
            'RangeBeginningDate': "", 
            'RangeBeginningTime': "", 
            'RangeEndingDate': "", 
            'RangeEndingTime': "", 
            'GranuleID': "" 
        } )

    #  Global attributes: time.

    doy = ( time - datetime(year=time.year,month=1,day=1) ).days + 1
    output.setncatts( {
        'year': np.int32( time.year ),
        'month': np.int32( time.month ),
        'day': np.int32( time.day ),
        'hour': np.int32( time.hour ),
        'minute': np.int32( time.minute ),
        'second': float( time.second ),
        'doy': np.int32( doy ) } )

    #  Global attributes: satellites and reference stations (if available).

    output.setncatts( {
        'mission': mission,
        'receiver': receiver,
        'transmitter': transmitter } )

    #  Get the receiver satellite object, write the satellite ID, the instrument ID, 
    #  and the satellite sub-identifier to global attributes. 

    receiver_sat = get_receiver_satellites( "aws", receiver=receiver )[0]

    if "wmo" in receiver_sat.keys(): 
        receiver_wmo = receiver_sat['wmo']
        if 'satellite_subid' in receiver_wmo.keys(): 
            output.setncatts( { 'wmo_satellite_identifier': np.int32(receiver_wmo['satellite_id']), 
                           'wmo_satellite_sub_identifier': np.int32(receiver_wmo['satellite_subid']), 
                           'wmo_instrument_identifier': np.int32(receiver_wmo['instrument_id']) } )
        else: 
            output.setncatts( { 'wmo_satellite_identifier': np.int32(receiver_wmo['satellite_id']), 
                           'wmo_satellite_sub_identifier': np.int32(0), 
                           'wmo_instrument_identifier': np.int32(receiver_wmo['instrument_id']) } )
    else: 
        output.setncatts( { 'wmo_satellite_identifier': np.int32(0), 
                           'wmo_satellite_sub_identifier': np.int32(0), 
                           'wmo_instrument_identifier': np.int32(0) } )

    #  Write WMO identifiers of originating center. 

    if 'centerwmo' in optional.keys(): 

        opt = optional['centerwmo']

        if 'originating_center_id' in opt.keys(): 
            output.setncatts( { 'wmo_originating_center_identifier': np.int32(opt['originating_center_id']) } )
        else: 
            output.setncatts( { 'wmo_originating_center_identifier': np.int32(0) } )

        if 'originating_subcenter_id' in opt.keys(): 
            output.setncatts( { 'wmo_originating_center_sub_identifier': np.int32(opt['originating_subcenter_id']) } )
        else: 
            output.setncatts( { 'wmo_originating_center_sub_identifier': np.int32(0) } )

    else: 

        output.setncatts( { 'wmo_originating_center_identifier': np.int32(0), 
                           'wmo_originating_center_sub_identifier': np.int32(0) } )

    #  Conventions, history, comment. 

    output.setncatts( { 'Conventions': "CF-1.10", 
            'history': "27 November 2023", 
            'comment': "Radio occultation data using the satellites of the " + \
                    "Global Navigation Satellite Systems (GNSS), hosted in " + \
                    "the Registry of Open Data of Amazon Web Services" } )

    #  Define dimensions.

    output.createDimension( "geopotential", nlevels )

    #  Define variables.

    outvars = {}

    varname = "time"
    var = output.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
        'standard_name': "time",
        'comment': "The reference time of the occultation in GPS time, " + \
                "or time elapsed since January 6, 1980 at 00Z",
        'units': "seconds since 1980-01-06 00:00:00 UTC", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'refTime': var } )

    varname = "reference_longitude"
    var = output.createVariable( varname, np.dtype('f4') )
    var.setncatts( {
        'standard_name': "longitude",
        'comment': "The reference longitude of the occultation",
        'units': "degrees_east", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'refLongitude': var } )

    varname = "reference_latitude"
    var = output.createVariable( varname, np.dtype('f4') )
    var.setncatts( {
        'standard_name': "latitude",
        'comment': "The reference latitude of the occultation",
        'units': "degrees_north", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'refLatitude': var } )

    varname = "geopotential"
    var = output.createVariable( varname, np.dtype('f8'), dimensions=("geopotential",) )
    var.setncatts( {
        'standard_name': "geopotential",
        'comment': "Geopotential energy per unit mass; divide " + \
            "by the WMO standard constant for gravity (J/kg/m) " + \
            "to obtain geopotential height (gpm)",
        'units': "J/kg", 
        'axis': "Z", 
        'positive': "up" } )
    outvars.update( { 'geopotential': var } )

    varname = "altitude"
    var = output.createVariable( varname, np.dtype('f4'), dimensions=("geopotential",) )
    var.setncatts( {
        'standard_name': "altitude",
        'comment': "Altitude above the mean sea-level geoid",
        'units': "meter", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'altitude': var } )

    varname = "refractivity"
    var = output.createVariable( varname, np.dtype('f8'), dimensions=("geopotential",) )
    var.setncatts( {
        'long_name': "analyzedd microwave refractivity", 
        'comment': "Analyzed microwave refractivity, " + \
                "in N-units, or the index of refraction less one in parts per million",
        'units': "1", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'refractivity': var } )

    varname = "pressure"
    var = output.createVariable( varname, np.dtype('f4'), dimensions=("geopotential",) )
    var.setncatts( {
        'standard_name': "air_pressure",
        'comment': "Atmospheric pressure retrieved by statistical methods using a prior",
        'units': "Pa", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'pressure': var } )

    varname = "temperature"
    var = output.createVariable( varname, np.dtype('f4'), dimensions=("geopotential",) )
    var.setncatts( {
        'standard_name': "air_temperature",
        'description': "Atmospheric temperature retrieved by statistical methods using a prior",
        'units': "K", 
        '_FillValue': _FillValue_float, } )
    outvars.update( { 'temperature': var } )

    varname = "water_vapor_partial_pressure"
    var = output.createVariable( varname, np.dtype('f4'), dimensions=("geopotential",) )
    var.setncatts( {
        'standard_name': "water_vapor_partial_pressure_in_air",
        'comment': "Partial pressure of water vapor retrieved by statistical methods using a prior",
        'units': "Pa", 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'waterVaporPressure': var } )

    varname = "quality"
    var = output.createVariable( varname, np.dtype('f4'), dimensions=("geopotential",) )
    var.setncatts( { \
        'long_name': "quality", 
        'comment': "Quality of the retrieval, with 0 for a bad " + \
            "retrieval and 1 for a good retrieval",
        'units': "1", 
        'valid_range': np.array( [ -0.1, 100.1 ], dtype=np.single ), 
        '_FillValue': _FillValue_float } )
    outvars.update( { 'quality': var } )

    varname = "superrefraction_altitude"
    var = output.createVariable( varname, np.dtype('f8') )
    var.setncatts( {
        'long_name': "altitude of super-refraction layer", 
        'comment': "The altitude above the mean sea-level geoid of " + \
            "the highest super-refracting layer. If super-refraction is " + \
            "not analyzed, leave as fill values; if no super-refraction " + \
            "is found, set to -1000.0.",
        'units': "meter", 
        '_FillValue': _FillValue_double } )
    outvars.update( { 'superRefractionAltitude': var } )

    varname = "setting"
    var = output.createVariable( varname, np.dtype('b') )
    var.setncatts( {
        'long_name': "geometry of occultation", 
        'comment': "A flag that states whether this is a rising or a " + \
            "setting occultation. If the value is 1, it is a setting " + \
            "occultation. If the value is 0, it is a rising occultation. " + \
            "If left unfilled, then no determination of occultation geometry " + \
            "is possible.",
        'units': "1",
        '_FillValue': _FillValue_byte } )
    outvars.update( { 'setting': var } )

    #  Done.

    return outvars
