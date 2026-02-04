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


#  Logging.

import logging
LOGGER = logging.getLogger( __name__ )


################################################################################
#  Parameters.
################################################################################

#  AWS processing version.

AWSversion = "1.1"

#  This is the required ordering of RO variables (bending angle, impact
#  parameter) in height and the ordering of meteorological variables
#  (refractivity, pressure, temperature, water vapor pressure, altitude)
#  in altitude. Valid values are "ascending" and "descending" for each.

required_RO_order = "descending"
required_met_order = "ascending"

#  Database variables to be saved in DynamoDB table. This is not the
#  only information to be saved in the table, though. Other information
#  is contained in the partition key and sort key. Still more is
#  contained in the pointers to the data files by processing center and
#  file type.

#  File naming conventions.

file_indexing = {
        'level1b': "calibratedPhase",
        'level2a': "refractivityRetrieval",
        'level2b': "atmosphericRetrieval" }

#  Database variables to be saved in DynamoDB table. This is not the
#  only information to be saved in the table, though. Other information
#  is contained in the partition key and sort key. Still more is
#  contained in the pointers to the data files by processing center and
#  file type.

database_variables = {
        'longitude': float,             # degrees east, -180 - 180
        'latitude': float,              # degrees north, -90 - 90
        'time': str,                    # ISO format time
        'gps_seconds': float,           # Time of occultation in GPS seconds
        'local_time': float,            # hours, 0-24
        'setting': bool,                # True for setting, False for rising
        'mission': str,                 # as defined in documentation
        'transmitter': str,             # 3-char GNSS prn; e.g., "G03"
        'receiver': str }               # as defined in documentation

#  Root for logging output.

loggingRoot = f"logs/{AWSversion.replace('.','_')}/{datetime.now().strftime('%Y%m%d')}"


#  Fill values.

_FillValue_byte = np.byte( -128 )
_FillValue_int = np.int32( -999 )
_FillValue_float = np.float32( -9.99e20 )
_FillValue_double = np.float64( -9.99e20 )

#  For testing...

TEST = os.getenv( "TEST" )

#  S3 buckets and DynamoDB table.

if TEST is None:
    from ..Utilities.resources import stagingBucket
    definitionsBucket = "gnss-ro-processing-definitions"
    dynamodbTable = f"gnss-ro-data-stagingv{AWSversion.replace('.','_')}"
    batchJobsRoot = "batchprocess-jobs"
else: 
    definitionsBucket = "gnss-ro-data-test"
    stagingBucket = "gnss-ro-data-test"
    dynamodbTable = f"gnss-ro-test-v1"
    batchJobsRoot = "batchprocess-jobs"


################################################################################
#  Output file/path definition.
################################################################################

def defpath( file_type, processing_center, mission, transmitter, receiver,
             time, occid, center_version ):
    """Defines relative path to processed file according to the AWS file_type
    ("level1b", "level2a", "level2b"), processing_center, mission,
    transmitter, receiver, time (instance of Time.Calendar), center processing
    version (center_version). The mission and receiver must be AWS definitions of
    those quantities. The output path has the mission name as its root.
    """

    LOGGER.debug( "Running _defpath: " +
            json.dumps( { 'file_type': file_type, 'processing_center': processing_center,
                'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
                'time': time.isoformat() } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [] }

#  File indexing conventions.

    file_indexing = {
            'level1b': "calibratedPhase",
            'level2a': "refractivityRetrieval",
            'level2b': "atmosphericRetrieval" }

#  Define the directory hierarchy.

    path = os.path.join( "contributed", "v"+AWSversion, processing_center,
            mission, file_indexing[file_type], "{:4d}".format( time.year ),
            "{:02d}".format( time.month ), "{:02d}".format( time.day ) )

#  Define the file name.

    path = os.path.join( path, f"{file_indexing[file_type]}_{mission}_{processing_center}_{center_version}_{occid}.nc" )

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

    Arguments
    =========
    processing_center           ucar, romsaf, jpl, etc.
    processing_center_version   version of retrieval provided by the processing center
    processing_center_path      Relative path to the file being translated
    data_use_license            URL for the data use license designated by the processing center
    retrieval_references        List of DOI ("doi:???") of references for the retrieval system
    ntimes                      The number of high-rate times
    nsignals                    The number of occultation signals to be translated
    time                        datetime.datetime instance reference time of occultation
    mission                     The AWS name of the mission
    transmitter                 The 3-character definition of the transmitter
    receiver                    The AWS name of the LEO receiver
    optional                    A dictionary of optional arguments. They can include 
                                referencesat, referencestation, centerwmo, etc. 
    """

    #  Global attributes.

    output.setncatts( {
            'file_type': "GNSS-RO-in-AWS-Open-Data-calibratedPhase",
            'AWSversion': AWSversion,
            'processing_center': processing_center,
            'processing_center_version': processing_center_version,
            'processing_center_path': processing_center_path,
            'data_use_license': data_use_license,
            'references': retrieval_references } )

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
            'leo': receiver,
            'occGnss': transmitter } )

    if 'referencesat' in optional.keys(): 
        if optional['referencesat'] is not None: 
            output.setncatts( { 'refGnss': optional['referencesat'] } )
        else: 
            output.setncatts( { 'refGnss': "" } )
    else:
        output.setncatts( { 'refGnss': "" } )

    if 'referencestation' in optional.keys(): 
        if optional['referencestation'] is not None: 
            output.setncatts( { 'refStation': optional['referencestation'] } )
        else: 
            output.setncatts( { 'refStation': "" } )
    else:
        output.setncatts( { 'refStation': "" } )

    #  Create NetCDF dimensions.

    output.createDimension( 'time', ntimes )
    output.createDimension( 'obscode', 3 )
    output.createDimension( 'xyz', 3 )
    output.createDimension( 'signal', nsignals )

    #  Create NetCDF variables.

    outvars = {}

    varname = 'startTime'
    var = output.createVariable( varname, 'd' )
    var.setncatts( { 'units': 'GPS seconds', \
            'description': 'Reference start time of the occultation at the receiver', 
            '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'endTime'
    var = output.createVariable( varname, 'd' )
    var.setncatts( { 'units': 'GPS seconds', \
            'description': 'Time of last data point at the receiver', 
            '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'navBitsPresent'
    var = output.createVariable( varname, 'b', ('signal',) )
    var.setncatts(
            { 'description': 'This logical variable declares whether the ' + \
            'bit-stream of a navigation message was removed in the ' + \
            'generation of excess phase. If non-zero, then the ' + \
            'navigation message bits have been removed; if zero, ' + \
            'then the navigation message bits have not been removed. ' + \
            'If the signal is a data-less "pilot" tone, meaning there is ' + \
            'no navigation message on the signal, the variable is ' + \
            'meaningless but is set to True nonetheless.', 
              '_FillValue': _FillValue_byte } )
    outvars.update( { varname: var } )

    varname = 'snrCode'
    var = output.createVariable( varname, 'c', ('signal','obscode') )
    var.setncatts( { 'description': 'RINEX 3 observation code ' + \
                'for the signal-to-noise ratio observation' } )
    outvars.update( { varname: var } )

    varname = 'phaseCode'
    var = output.createVariable( varname, 'c', ('signal','obscode') )
    var.setncatts( { 'description': 'RINEX 3 observation ' + \
                'code for the excess phase observation' } )
    outvars.update( { varname: var } )

    varname = 'carrierFrequency'
    var = output.createVariable( 'carrierFrequency', 'd', ('signal',) )
    var.setncatts( { 'units': 'Hz', 'description': \
                'Carrier frequency of the tracked GNSS signal', 
                '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'time'
    var = output.createVariable( varname, 'd', ('time',) )
    var.setncatts( { 'units': 'seconds', 
                    'description': 'Observation times, the independent coordinate, with respect to startTime', 
                    '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'snr'
    var = output.createVariable( varname, 'd', ('time','signal') )
    var.setncatts( { 'units': 'V/V (1 Hz)', 
                'description': 'Signal-to-noise ratio', 
                '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'excessPhase'
    var = output.createVariable( varname, 'd', ('time','signal') )
    var.setncatts( { 'units': 'm', 
                'description': 'Excess phase, or phase in excess of the vacuum optical path', 
                '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'rangeModel'
    var = output.createVariable( varname, 'd', ('time','signal') )
    var.setncatts( { 'units': 'm', 
                'description': 'The model for pseudo-range used in open-loop tracking ' + \
                        'less transmitter and receiver clock biases', 
                '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'phaseModel'
    var = output.createVariable( varname, 'd', ('time','signal') )
    var.setncatts( { 'units': 'm', 
                'description': 'The model for phase used in open-loop tracking', 
                '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'positionLEO'
    var = output.createVariable( varname, 'd', ('time', 'xyz'))
    var.setncatts( { 'units': 'm', 
                'description': 'Receiver (low-Earth orbiter -- LEO) satellite position ' + \
                        'in Cartesian Earth-centered fixed (ECF) coordinates', 
                '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = 'positionGNSS'
    var = output.createVariable( varname, 'd', ('time', 'xyz'))
    var.setncatts( { 'units': 'm', 
                'description': 'GNSS transmitter position in Cartesian Earth-centered fixed ' + \
                        '(ECF) coordinates at the time of transmission of the signal', 
                '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

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
    optional                    A dictionary of optional arguments. They can include 
                                referencesat, referencestation, centerwmo, etc. 
    """

    #  Global attributes.

    output.setncatts( {
        'file_type': "GNSS-RO-in-AWS-Open-Data-refractivityRetrieval",
        'AWSversion': AWSversion,
        'processing_center': processing_center,
        'processing_center_version': processing_center_version,
        'processing_center_path': processing_center_path,
        'data_use_license': data_use_license,
        'optimization_references': optimization_references,
        'ionospheric_references': ionospheric_references,
        'references': retrieval_references } )

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

    #  Satellite attributes.

    output.setncatts( {
        'mission': mission, \
        'leo': receiver, \
        'occGnss': transmitter } )
    #  Create dimensions.

    output.createDimension( "impact", nimpacts )
    output.createDimension( "level", nlevels )
    output.createDimension( "signal", 2 )
    output.createDimension( "xyz", 3 )

    #  Create variables.

    outvars = {}

    varname = "refTime"
    var = output.createVariable( varname, 'd' )
    var.setncatts( { \
        'units': "GPS seconds", \
        'description': "The reference time of the occultation", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "refLongitude"
    var = output.createVariable( varname, 'f' )
    var.setncatts( { \
        'units': "degrees east", \
        'description': "The reference longitude of the occultation", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "refLatitude"
    var = output.createVariable( varname, 'f' )
    var.setncatts( { \
        'units': "degrees north", \
        'description': "The reference latitude of the occultation", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "equatorialRadius"
    var = output.createVariable( varname, 'd' )
    var.setncatts( { \
        'units': "m", \
        'description': "The equatorial radius that describes the ellipsoid " + \
                "that approximates the Earth's mean sea-level", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "polarRadius"
    var = output.createVariable( varname, 'd' )
    var.setncatts( { \
        'units': "m", \
        'description': "The polar radius that describes the ellipsoid " + \
                "that approximates the Earth's mean sea-level",
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "undulation"
    var = output.createVariable( varname, 'd' )
    var.setncatts( { \
        'units': "m", \
        'description': "The geoid undulation at the location of the RO " + \
                "sounding; add this quantity to the mean sea-level ellipsoid " + \
                "described by equatorialRadius and polarRadius to determine the " + \
                "position of the mean sea-level geoid", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "centerOfCurvature"
    var = output.createVariable( varname, 'd', dimensions=("xyz",) )
    var.setncatts( { \
        'units': "m", \
        'description': "The reference center of curvature for the occultation",
        'reference_frame': "ECEF", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "radiusOfCurvature"
    var = output.createVariable( varname, 'd' )
    var.setncatts( { \
        'units': "m", \
        'description': "The effective radius of curvature of the occultation", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "impactParameter"
    var = output.createVariable( varname, 'd', dimensions=("impact",) )
    var.setncatts( { \
        'units': "m", \
        'description': "The impact parameter is the independent coordinate of " + \
                "retrievals of bending angle", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "carrierFrequency"
    var = output.createVariable( varname, 'd', dimensions=("signal",) )
    var.setncatts( { \
        'units': "Hz", \
        'description': "Carrier frequency of the tracked GNSS signal", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "rawBendingAngle"
    var = output.createVariable( varname, 'd', dimensions=("impact","signal") )
    var.setncatts( { \
        'units': "radians", \
        'description': "The bending angle for each signal, unoptimzied, " + \
                "not fused with a model, not corrected for the ionospheric " + \
                "influence, positive for downward bending", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "bendingAngle"
    var = output.createVariable( varname, 'd', dimensions=("impact",) )
    var.setncatts( { \
        'units': "radians", \
        'description': "The unoptimized bending angle calibrated to " + \
                "eliminate ionospheric influence, positive for downward bending", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "optimizedBendingAngle"
    var = output.createVariable( varname, 'd', dimensions=("impact",) )
    var.setncatts( { \
        'units': "radians", \
        'description': "The bending angle calibrated to eliminate " + \
                "ionospheric influence and fused with a model by a " + \
                "statistical method, positive for downward bending", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "altitude"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( { \
        'units': "m", \
        'description': "Altitude above the mean sea-level geoid", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "longitude"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( { \
        'units': "degrees east", \
        'description': "Longitude of the occultation tangent point", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "latitude"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( { \
        'units': "degrees north", \
        'description': "Latitude of the occultation tangent point", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "orientation"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( { \
        'units': "degrees", \
        'description': "The direction of the occultation ray, " + \
                "transmitter to receiver, at the occultation tangent " + \
                "point, measured eastward from north", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "geopotential"
    var = output.createVariable( varname, 'd', dimensions=("level",) )
    var.setncatts( { \
        'units': "J/kg", \
        'description': "Geopotential energy per unit mass at the " + \
                "occultation tangent point; divide by the WMO standard " + \
                "constant for gravity (J/kg/m) to obtain geopotential " + \
                "height (gpm)", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "refractivity"
    var = output.createVariable( varname, 'd', dimensions=("level",) )
    var.setncatts( { \
        'units': "N-units", \
        'description': "Microwave refractivity at the occultation tangent point", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "dryPressure"
    var = output.createVariable( varname, 'd', dimensions=("level",) )
    var.setncatts( { \
        'units': "Pa", \
        'description': "Dry pressure at the occultation tangent point; " + \
                "it is the pressure retrieved when ignoring the contribution " + \
                "of water vapor to microwave refractivity, the equation of " + \
                "state, and the hydrostatic equation; see " + \
                "doi:10.5194/amt-7-2883-2014", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "quality"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( { \
        'units': "none", \
        'description': "Quality of retrieval", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "superRefractionImpactHeight"
    var = output.createVariable( varname, 'd' )
    var.setncatts( { \
        'units': "m", \
        'description': "The impact height---or the impact parameter that defines " + \
                "the location of the duct less the effective radius of curvature " + \
                "(radiusOfCurvature)---of the highest super-refracting layer. If " + \
                "super-refraction is not analyzed, leave as fill values; if " + \
                "super-refraction is analyzed but not found, set to -1000.0", 
        '_FillValue': _FillValue_double } )
    outvars.update( { varname: var } )

    varname = "setting"
    var = output.createVariable( varname, 'b' )
    var.setncatts( { \
        'units': "none", \
        'description': "A flag that states whether this is a rising or a " + \
            "setting occultation. If the value is 1, it is a setting " + \
            "occultation. If the value is 0, it is a rising occultation. " + \
            "If left unfilled, then no determination of occultation geometry " + \
            "is possible.",
        '_FillValue': _FillValue_byte } )
    outvars.update( { varname: var } )

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
    optional                    A dictionary of optional arguments. They can include 
                                referencesat, referencestation, centerwmo, etc. 

    """

    #  Global attributes.

    output.setncatts( {
        'file_type': "GNSS-RO-in-AWS-Open-Data-atmosphericRetrieval",
        'AWSversion': AWSversion } )

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

    #  Satellite attributes.

    output.setncatts( {
        'mission': mission, \
        'leo': receiver, \
        'occGnss': transmitter } )

    #  Center-specific attributes.

    output.setncatts( {
        'processing_center': processing_center,
        'processing_center_version': processing_center_version,
        'processing_center_path': processing_center_path,
        'data_use_license': data_use_license,
        'references': retrieval_references } )

    #  Define dimensions.

    output.createDimension( "level", nlevels )

    #  Define variables.

    outvars = {}

    varname = "refTime"
    var = output.createVariable( varname, 'd' )
    var.setncatts( {
        'units': "GPS seconds",
        'description': "The reference time of the occultation", 
        '_FillValue': _FillValue_double 
        } )
    outvars.update( { varname: var } )

    varname = "refLongitude"
    var = output.createVariable( varname, 'f' )
    var.setncatts( {
        'units': "degrees east",
        'description': "The reference longitude of the occultation", 
        '_FillValue': _FillValue_float 
        } )
    outvars.update( { varname: var } )

    varname = "refLatitude"
    var = output.createVariable( varname, 'f' )
    var.setncatts( {
        'units': "degrees north",
        'description': "The reference latitude of the occultation", 
        '_FillValue': _FillValue_float 
        } )
    outvars.update( { varname: var } )

    varname = "altitude"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( {
        'units': "m",
        'description': "Altitude above the mean sea-level geoid", 
        '_FillValue': _FillValue_float 
        } )
    outvars.update( { varname: var } )

    varname = "geopotential"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( {
        'units': "J/kg",
        'description': "Geopotential energy per unit mass at the occultation tangent point; "
                "divide by a standard constant for gravity to obtain geopotential height "
                "(gpm), typically the WMO standard (9.80665 J/kg/m)", 
        '_FillValue': _FillValue_float 
        } )
    outvars.update( { varname: var } )

    varname = "refractivity"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( {
        'units': "N-units",
        'description': "Analyzed microwave refractivity at the occultation tangent point", 
        '_FillValue': _FillValue_float 
        } )
    outvars.update( { varname: var } )

    varname = "pressure"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( {
        'units': "Pa",
        'description': "Atmospheric pressure retrieved by statistical methods using a prior", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "temperature"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( {
        'units': "K",
        'description': "Atmospheric temperature retrieved by statistical methods using a prior", 
        '_FillValue': _FillValue_float 
        } )
    outvars.update( { varname: var } )

    varname = "waterVaporPressure"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( {
        'units': "Pa",
        'description': "Partial pressure of water vapor retrieved by statistical methods using a prior", 
        '_FillValue': _FillValue_float 
        } )
    outvars.update( { varname: var } )

    varname = "quality"
    var = output.createVariable( varname, 'f', dimensions=("level",) )
    var.setncatts( { \
        'units': "none", \
        'description': "Quality of retrieval", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "superRefractionAltitude"
    var = output.createVariable( varname, 'f' )
    var.setncatts( {
        'units': "m",
        'description': "The altitude above the mean sea-level geoid of " + \
                "the highest super-refracting layer. If super-refraction is " + \
                "not analyzed, leave as fill values; if no super-refraction " + \
                "is found, set to -1000.0.", 
        '_FillValue': _FillValue_float } )
    outvars.update( { varname: var } )

    varname = "setting"
    var = output.createVariable( "setting", 'b' )
    var.setncatts( { \
        'units': "none", \
        'description': "A flag that states whether this is a rising or a " + \
                "setting occultation. If the value is 1, it is a setting " + \
                "occultation. If the value is 0, it is a rising occultation. " + \
                "If left unfilled, then no determination of occultation geometry " + \
                "is possible.",
        '_FillValue': _FillValue_byte } )
    outvars.update( { varname: var } )

    #  Done.

    return outvars
