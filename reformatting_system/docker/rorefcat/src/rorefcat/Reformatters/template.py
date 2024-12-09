#  Template for reformatters for an individual processing center. 

#  Logging.

import logging
LOGGER = logging.getLogger( __name__ )

#  Suppress warnings. 

import warnings
warnings.filterwarnings("ignore")

#  Define the archive storage bucket for center data and the bucket containing 
#  the liveupdate incoming stream. 

archiveBucket = "center-archive-bucket"
liveupdateBucket = "center-liveupdate-bucket"


################################################################################
#  Parameters relevant to processing center.
################################################################################

processing_center = "<processing_center>"

#  Define WMO originating center identifier. 

centerwmo = { 'originating_center_id': 60 }

#  Digital object identifiers for references to ionospheric calibration strategy 
#  (ionospheric_references), statistical optimization/upper level initialization 
#  strategy (optimization_references), and retrieval algorithm (retrieval_references). 
#  Also provide a link to the license. 

ionospheric_references = \
    [ "doi:10.1029/1999RS002199", "doi:10.5194/amt-9-335-2016" ]

optimization_references = \
    [ "doi:10.1029/2000RS002370" ]

retrieval_references = \
    [ "doi:10.2151/jmsj.2004.507" ]

data_use_license = "https://www.ucar.edu/terms-of-use/data"

#  Order of output profiles of radio occultation variables, such as
#  bending angle vs. impact parameter, and of atmospheric variables,
#  such as refractivity, dry pressure, dry temperature, temperature,
#  water vapor vs. height.

required_RO_order = "descending"
required_met_order = "ascending"


################################################################################
#  Utility for parsing RO retrieval file names.
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

    return ret


################################################################################
#  level1b translator
################################################################################

def level1b2aws( input_file, output_file, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, **extra ):
    """Reformat a level 1b file containing excess phase and signal-to-noise 
    ratio (SNR) data into a new standard format. The following arguments are 
    required: 

    input_file                  Path to the input file, the one contributed by 
                                an RO processing center; 
    output_file                 Path to the output file to be generated; 
    mission                     Name of the RO mission, AWS standard as defined 
                                for the module Missions/mission.py; 
    transmitter                 3-character string defining the GNSS transmitter, 
                                RINEX 3 standard format; 
    receiver                    Name of the RO receiver satellite, AWS standard 
                                as defined in Missions/mission.py; 
    input_file_type             Type of the input file specific to the 
                                contributing retrieval center; this information 
                                may be useful to the reformatter; 
    processing_center_version   A string without underscores that defines the 
                                version of the retrieval algorithm used by the 
                                contributing RO retrieval center; 
    processing_center_path      A string that defines a nominal path to the 
                                input file that facilitates its discovery for 
                                an interested user; 
    version                     An instance returned by Versions.get_version 
                                that defines the templates for the output file. 
    """

    #  Define the level for processing: "level1b" corresponds to excess phase and 
    #  SNR data. 

    level = "level1b"
    fileformatter = version[level]

    #  Log run.

    LOGGER.info( "Running level1b2aws: " + \
            json.dumps( { 'atmPhs_file': atmPhs_file, 'level1b_file': level1b_file,
                'mission': mission, 'transmitter': transmitter, 'receiver': receiver,
                'processing_center_path': processing_center_path } ) )

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Input file. 

    try:
        d = Dataset( input_file, 'r' )
    except:
        ret['status'] = "fail"
        comment = f"File {input_file} is not a NetCDF file"
        ret['messages'].append( "FileNotNetCDF" )
        ret['comments'].append( comment )
        LOGGER.warning( comment )
        return ret

    #  Create the output file template. 
    #  ================================
    #   ntimes => number of times in the excess phase and SNR data arrays
    #   nsignals => number of RINEX signals tracked in the RO
    #   sounding_datetime => datetime of the occultation (datetime.datetime instance)
    #   referencesat => RINEX-3 name of references GNSS satellite used in 
    #       single-difference calibration
    #   referencestation => Name of ground station used in double-difference 
    #       calibration
    #   starttime => GPS time (in seconds) marking the epoch of all time data 
    #       for the level 1b data. 

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

    outvars = fileformatter( e,
                processing_center, processing_center_version, processing_center_path,
                data_use_license, retrieval_references, ntimes, nsignals, sounding_datetime, mission,
                transmitter, receiver, referencesat=referencesat, referencestation=referencestation, 
                centerwmo=centerwmo, starttime=starttime )

    #  outvars is a dictionary containing pointers to various netCDF4.Dataset.variables 
    #  into which to write the data (into the output file). Common keys for these data 
    #  include "startTime", "endTime", "time", "snr", "phase", "phaseModel", etc. 
    #
    #  The priority of time vs. signal in SNR and phase data can differ according to 
    #  format template (version), so be careful to write in the correct order. Use the 
    #  numpy.ndarray.shape attribute to interpret the dimension priority. 

    outvarsnames = sorted( list( outvars.keys() ) )

    ################################################################################
    #  Read input data, process and interpret as necessary and write to output through 
    #  pointers in outvars!!!
    ################################################################################

    ...

    ################################################################################

    #  Close input and output files.

    d.close()
    e.close()

    #  Update status and provide metadata on the RO sounding extracted from the 
    #  input file. "setting" should be 1 for a setting occultation, 0 for a 
    #  rising occultation. 

    ret['status'] = "success"
    ret['metadata'].update( { 
            "gps_seconds": starttime, 
            "occ_duration": stoptime-starttime, 
            "setting": 1 } )

    LOGGER.info(f"reformatted file path: {output_file}")

    return ret


################################################################################
#  level 2a reformatter 
################################################################################

def level2a2aws( input_file, output_file, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, setting=None, **extra ):
    """As for the level 1b reformatter but for bending angles, impact parameters, 
    retrieved refractivity..."""

    #  Initialize.

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    #  Specify level, file reformatter. Note that the required_RO_order indicates 
    #  whether impact parameters (for bending angle) and altitudes/geopotentials 
    #  (for refractivity) should be written in "ascending" or "descending" order. 

    level = "level2a"
    fileformatter = version[level]
    required_RO_order = version['module'].required_RO_order

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

    ################################################################################
    #  Define output file format.
    ################################################################################

    outvars = fileformatter( e,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, optimization_references, ionospheric_references, retrieval_references,
        nlevels_bending, nlevels_refractivity, datetime, mission, transmitter, receiver, centerwmo=centerwmo )

    #  nlevels_bending => number of levels in impact parameter and bending angle arrays
    #  nlevels_refractivity => numver of levels in altitude, geopotential, and refractivity arrays

    outvarsnames = sorted( list( outvars.keys() ) )

    #  Example code for writing into output file global attributes... Note that 
    #  the extra arguments passed into this function can contain valuable information 
    #  for the reformatter; in this case, gps_seconds and occ_duration. 

    if { "gps_seconds", "occ_duration" }.issubset( extra.keys() ) and \
            { "RangeBeginningDate", "RangeBeginningTime", "RangeEndingDate", "RangeEndingTime" }.issubset( e.ncattrs() ): 
        date0 = Time( gps=extra['gps_seconds'] ).calendar( "utc" ).isoformat()
        date1 = Time( gps=extra['gps_seconds']+extra['occ_duration'] ).calendar( "utc" ).isoformat()
        e.setncatts( {
            'RangeBeginningDate': date0[:10], 
            'RangeBeginningTime': date0[11:19], 
            'RangeEndingDate': date1[:10], 
            'RangeEndingTime': date1[11:19] } )

    ################################################################################
    #  Do the reformatting and writing to output!! 
    ################################################################################

    d.close()
    e.close()

    #  Update output dictionary.

    ret['status'] = "success"

    #  RO metadata to return to caller.  Nothing is mandatory here. "setting" 
    #  should be 1 for a setting occultation, 0 for a rising occultation. 

    ret['metadata'].update( { 
            "longitude": reference_longitude, 
            "latitude": reference_latitude, 
            "gps_seconds": reference_time, # in GPS seconds
            "setting": 0, 
            "orientation": reference_orientation 
    } )

    #  reference_orientation is the orientation of the RO ray at its tangent 
    #  point, transmitter-to-receiver, measured eastward from north in degrees. 

    #  Execution complete.

    LOGGER.info(f"reformatted file path: {output_file}")

    return ret


################################################################################
#  level 2b reformatter
################################################################################

def level2b2aws( input_file, output_file, mission, transmitter, receiver,
        input_file_type, processing_center_version, processing_center_path,
        version, setting=None, **extra ):
    """As for level1b2aws and level2a2aws, except for retrieved profiles of 
    temperature, pressure, and water vapor..."""

    level = "level2b"

    #  Log run.

    LOGGER.info( "Running level2b2aws" )
    LOGGER.info( f"input_file={input_file}, output_file={output_file}" )
    LOGGER.info( f"mission={mission}, transmitter={transmitter}, receiver={receiver}" )
    LOGGER.info( f"input_file_type={input_file_type}, processing_center_path={processing_center_path}" )

    #  Initialize. "ascending" or "descending" order for altitude and geopotential? 

    ret = { 'status': None, 'messages': [], 'comments': [], 'metadata': {} }

    level = "level2b"
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

    #  Define output template.

    outvars = fileformatter( e,
        processing_center, processing_center_version, processing_center_path,
        data_use_license, retrieval_references,
        nlevels, cal.datetime(), mission, transmitter, receiver, centerwmo=centerwmo )

    outvarsnames = sorted( list( outvars.keys() ) )

    #  Close input and output files.

    d.close()
    e.close()

    ret['status'] = "success"

    LOGGER.info(f"reformatted file path: {output_file}")

    return ret

