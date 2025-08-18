##################################################################
#  NOAA module for rotation-collocation utility. 
##################################################################

# Convert ATMS NOAA HDF5 granules into AER's standard radiance format.
#
# Usage example:
# python convert_satms_hdf5torad.py SATMS_j02_d20250410_t0000298_e0001014_b12509_c20250410004550539000_oeac_ops.h5 GATMO_j02_d20250410_t0000298_e0001014_b12509_c20250410004550980000_oeac_ops.h5 --outfile SATMS_j02_d20250410_t0000298_e0001014_b12509_c20250410004550539000_oeac_ops_rad.nc --log test_convert.log -vv
#
# Developed by: AER, Inc., Apr. 2025
# Copyright: AER, Inc., 2025
##################################################################

import numpy as np
from netCDF4 import Dataset
import boto3
from botocore import UNSIGNED
from .timestandards import Time
from .constants_and_utils import defaults_file

satellites_info = [ 
        { 'name': "Suomi-NPP", 'mname': "npp", 'bucket': "noaa-nesdis-snpp-pds" }, 
        { 'name': "JPSS-1", 'mname': "j01", 'bucket': "noaa-nesdis-n20-pds" }, 
        { 'name': "JPSS-2", 'mname': "j02", 'bucket': "noaa-nesdis-n21-pds" } ]

jpss_satellites = [ s['name'] for s in satellites_info ]
noaa_time_convention = "utc"
HOME = os.path.expanduser( "~" )
root_path_variable = "noaa_jpss_root"

#  Exception handling.

class Error( Exception ):
    pass

class noaaError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment


def setdefaults( root_path ): 
    """Set the root path for NOAA JPSS data in the AWS Registry of Open Data and 
    write it into the defaults files."""

    ret = { 'status': None, 'messages': [], 'comments': [], 'data': None }
    new_defaults = {}

    try:
        os.makedirs( root_path, exist_ok=True )

    except:
        ret['status'] = "fail"
        ret['messages'].append( "BadPathName" )
        ret['comments'].append( f'Unable to create root_path ("{root_path}") as a directory.' )
        return ret

    new_defaults.update( { root_path_variable: os.path.abspath( root_path ) } )

    #  Get old defaults.

    if os.path.exists( defaults_file ):
        with open( defaults_file, 'r' ) as f:
            defaults = json.load( f )
    else:
        defaults = {}

    #  Update with new defaults.

    defaults.update( new_defaults )

    #  Write to defaults file.

    with open( defaults_file, 'w' ) as f:
        json.dump( defaults, f, indent="  " )
    os.chmod( defaults_file, stat.S_IRUSR | stat.S_IWUSR )

    ret['data'] = defaults

    #  Done.

    ret['status'] = "success"

    return


def checkdefaults():
    """Check that all of the defaults needed for execution have been set."""

    ret = { 'status': None, 'messages': [], 'comments': [] }

    #  Check for existence of package defaults file.

    if not os.path.exists( defaults_file ):
        ret['status'] = "fail"
        ret['messages'].append( "MissingDefaultsFile" )
        ret['comments'].append( 'Missing defaults file; be certain to run "rotcol setdefaults ..."' )
        return ret

    #  Read defaults file.

    with open( defaults_file, 'r' ) as f:
        defaults = json.load( f )

    #  Check that the data root path has been set.

    if root_path_variable not in defaults.keys():
        ret['status'] = "fail"
        ret['messages'].append( "MissingNOAAJPSSDataStoreRoot" )
        ret['comments'].append( 'Missing data root for NOAA JPSS data repository; be certain to run "rotcol setdefaults noaa --dataroot ..."' )
        return ret

    #  Done.

    ret['status'] = "success"
    return ret


class AWSOpenDataRegistryJPSS(): 
    """A class to interact with the NOAA repository of JPSS satellite data in 
    the AWS Registry of Open Data."""

    def __init__( self ): 

        self.session = boto3.Session( region_name="us-east-1" )     #  For JPSS S3 buckets
        self.s3 = self.session.resource( "s3", config = boto3.session.Config( signature_version=UNSIGNED ) )

        #  Get root data path.

        with open( defaults_file, 'r' ) as f:
            defaults = json.load( f )
            self.data_root = defaults[root_path_variable]

        #  Get inventory of data in local file system. 

        self.inventory = {}
        self.inventory_jpss_atms()


    def inventory_jpss_atms( self ): 
        """Create an inventory of the files available on the local file system, 
        keeping information ont he absolute path and the time range of data."""

        #  Get list of satellites. 

        data_root = os.path.join( self.data_root, "atms" )
        satellites = [ p for p in os.listdir( data_root ) if p in jpss_satellites \
                and os.path.isdir( os.path.join( data_root, p ) ) ]

        #  Initialize inventory. 

        self.inventory = { sat: [] for sat in satellites }

        for sat in satellites:

            satrec = [ s for s in satellites_info if s['name'] == sat ][0]
            mname = satrec['mname']

            for root, subdirs, files in os.walk( os.path.join( data_root, sat ) ):
                subdirs.sort()
                files.sort()

                ss_sdr = "^SATMS_" + mname + r"_d(\d{8}_t\d{7})_e(\d{7})_b\d+_c\d+_oeac_ops.h5$"
                ss_sdr_geo = "^GATMO_" + mname + r"_d(\d{8}_t\d{7})_e(\d{7})_b\d+_c\d+_oeac_ops.h5$"
                st = "%Y%m%d_t%H%M%S"

                for file in files:
                    m_sdr = re.search( ss_sdr, file )
                    m_sdr_geo = re.search( ss_sdr_geo, file )

                    if m_sdr is not None:
                        m = m_sdr
                        t1 = Time( **{ noaa_time_convention: datetime.strptime( m.group(1)[:-1], st ) } ) + timedelta( seconds=int(m.group(1)[-1])*0.1 )
                        t2 = Time( **{ noaa_time_convention: datetime.strptime( m.group(1)[:8] + m.group(2)[:-1], "%Y%m%d%H%M%S" ) } ) \
                                + timedelta( seconds=int(m.group(2)[-1])*0.1 )
                        t2 += timedelta( seconds=0.4 )
                        if t2 < t1: t2 += timedelta( days=1 )
                        rec = { 'satellite': sat, 'type': "sdr", 'path': os.path.join( root, file ), 'timerange': ( t1, t2 ) }
                        self.inventory[sat].append( rec )

                    elif m_sdr_geo is not None:
                        m = m_sdr_geo
                        t1 = Time( **{ noaa_time_convention: datetime.strptime( m.group(1)[:-1], st ) } ) + timedelta( seconds=int(m.group(1)[-1])*0.1 )
                        t2 = Time( **{ noaa_time_convention: datetime.strptime( m.group(1)[:8] + m.group(2)[:-1], "%Y%m%d%H%M%S" ) } ) \
                                + timedelta( seconds=int(m.group(2)[-1])*0.1 )
                        t2 += timedelta( seconds=0.4 )
                        if t2 < t1: t2 += timedelta( days=1 )
                        rec = { 'satellite': sat, 'type': "sdr-geo", 'path': os.path.join( root, file ), 'timerange': ( t1, t2 ) }
                        self.inventory[sat].append( rec )

    def get_jpss_atms_recs( self, satellite, timerange ): 
        """Return a listing of the paths to JPSS ATMS data files given a 
        satellite name and a time range. The timerange is a two-element 
        tuple/list with instances of timestandards.Time or datetime.datetime. 
        If it is the latter, then the datetime elements are understood to be 
        UTC."""

        #  Check input. Interpret datetime.datetime as timestandards.Time instances 
        #  if necessary. 

        if len( timerange ) != 2:
            raise noaaError( "InvalidArgument", "timerange must be a tuple/list of two elements" )

       if isinstance( timerange[0], datetime ) and isinstance( timerange[1], datetime ):
            _timerange = [ Time( utc=timerange[i] ) for i in range(2) ]
        elif isinstance( timerange[0], Time ) and isinstance( timerange[1], Time ):
            _timerange = timerange
        else:
            raise noaaError( "InvalidArgument", "The elements of timerange must both be " + \
                    "datetime.datetime or timestandards.Time" )

        ret = sorted( [ rec for rec in self.inventory[satellite] if \
                rec['timerange'][0] <= _timerange[1] and rec['timerange'][1] >= _timerange[0] ] )

        return ret

    def populate_jpss_atms( self, satellite, timerange ):
        """Download a Metop AMSU-A data that falls within a timerange.

        * satellite must be one of 'satellites' as defined in the header. 
        * timerange is a 2-element tuple/list of instances of timestandards.Time
          or instances of datetime.datetime defining the range of times over which
          to retrieve data. If they are instances of datetime.datetime, then the
          convention is that they are both UTC."""

        #  Check input. Interpret datetime.datetime as timestandards.Time instances
        #  if necessary.

        if len( timerange ) != 2:
            raise noaaError( "InvalidArgument", "timerange must be a tuple/list of two elements" )

        if isinstance( timerange[0], datetime ) and isinstance( timerange[1], datetime ):
            _timerange = [ Time( utc=timerange[i] ) for i in range(2) ]
        elif isinstance( timerange[0], Time ) and isinstance( timerange[1], Time ):
            _timerange = timerange
        else:
            raise noaaError( "InvalidArgument", "The elements of timerange must both be " + \
                    "datetime.datetime or timestandards.Time" )

        #  Get satellite record. 

        satrec = [ rec for rec in satellites_info if rec['name']==satellite ][0]
        bucket = self.s3.Bucket( satrec['bucket'] )
        mname = satrec['mname']

        #  Find day range. 

        cal = ( _timerange[0] - 60 ).calendar("utc")
        firstday = datetime( cal.year, cal.month, cal.day )
        cal = ( _timerange[1] + 60 ).calendar("utc")
        lastday = datetime( cal.year, cal.month, cal.day )

        #  Get listing of all files that satisfy the time range. 

        dt = firstday + timedelta(days=0)

        #  Loop over day. 

        while dt <= lastday: 
            keys = []
            for p in [ "ATMS-SDR", "ATMS-SDR-GEO" ]: 
                keys += [ obj.key for obj in bucket.all().filter( Prefix=p+dt.strftime("/%Y/%m/%d/") ) ]

        #  Subset keys for timerange. 

        skeys = []

        for key in keys: 
            ss = r"^[A-Z]{5}_" + mname + r"_d(\d{8}_t\d{7})_e(\d{7})_b\d+_c\d+_oeac_ops.h5$"
            m = re.search( ss, os.path.basename( key ) )

            if m is None: continue

            t1 = Time( **{ noaa_time_convention: datetime.strptime( m.group(1)[:-1], st ) } ) + timedelta( seconds=int(m.group(1)[-1])*0.1 )
            t2 = Time( **{ noaa_time_convention: datetime.strptime( m.group(1)[:8] + m.group(2)[:-1], "%Y%m%d%H%M%S" ) } ) \
                    + timedelta( seconds=int(m.group(2)[-1])*0.1 )
            t2 += timedelta( seconds=0.4 )
            if t2 < t1: t2 += timedelta( days=1 )

            if t1 <= _timerange[1] and t2 >= _timerange[0]: 
                skeys.append( key )

        #  Download subsetted list of keys. 

        for key in keys: 
            lpath = os.path.join( self.data_root, satellite, key )
            bucket.download_file( key, lpath )
    
        #  Update. 

        self.inventory_jpss_atms()

        #  Done. 

        ret = self.get_jpss_atms_recs( satellite, _timerange )

        return ret


def ATMSsdrReader(sdrfile, geofile ):
    """Read SDR and GEO data files in HDF5 format. They correspond 
    to SATMS and GATMO files in the AWS Registry of Open Data. Data 
    are loaded into an output dictionary."""

    #  Based on code convert_satms_hdf5torad.py composed by Pan Liang
    #  (AER, pan.liang@janusresearch.us). 

    sdr = {}

    with Dataset(sdrfile, 'r') as ncf:

        # ncf.set_auto_mask(False)

        grpSDR = ncf['/All_Data/ATMS-SDR_All']

        # Dimensions

        natrack = grpSDR.dimensions['phony_dim_0'].size
        nxtrack = grpSDR.dimensions['phony_dim_1'].size
        nchannels = grpSDR.dimensions['phony_dim_2'].size

        sdr.update( { 'natrack':natrack, 'nxtrack':nxtrack, 'nchannels':nchannels } )

        # Global attributes

        sdr.update( { 
                     'Distributor': ncf.Distributor, 
                     'Mission_Name': ncf.Mission_Name, 
                     'N_GEO_Ref': ncf.N_GEO_Ref, 
                     'N_HDF_Creation_Date': ncf.N_HDF_Creation_Date, 
                     'N_HDF_Creation_Time': ncf.N_HDF_Creation_Time, 
                     'Platform_Short_Name': ncf.Platform_Short_Name, 
                     'Input_SDR_Filename': os.path.basename(sdrfile) 
                    } )

        # Read SDR variables

        grpSDR = ncf['/All_Data/ATMS-SDR_All']

        # Tb dimension (atrack, xtrack, channel)

        tbshort =  grpSDR.variables['BrightnessTemperature'][:].

        # Tb scale+offset factors dimension 2

        tbfactors = grpSDR.variables['BrightnessTemperatureFactors'][:]

        # Apply the scale+offset factors

        tb = np.full_like(tbshort, fill_value=fill_value, dtype=np.float32)
        tb[:] = tbshort[:]*tbfactors[0] + tbfactors[1]

        # Acquisition time for each FOV, dimension (atrack, xtrack)
        # Microsecond since IET(1/1/1958), convert to seconds by 1e-6

        BeamTime = grpSDR.variables['BeamTime'][:]*1e-6

        # Generate the epoch for the times. 

        epoch = Time( utc=Calendar(year=1958, month=1, day=1) )

        # NEdt diemension (atrack, channel)

        NEdTCold = grpSDR.variables['NEdTCold'][:]
        NEdTWarm = grpSDR.variables['NEdTWarm'][:]

        # Add output variables. 

        sdr.update( { 
                     'tb': tb, 
                     'epoch': epoch, 
                     'BeamTime': BeamTime, 
                     'NEdTCold': NEdTCold, 
                     'NEdTWarm': NEdTWarm
                    } )

    # Read GEO variables

    with Dataset(geofile,'r') as ncf: 

        grpGEO = ncf['/All_Data/ATMS-SDR-GEO_All']

        # Uncomment the next line to print out all the parameters
        # print(grpGEO)

        # Variables for each FOV, dimension (atrack, xtrack)

        Latitude = grpGEO.variables['Latitude'][:]
        Longitude = grpGEO.variables['Longitude'][:]
        SatelliteAzimuthAngle = grpGEO.variables['SatelliteAzimuthAngle'][:]
        SatelliteZenithAngle = grpGEO.variables['SatelliteZenithAngle'][:]
        SolarAzimuthAngle = grpGEO.variables['SolarAzimuthAngle'][:]
        SolarZenithAngle = grpGEO.variables['SolarZenithAngle'][:]

        # Add output variables. 

        sdr.update( { 
                     'Latitude': Latitude, 
                     'Longitude': Longitude, 
                     'SatelliteAzimuthAngle': SatelliteAzimuthAngle, 
                     'SatelliteZenithAngle': SatelliteZenithAngle, 
                     'SolarAzimuthAngle': SolarAzimuthAngle, 
                     'SolarZenithAngle': SolarZenithAngle
                    } )

    return sdr

