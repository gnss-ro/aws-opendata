"""nasa_earthdata.py

Author: Stephen Leroy (sleroy@aer.com)
Date: March 1, 2024

This module contains classes and methods useful for accessing the NASA 
DAAC.  Note that you'll have to create an account on the NASA Earthdata 
server."""


import os, re, stat, json, subprocess
import earthaccess
import requests, netrc, boto3
from platform import system
from datetime import datetime, timedelta, timezone
from .timestandards import Time
from .constants_and_utils import defaults_file

#  Definitions. 

#  ATMS pointers are for version 3 level 1B radiances. 
#  AIRS pointers are for L1C. 
#  CrIS pointers are for "full spectral resolution, version 3" level 1B radiances. 

Satellites = { 
        'Aqua': { 
                'aliases': [ "Aqua" ], 
                'airs': "10.5067/VWD3DRC07UEN"      # This is the AIRS L1C IR product (AIRICRAD)
        }, 
        'Suomi-NPP': { 
                'aliases': [ "Suomi-NPP", "SNPP" ], 
                'atms': "10.5067/FCXKUUE9VCLN", 
                'cris': "10.5067/ZCRSHBM5HB23"
        }, 
        'JPSS-1': { 
                'aliases': [ "JPSS-1", "NOAA-20" ], 
                'atms': "10.5067/MUNII2DHSSY3", 
                'cris': "10.5067/LVEKYTNSRNKP" 
        }, 
        'JPSS-2': {
                'aliases': [ "JPSS-2", "NOAA-21" ] 
        }
    }

HOME = os.path.expanduser( "~" )
root_path_variable = "nasa_earthdata_root"
earthdata_machine = "urs.earthdata.nasa.gov"
time_limit = timedelta( seconds=3600 )

#  String parsing. 

search_parse = { 
        'atms': { 
            'file': re.compile( r'^SNDR\..*(\d{8}T\d{4})\.m.*\.nc$' ), 
            'time': "%Y%m%dT%H%M" }, 
        'airs': { 
            'file': re.compile( r'^AIRS.(\d{4}\.\d{2}\.\d{2})\.(\d{3})\.L1C\.AIRS_Rad\.v[\d\.]+\.G\d+\.hdf$' ), 
            'time': "%Y.%m.%d" }
    }

#  Establist the netrc file name. 

if system() == "Windows": 
    netrc_file = os.path.join( HOME, "_netrc" )
else: 
    netrc_file = os.path.join( HOME, ".netrc" )

#  Define metadata on the Earthdata DAACs. 

earthdata_daacs = {
        'podaac': { 'endpoint': "https://archive.podaac.earthdata.nasa.gov/s3credentials", 
                    'region': "us-west-2" }, 
        'gesdisc': { 'endpoint': "https://data.gesdisc.earthdata.nasa.gov/s3credentials", 
                    'region': "us-west-2" } }

#  Exception handling. 

class Error( Exception ): 
    pass 

class earthdataError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment 


def setdefaults( root_path=None, earthdatalogin=None ): 
    """This method sets the default root path for NASA GDAAC downloads.
    Optionally, it also allows you to register your Earthdata username and 
    password in your home .netrc (if you haven't done so already).

    The Earthdata username and password can be given as a 2-tuple 
    (username,password) in optional argument earthdatalogin."""

    ret = { 'status': None, 'messages': [], 'comments': [], 'data': None }
    new_defaults = {}

    #  Set root data path. Create the directory. 

    if root_path is not None:

        try:
            os.makedirs( root_path, exist_ok=True )

        except:
            ret['status'] = "fail"
            ret['messages'].append( "BadPathName" )
            ret['comments'].append( f'Unable to create root_path ("{root_path}") as a directory.' )
            return ret

        new_defaults.update( { root_path_variable: os.path.abspath( root_path ) } )

    #  Write Earthdata login username and password to .netrc file if 
    #  provided. 

    if earthdatalogin is not None: 
        if isinstance(earthdatalogin,tuple) or isinstance(earthdatalogin,list): 
            if len(earthdatalogin) == 2: 

                if os.path.exists( netrc_file ): 
                    with open( netrc_file, 'r' ) as f: 
                        lines = f.readlines()
                else: 
                    lines = []
                
                #  Catalog lines according to machine. 

                catalog = {}
                for line in lines: 
                    m = re.search( r"^machine\s+(\S+)", line )
                    if m: 
                        catalog.update( { m.group(1): line.strip() } )

                #  Update catalog to include earthdata. 

                catalog.update( { earthdata_machine: \
                        "machine {:} login {:} password {:}".format( earthdata_machine, *earthdatalogin ) 
                        } )

                #  Write new netrc. 

                with open( netrc_file, 'w' ) as f: 
                    for machine, line in catalog.items(): 
                        f.write( line + "\n" )

                os.chmod( netrc_file, stat.S_IRUSR | stat.S_IWUSR )

            else: 

                ret['status'] = "fail"
                ret['messages'].append( "InvalidArgument" )
                ret['comments'].append( "earthdatalogin must have length 2" )
                return ret

        else: 

            ret['status'] = "fail"
            ret['messages'].append( "InvalidArgument" )
            ret['comments'].append( "earthdatalogin must be a tuple or a list" )
            return ret

    if len( new_defaults ) > 0:

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

    return ret


def checkdefaults(): 
    """Check that all of the defaults needed for execution have been set."""

    ret = { 'status': None, 'messages': [], 'comments': [] }

    #  Check to see that the Earthdata machine has been defined in .netrc. 

    if not os.path.exists( netrc_file ): 
        ret['status'] = "fail"
        ret['messages'].append( "MissingNetrcFile" )
        ret['comments'].append( 'The .netrc file does not exists; be certain to run ' + \
                '"rotcol setdefaults earthdata ..."' )
        return ret

    #  Check to see that the earthdata_machine entry exists in the netrc file. 

    with open( netrc_file, 'r' ) as f: 
        lines = f.readlines()

    for line in lines: 
        m = re.search( r"^machine\s+(\S+)", line )
        if m: 
            machine = m.group(1)
            if machine == earthdata_machine: 
                break
        else: 
            machine = None

    if machine != earthdata_machine: 
        ret['status'] = "fail"
        ret['messages'].append( "MissingEarthdataMachine" )
        ret['comments'].append( 'The .netrc file does not contain an entry for NASA Earthdata; be ' + \
                'certain to run "rotcol setdefaults earthdata --username ... --password ..."' )
        return ret

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
        ret['messages'].append( "MissingEarthdataRoot" )
        ret['comments'].append( 'Missing data root for NASA Earthdata; be certain to run ' + \
                '"rotcol setdefaults earthdata --dataroot ..."' )
        return ret

    #  Done. 

    ret['status'] = "success"
    return ret


class NASAEarthdata(): 
    """Class to handle interaction with NASA DAACs."""

    def __init__( self ): 
        """Create an instance of EUMETSATDataStore. data_root should be an 
        absolute path to the storage location for downloaded EUMETSAT Data
        Store data."""

        #  Configure credentials. 

        self.earthaccess = earthaccess.login( strategy="netrc", persist=True )

        #  Get root data path. 

        with open( defaults_file, 'r' ) as f: 
            defaults = json.load( f )
            self.data_root = defaults[root_path_variable]
            os.makedirs( self.data_root, exist_ok=True )

        #  Initialize inventory. 

        self.inventory = {}
        self.regenerate_inventory()

        return

    def regenerate_inventory( self ): 
        """Create an inventory of the NASA Earthdata files available on the 
        local file system (as obtained from GES DISC)."""

        #  Get list of satellites. 

        instruments = [ p for p in os.listdir( self.data_root ) if os.path.isdir( os.path.join( self.data_root, p ) ) ]

        #  Loop over instruments. 

        for instrument in instruments: 

            data_root = os.path.join( self.data_root, instrument )

            #  Initialize inventory. 

            if os.path.isdir( data_root ): 
                satellites = [ p for p in os.listdir( data_root ) if p in Satellites.keys() \
                        and os.path.isdir( os.path.join( data_root, p ) ) ]
            else: 
                satellites = []

            if instrument not in self.inventory: 
                self.inventory.update( { instrument: {} } )

            #  Loop over satellites. 

            for sat in satellites: 

                for root, subdirs, files in os.walk( os.path.join( data_root, sat ) ): 
                    subdirs.sort()
                    files.sort()

                    if sat not in self.inventory[instrument]: 
                        self.inventory[instrument].update( { sat: [] } )

                    for file in files: 
                        m = search_parse[instrument]['file'].search( file )
                        if m is None: 
                            continue

                        if instrument == "atms": 
                            t1 = Time( utc = datetime.strptime( m.group(1), search_parse[instrument]['time'] ) ) 
                        elif instrument == "airs": 
                            t1 = Time( tai = datetime.strptime( m.group(1), search_parse[instrument]['time'] ) ) + int( m.group(2) ) * 360
                        t2 = t1 + 360

                        rec = { 'satellite': sat, 'path': os.path.join( root, file ), 'timerange': ( t1, t2 ) }
                        self.inventory[instrument][sat].append( rec )

        return

    def get_paths( self, satellite, instrument, timerange ): 
        """Return a listing of the paths to sounder data files given a 
        satellite name, and instrument name, and a time range. The timerange 
        is a two-element tuple/list with instances of timestandards.Time or 
        datetime.datetime. If it is the latter, then the datetime elements 
        are understood to be UTC."""

        #  Check input. Interpret datetime.datetime as timestandards.Time instances 
        #  if necessary. 

        if satellite not in Satellites.keys(): 
            raise earthdataError( "InvalidArgument", "The satellite must be one of " + \
                    ", ".join( list( Satellites.keys() ) ) )

        elif instrument not in Satellites[satellite].keys() or instrument == "aliases": 
            raise earthdataError( "InvalidArgument", 
                    f"The instrument {instrument} is not defined for satellite {satellite}" )

        if not isinstance( timerange, tuple ) and not isinstance( timerange, list ): 
            raise earthdataError( "InvalidArgument", "timerange must be a tuple/list of two elements" )

        if len( timerange ) != 2: 
            raise earthdataError( "InvalidArgument", "timerange must be a tuple/list of two elements" )

        if isinstance( timerange[0], datetime ) and isinstance( timerange[1], datetime ): 
            _timerange = [ Time( utc=timerange[i] ) for i in range(2) ]
        elif isinstance( timerange[0], Time ) and isinstance( timerange[1], Time ): 
            _timerange = timerange
        else: 
            raise earthdataError( "InvalidArgument", "The elements of timerange must both be " + \
                    "datetime.datetime or timestandards.Time" )

        ret = []
        if instrument in self.inventory.keys(): 
            if satellite in self.inventory[instrument].keys(): 
                ret = sorted( [ rec['path'] for rec in self.inventory[instrument][satellite] if \
                        rec['timerange'][0] <= _timerange[1] and rec['timerange'][1] >= _timerange[0] ] )

        return ret

    def populate( self, satellite, instrument, timerange ): 
        """Download sounder data that fall within a timerange. 

        * satellite must be one of Satellites.keys(). 
        * instrument is one of 'atms', 'airs'. 
        * timerange is a 2-element tuple/list of instances of timestandards.Time 
          or instances of datetime.datetime defining the range of times over which 
          to retrieve data. If they are instances of datetime.datetime, then the 
          convention is that they are both UTC."""

        #  Check input. Interpret datetime.datetime as timestandards.Time instances 
        #  if necessary. 

        if satellite not in Satellites.keys(): 
            raise earthdataError( "InvalidArgument", "The satellite must be one of " + \
                    ", ".join( list( Satellites.keys() ) ) )

        elif instrument not in Satellites[satellite].keys() or instrument == "aliases": 
            raise earthdataError( "InvalidArgument", 
                    f"The instrument {instrument} is not defined for satellite {satellite}" )

        if not isinstance( timerange, tuple ) and not isinstance( timerange, list ): 
            raise earthdataError( "InvalidArgument", "timerange must be a tuple/list of two elements" )

        if len( timerange ) != 2: 
            raise earthdataError( "InvalidArgument", "timerange must be a tuple/list of two elements" )

        if isinstance( timerange[0], datetime ) and isinstance( timerange[1], datetime ): 
            _timerange = [ Time( utc=timerange[i] ) for i in range(2) ]
        elif isinstance( timerange[0], Time ) and isinstance( timerange[1], Time ): 
            _timerange = timerange
        else: 
            raise earthdataError( "InvalidArgument", "The elements of timerange must both be " + \
                    "datetime.datetime or timestandards.Time" )

        #  Query the local and remote inventories. 

        local_inventory = []
        if instrument in self.inventory.keys(): 
            if satellite in self.inventory[instrument].keys(): 
                local_inventory = [ e['path'] for e in self.inventory[instrument][satellite] ]
        etimerange = [ _timerange[0]-360, _timerange[1]+360 ]
        temporal = tuple( [ t.calendar("utc").datetime().strftime("%Y-%m-%d") for t in etimerange ] ) 
        
        remote_inventory = earthaccess.search_data( doi=Satellites[satellite][instrument], temporal=temporal )

        #  Get a listing of data files that are available at Earthdata but not in the local inventory. 

        local_basenames = [ os.path.basename( p ) for p in local_inventory ]

        get = []
        for p in remote_inventory: 
            basename = os.path.basename( p.data_links()[0] )
            if basename in local_basenames: 
                continue
            m = search_parse[instrument]['file'].search( basename )
            t = Time( utc=datetime.strptime( m.group(1), search_parse[instrument]['time'] ) )
            if instrument == "airs": 
                t += ( int( m.group(2) ) - 1 ) * 360
            if t+360 >= _timerange[0] and t <= _timerange[1]: 
                get.append( p )

        #  Get data files that we don't yet have. 

        if len( get ) > 0: 

            earthaccess.download( get, "tmp" )
            files = sorted( [ os.path.join( "tmp", f ) for f in os.listdir( "tmp" ) ] )

            for file in files: 

                #  Parse file name for time of granule. 

                m = search_parse[instrument]['file'].search( os.path.basename(file) )
                dt = datetime.strptime( m.group(1), search_parse[instrument]['time'] )
                if instrument == "airs": 
                    dt += timedelta( seconds = int( m.group(2) ) * 360 )

                #  Define local path for file. 

                lpath = os.path.join( self.data_root, instrument, satellite, 
                            f'{dt.year:02d}', f'{dt.month:02d}', f'{dt.day:02d}', 
                            os.path.basename( file ) )

                #  Move file. 

                os.makedirs( os.path.dirname( lpath ), exist_ok=True )
                os.link( file, lpath )
                os.unlink( file )

        #  Regenerate inventory. 

        self.regenerate_inventory()

        return 

