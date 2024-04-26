"""eumetsat.py


Author: Stephen Leroy (sleroy@aer.com)
Date: February 29, 2024

This module contains classes and methods useful for accessing the EUMETSAT Data Store. 
Note that you'll have to create an account on the EUMETSAT Data Store beforehand, set 
credentials using "eumdac set-credentials" at the Linux command line, and then set the 
default download root path using the setdefaults method below.
"""


import os, re, stat, json, subprocess 
from eumdac.token import AccessToken
from eumdac.datastore import DataStore
from datetime import datetime, timedelta
from .timestandards import Time
from .constants_and_utils import defaults_file


#  Definitions. 

metop_satellites = [ "Metop-A", "Metop-B", "Metop-C" ]
eumetsat_time_convention = "utc"
HOME = os.path.expanduser( "~" )
root_path_variable = "eumetsat_data_store_root"

#  Exception handling. 

class Error( Exception ): 
    pass 

class eumetsatError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment 


def setdefaults( root_path=None, eumetsattokens=None ): 
    """This method sets the default root path for EUMETSAT Data Store downloads.
    It also can set the consumer key and consumer secret as created on the 
    EUMETSAT Data Store website/account. If the keyword eumetsattokens is given, 
    it must be a 2-element tuple/list containing the user's consumer key and 
    consumer secret as provided for the user's account on the EUMETSAT Data 
    Store."""

    ret = { 'status': None, 'messages': [], 'comments': [], 'data': None }
    new_defaults = {}

    if root_path is not None:

        try:
            os.makedirs( root_path, exist_ok=True )

        except:
            ret['status'] = "fail"
            ret['messages'].append( "BadPathName" )
            ret['comments'].append( f'Unable to create root_path ("{root_path}") as a directory.' )
            return ret

        new_defaults.update( { root_path_variable: os.path.abspath( root_path ) } )

    #  Define consumer key, consumer secret. 

    if eumetsattokens is not None: 
        if isinstance(eumetsattokens,tuple) or isinstance(eumetsattokens,list): 
            if len(eumetsattokens) == 2: 
                command = [ "eumdac", "set-credentials", eumetsattokens[0], eumetsattokens[1] ]
                resp = subprocess.run( command, capture_output=True )

            else:
                ret['status'] = "fail"
                ret['messages'].append( "InvalidArgument" )
                ret['comments'].append( "eumetsattokens must have length 2" )
                return ret

        else:
            ret['status'] = "fail"
            ret['messages'].append( "InvalidArgument" )
            ret['comments'].append( "eumetsattokens must be a tuple or a list" )
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

    return


def checkdefaults(): 
    """Check that all of the defaults needed for execution have been set."""

    ret = { 'status': None, 'messages': [], 'comments': [] }

    #  Check for existence of ~/.eumdac, which is created by the eumdac --set-credentials command. 

    HOME = os.path.expanduser( "~" )
    credentials_file = os.path.join( HOME, ".eumdac", "credentials" )
    if not os.path.exists( credentials_file ): 
        ret['status'] = "fail"
        ret['messages'].append( "MissingCredentialsFile" )
        ret['comments'].append( 'Credentials file for the EUMETSAT Data Store is missing; be certain to run "rotcol setdefaults eumetsat ..."' )
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
        ret['messages'].append( "MissingEUMETSATDataStoreRoot" )
        ret['comments'].append( 'Missing data root for EUMETSAT Data Store; be certain to run "rotcol setdefaults eumetsat --dataroot ..."' )
        return ret

    #  Done. 

    ret['status'] = "success"
    return ret


class EUMETSATDataStore(): 
    """Class to handle interaction with the EUMETSAT Data Store."""

    def __init__( self ): 
        """Create an instance of EUMETSATDataStore. data_root should be an 
        absolute path to the storage location for downloaded EUMETSAT Data
        Store data."""

        #  Get credentials. 

        credentials_file = os.path.join( HOME, ".eumdac", "credentials" )
        if not os.path.exists( credentials_file ): 
            raise eumetsatError( "CredentialsFileNotFound", 
                    "Be sure to run eumdac set-credentials on the command line" )

        f = open( credentials_file, 'r' ) 
        self.credentials = tuple( f.readline().split(",") )
        f.close()

        self.token = None
        self.datastore = None

        #  Get root data path. 

        with open( defaults_file, 'r' ) as f: 
            defaults = json.load( f )
            self.data_root = defaults[root_path_variable]

        self.inventory = {}

        #  Initialize token, inventory. 

        self.get_token()
        self.inventory_metop_amsua()

    def get_token( self ): 
        self.token = AccessToken( self.credentials )
        self.datastore = DataStore( self.token )

    def inventory_metop_amsua( self ): 
        """Create an inventory of the files available on the local file system, 
        keeping information on the absolute path and the time range of data."""

        #  Get list of satellites. 

        data_root = os.path.join( self.data_root, "amsua" )
        satellites = [ p for p in os.listdir( data_root ) if re.search( r"^Metop-", p ) \
                and os.path.isdir( os.path.join( data_root, p ) ) ]

        #  Initialize inventory. 

        self.inventory = { sat: [] for sat in satellites }

        #  Loop over satellites. 

        for sat in satellites: 

            for root, subdirs, files in os.walk( os.path.join( data_root, sat ) ): 
                subdirs.sort()
                files.sort()

                ss = "^" + sat + r"_AMSAL1_(\d{8}T\d{6})Z_(\d{8}T\d{6})Z\.nc$"
                st = "%Y%m%dT%H%M%S" 

                for file in files: 
                    m = re.search( ss, file )
                    if m is None: 
                        continue
                    t1 = Time( **{ eumetsat_time_convention: datetime.strptime( m.group(1), st ) } ) 
                    t2 = Time( **{ eumetsat_time_convention: datetime.strptime( m.group(2), st ) } ) 

                    rec = { 'satellite': sat, 'path': os.path.join( root, file ), 'timerange': ( t1, t2 ) }
                    self.inventory[sat].append( rec )

    def get_metop_amsua_paths( self, satellite, timerange ): 
        """Return a listing of the paths to Metop AMSU-A data files given a 
        satellite name and a time range. The timerange is a two-element 
        tuple/list with instances of timestandards.Time or datetime.datetime. 
        If it is the latter, then the datetime elements are understood to be 
        UTC."""

        #  Check input. Interpret datetime.datetime as timestandards.Time instances 
        #  if necessary. 

        if len( timerange ) != 2: 
            raise eumetsatError( "InvalidArgument", "timerange must be a tuple/list of two elements" )

        if isinstance( timerange[0], datetime ) and isinstance( timerange[1], datetime ): 
            _timerange = [ Time( utc=timerange[i] ) for i in range(2) ]
        elif isinstance( timerange[0], Time ) and isinstance( timerange[1], Time ): 
            _timerange = timerange
        else: 
            raise eumetsatError( "InvalidArgument", "The elements of timerange must both be " + \
                    "datetime.datetime or timestandards.Time" )

        ret = sorted( [ rec['path'] for rec in self.inventory[satellite] if \
                rec['timerange'][0] <= _timerange[1] and rec['timerange'][1] >= _timerange[0] ] )

        return ret

    def populate_metop_amsua( self, satellite, timerange ): 
        """Download a Metop AMSU-A data that falls within a timerange. 

        * satellite must be one of Metop-A, Metop-B, Metop-C. 
        * timerange is a 2-element tuple/list of instances of timestandards.Time 
          or instances of datetime.datetime defining the range of times over which 
          to retrieve data. If they are instances of datetime.datetime, then the 
          convention is that they are both UTC."""

        #  Check input. Interpret datetime.datetime as timestandards.Time instances 
        #  if necessary. 

        if len( timerange ) != 2: 
            raise eumetsatError( "InvalidArgument", "timerange must be a tuple/list of two elements" )

        if isinstance( timerange[0], datetime ) and isinstance( timerange[1], datetime ): 
            _timerange = [ Time( utc=timerange[i] ) for i in range(2) ]
        elif isinstance( timerange[0], Time ) and isinstance( timerange[1], Time ): 
            _timerange = timerange
        else: 
            raise eumetsatError( "InvalidArgument", "The elements of timerange must both be " + \
                    "datetime.datetime or timestandards.Time" )

        #  Load the data collection interface. 

        amsua_collection = self.datastore.get_collection( "EO:EUM:DAT:METOP:AMSUL1" )

        #  Establish time interval and get inventory of data at the EUMETSAT Data 
        #  Store

        start = _timerange[0].calendar(eumetsat_time_convention).isoformat()
        end = _timerange[1].calendar(eumetsat_time_convention).isoformat()

        command = [ "eumdac", "search", "-c", "EO:EUM:DAT:METOP:AMSUL1", "--satellite",  
                satellite, "-s", start, "-e", end ]

        if datetime.now() >= self.token.expiration: 
            self.get_token()

        ret = subprocess.run( command, capture_output=True )
        inventory = sorted( ret.stdout.decode().split("\n") )

        #  Scan inventory of files; check to see if all files already exist in data 
        #  repository directory. 

        download = False
        matched_files = [] 

        for item in inventory: 

            m = re.search( r"(\d{14})Z_(\d{14})Z", item )
            if not m: continue

            #  Start time and end time of inventory item. 

            dt1 = datetime.strptime( m.group(1), "%Y%m%d%H%M%S" )
            dt2 = datetime.strptime( m.group(2), "%Y%m%d%H%M%S" )

            #  Does the directory corresponding to this inventory item exist on the 
            #  local file system? If not, then download all data. 

            dirpath = os.path.join( self.data_root, "amsua", satellite, f'{dt1.year:4d}', f'{dt1.month:02d}', f'{dt1.day:02d}' )
            if not os.path.isdir( dirpath ): 
                download = True
                break

            #  Get listing of all existing files on for this day. 

            local_files = os.listdir( dirpath )
            ss = dt1.strftime( "%Y%m%dT%H%M%SZ" ) + "_" + dt2.strftime( "%Y%m%dT%H%M%SZ" ) 
            match = [ file for file in local_files if re.search( ss, file ) ]

            #  Does a matched file exist? If not, download all files. 

            if len(match) == 0: 
                download = True
                break
            elif len(match) == 1: 
                matched_files.append( os.path.join( dirpath, match[0] ) )

        #  Download data if necessary. 

        if download: 

            os.makedirs( "tmp", exist_ok=True )

            #  Flush tmp holding directory. 

            existing_files = os.listdir( "tmp" )
            for file in existing_files: 
                os.unlink( os.path.join( "tmp", file ) )

            #  Initialize and enter loop of download chunks. Each download chunk is limited 
            #  to 5 days. 

            current_start_time = _timerange[0] + 0
            outpaths = []

            while current_start_time < _timerange[1]: 

                #  Establish time interval for download. 

                current_end_time = min( [ current_start_time + 86400 * 5, _timerange[1] ] )

                #  Define download command. 

                current_start = current_start_time.calendar(eumetsat_time_convention).isoformat()
                current_end = current_end_time.calendar(eumetsat_time_convention).isoformat()

                command = [ "eumdac", "download", "--collection", "EO:EUM:DAT:METOP:AMSUL1",
                        "--satellite", satellite, "-s", current_start, "-e", current_end, "-o", "tmp", "-y",
                        "--chain", f"product: AMSAL1, format: netcdf4" ]

                #  Renew access token if expired. 

                if datetime.now() >= self.token.expiration: 
                    self.get_token()

                ret = subprocess.run( command, capture_output=True )

                #  Scan the downloaded files, establish repository path according to 
                #  time of the first sounding in each file, and transfer them into the 
                #  repository. 

                files = os.listdir( "tmp" )

                for file in files:
                    inpath = os.path.join( "tmp", file )
                    m = re.search( r"^AMSAL1_(\d{8}T\d{6})Z_(\d{8}T\d{6})Z_epct_.*\.nc$", file )
                    t = datetime.strptime( m.group(1), "%Y%m%dT%H%M%S" )
                    outpath = os.path.join( self.data_root, "amsua", satellite, 
                            f'{t.year:4d}', f'{t.month:02d}', f'{t.day:02d}',
                            f'{satellite}_AMSAL1_{m.group(1)}Z_{m.group(2)}Z.nc' )
                    if not os.path.exists( outpath ):
                        os.makedirs( os.path.dirname(outpath), exist_ok=True )
                        os.link( inpath, outpath )
                    os.unlink( inpath )
                    outpaths.append( outpath )

                #  Next 5-day chunk...

                current_start_time += 86400 * 5

            #  Exit loop over 5-day chunks.

            #  Refresh inventory. 

            self.inventory_metop_amsua()

        #  Done. 

        ret = self.get_metop_amsua_paths( satellite, timerange )

        return ret

