"""Utilities for reading Space-Track TLE data."""

#  Imports. 

import os, re, json, stat
import numpy as np
import requests
from datetime import datetime, timedelta
from .timestandards import Time, Calendar
from .constants_and_utils import defaults_file

#  Definitions. 

root_path_variable = "spacetrack_data_root"
username_variable = "spacetrack_username"
password_variable = "spacetrack_password"

#  REST API querying command parameters. 

uri_base = "https://www.space-track.org"
authenticate_command = "ajaxauth/login"

#  There are three arguments to str.format in download_command: 
#   (1) 5-digit NORAD catalogue number specifying the satellite
#   (2) Begin date of the epoch range in the format YYYY-MM-DD
#   (3) End date of the epoch range in the format YYYY-MM-DD

download_command = "basicspacedata/query/class/tle/NORAD_CAT_ID/{:}/EPOCH/{:}--{:}/" + \
        "orderby/NORAD_CAT_ID%20asc/format/tle/emptyresult/show"

#  Parameters. 

spacetrack_time_convention = "utc"
gps0 = Time( gps=0 )

#  Table of satellites. 

Satellites = [ { 'name': "Metop-A", 'norad_number': 29499 }, 
              { 'name': "Metop-B", 'norad_number': 38771 }, 
              { 'name': "Metop-C", 'norad_number': 43689 }, 
              { 'name': "Suomi-NPP", 'norad_number': 37849 }, 
              { 'name': "JPSS-1", 'norad_number': 43013 }, 
              { 'name': "JPSS-2", 'norad_number': 54234 }, 
              { 'name': "Aqua", 'norad_number': 27424 } ]


#  Exception handling. 

class Error( Exception ): 
    pass

class spacetrackError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment


def setdefaults( root_path=None, spacetracklogin=None ):
    """This method sets the default root path for Spacetrack TLE data. If the 
    keyword spacetracklogin is given, it must be a 2-element tuple/list 
    containing the user's username and password for the user's Space-Track 
    account."""

    ret = { 'status': None, 'messages': [], 'comments': [], 'data': None }
    new_defaults = {}

    #  Update data root path and create the directory. 

    if root_path is not None:

        try:
            os.makedirs( root_path, exist_ok=True )

        except:
            ret['status'] = "fail"
            ret['messages'].append( "BadPathName" )
            ret['comments'].append( f'Unable to create root_path ("{root_path}") as a directory.' )
            return ret

        new_defaults.update( { root_path_variable: os.path.abspath( root_path ) } )

    #  Update username and password. 

    if spacetracklogin is not None: 

        if isinstance(spacetracklogin,tuple) or isinstance(spacetracklogin,list): 
            if len(spacetracklogin) == 2: 
                new_defaults.update( { username_variable: spacetracklogin[0], 
                          password_variable: spacetracklogin[1] } ) 

            else:
                ret['status'] = "fail"
                ret['messages'].append( "InvalidArgument" )
                ret['comments'].append( "spacetracklogin must have length 2" )
                return ret

        else:
            ret['status'] = "fail"
            ret['messages'].append( "InvalidArgument" )
            ret['comments'].append( "spacetracklogin must be a tuple or a list" )
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

    #  Read defaults file. 

    with open( defaults_file, 'r' ) as f: 
        defaults = json.load( f )

    #  Check that the data root path has been set. 

    keys = defaults.keys()

    if root_path_variable not in keys: 
        ret['status'] = "fail"
        ret['messages'].append( "MissingEarthdataRoot" )
        ret['comments'].append( 'Missing data root for Space-Track; be certain to run "rotcol setdefaults spacetrack --dataroot ..."' )
        return ret

    #  Check that the username and password have been set. 

    if username_variable not in keys and password_variable not in keys: 
        ret['status'] = "fail"
        ret['messages'].append( "MissingSpacetrackCredentials" )
        ret['comments'].append( 'Missing credentials for Space-Track; be certain to run ' + \
                '"rotcol setdefaults spacetrack --username ... --password ..."' )
        return ret

    #  Done. 

    ret['status'] = "success"
    return ret


class SpacetrackSatellite(): 
    """This class contains the TLE data, as obtained from Spacetrack, for one 
    satellite."""

    def __init__( self, Spacetrack_instance, norad_number ): 
        """Create an instance of class SpacetrackSatellite. It will contain the 
        TLE data for a satellite identified by its NORAD catalogue number 
        (norad_number). The first argument is the instance of class Spacetrack 
        from which this class is drawn."""

        if not isinstance( Spacetrack_instance, Spacetrack ): 
            raise spacetrackError( "InvalidArgument", "First argument to " + \
                    "SpacetrackSatellite not an instance of class Spacetrack" )

        #  Initialize. 

        self.status = None
        self.nrecs = None
        self.datetimes = None
        self.tles = None

        self.spacetrack = Spacetrack_instance
        self.norad_number = norad_number
        norad_number_string = f'{norad_number:05d}'

        if norad_number_string not in self.spacetrack.catalogue.keys(): 
            self.status = "success"
            self.nrecs = 0
            self.datetimes = []
            self.tles = []
        else: 
            self.build_instance()

        pass

    def build_instance( self ): 
        """Reconstruct the data in this instance."""

        data = {}

        norad_number_string = f'{self.norad_number:05d}'
        for file in self.spacetrack.catalogue[norad_number_string]: 

            f = open( file, 'r' )
            lines = f.readlines()
            f.close()

            if len(lines) % 2 == 1: 
                raise spacetrackError( "InvalidData", f'file {file} has an odd number of lines' )

            for iline in range(0,len(lines),2): 

                tle_line1 = lines[iline]
                tle_line2 = lines[iline+1]

                if tle_line1[0] != "1": 
                    raise spacetrackError( "InvalidData", 
                            f'Line {iline+1} in file {file} should be the first line of a TLE but is not' )

                if tle_line2[0] != "2": 
                    raise spacetrackError( "InvalidData", 
                            f'Line {iline+2} in file {file} should be the second line of a TLE but is not' )

                #  Get the time of the record. 

                year = 2000 + int( tle_line1[18:20] )
                doy = int( tle_line1[20:23] )
                fday = float( tle_line1[23:32] )

                time = Time( **{ spacetrack_time_convention: Calendar(year,1,1) } ) + 86400 * ( doy + fday - 1 )
                rec = { 'time': time, 'tle': ( tle_line1, tle_line2 ) }
                data.update( { time.calendar(spacetrack_time_convention).isoformat(timespec="seconds"): rec } )

        #  Sort the records. 

        sorted_key_list = sorted( list( data.keys() ) )

        #  Create data contents. 

        self.nrecs = len( sorted_key_list )
        self.datetimes = [ data[key]['time'] for key in sorted_key_list ]
        self.tles = [ data[key]['tle'] for key in sorted_key_list ]

        self.status = "success"

        return

    def select( self, timerange, extend=None ): 
        """Generate a list of TLEs. Each element of output will be a 2-tuple containing 
        the two lines of a TLE. All TLEs in the list must fall within a range of 
        datetimes defined by the two-element tuple/list datetimerange. 

        Arguments: 

        timerange       A two-element tuple/list containing two instances of timestandards.Time. 
                        The first element defines the start time of the TLE list to be 
                        generated; the second defines the end time of the TLE list. 

        extend          A two-element tuple/list of integers defining the how far to extend 
                        the list of TLEs before the datetimerange and after the datetimerange. 

        Example: 

        data = SpacetrackSatellite.select( 
                timerange = [ timestandards.Time( utc=timestandards.Calendar(2023,6,7) ), 
                                timestandards.Time( utc=timestandards.Calendar(2023,6,8) ) ], 
                extend = [0,1] )

        The statement generates a list of TLEs for the satellite defined by SpacetrackSatllite
        for the day of June 7, 2023, and appends to the list the first TLE for the succeeding 
        day."""

        if not isinstance(timerange,list) and not isinstance(timerange,tuple): 
            raise spacetrackError( "InvalidArgument", 'timerange must be a tuple or list' )

        if len( timerange ) != 2: 
            raise spacetrackError( "InvalidArgument", 'timerange must containing two elements' )

        if not isinstance(timerange[0],Time) or not isinstance(timerange[1],Time): 
            raise spacetrackError( "InvalidArgument", 'timerange must consist of timestandards.Time instances' )

        if extend is not None: 

            if not isinstance(extend,list) and not isinstance(extend,tuple): 
                raise spacetrackError( "InvalidArgument", 'extend must be a tuple or list' )

            if len( extend ) != 2: 
                raise spacetrackError( "InvalidArgument", 'extend must containing two elements' )

            if not isinstance(extend[0],int) or not isinstance(extend[1],int): 
                raise spacetrackError( "InvalidArgument", 'extend must consist of two int' )

        #  Extract elements. 

        dtr = np.array( [ dt - gps0 for dt in timerange ] )
        datetimes_gps = np.array( [ dt - gps0 for dt in self.datetimes ] )
        ii = np.argwhere( np.logical_and( dtr[0] <= datetimes_gps, datetimes_gps < dtr[1] ) ).squeeze()

        #  Extend? 

        if extend is None: 
            imin = ii[0]
            imax = ii[-1]
        else: 
            imin = max( [ 0, ii[0]-extend[0] ] )
            imax = min( [ self.nrecs-1, ii[-1]+extend[1] ] )

        #  Extract TLEs. 

        ret = [ self.tles[i] for i in range(imin,imax+1) ]

        #  Done. 

        return ret


    def nearest( self, time ): 
        """Get the TLE that is nearest in time (instance of timestandards.Time). The two lines of the 
        TLE are returned as a 2-tuple if successful. If unsuccessful, None is returned."""

        ret = { 'status': None, 'messages': [], 'comments': [] }

        if not isinstance( time, Time ): 
            raise spacetrackError( "InvalidArgument", "The argument must be an instance of timestandards.Time" )

        #  Iterate to assure update of local TLE data (if necessary). 

        for iteration in range(2): 

            datetimes_gps = np.array( [ dt-gps0 for dt in self.datetimes ] )
            time_gps = time - gps0

            if datetimes_gps.size == 0: 
                ret_tles = None
                ret_spacetrack = self.spacetrack.download_data( self.norad_number, time.calendar("utc").datetime() )
                self.build_instance()
                continue

            i = np.argmin( np.abs( datetimes_gps - time_gps ) ) 

            #  Check for valid return value. The nearest time must be better than 12 hours. 

            if np.abs( datetimes_gps[i] - time_gps ) < 12 * 3600: 
                ret_tles = self.tles[i]
                break 
            elif iteration==0: 
                ret_tles = None
                ret_spacetrack = self.spacetrack.download_data( self.norad_number, time.calendar("utc").datetime() )
                ret['messages'] += ret_spacetrack['messages']
                ret['comments'] += ret_spacetrack['comments']
                self.build_instance()
            else: 
                ret_tles = None


        if ret_tles is None: 
            ret['status'] = "fail"
            ret['messages'].append( "NoOrbitData" )
            ret['comments'].append( "No orbit data available at Space-Track.org for " + \
                    time.calendar("utc").isoformat( timespec="seconds" ) )
        else: 
            ret['status'] = "success"
            ret.update( { 'data': ret_tles } )

        return ret



class Spacetrack(): 
    """This is a portal to handling TLE data obtained from Spacetrack."""

    def __init__( self ): 
        """Create a portal to access TLE downloaded from spacetrack.org. Data must be 
        strict two-line element files, with each file containing data for only one 
        satellite."""

        with open( defaults_file, 'r' ) as f: 
            defaults = json.load( f )
            self.data_root = defaults[root_path_variable]

        if not os.path.isdir( self.data_root ): 
            raise spacetrackError( "PathNotFound", f'"{root_path_variable} does not exist or is not a directory' )

        #  Establish an authenticated session. 

        myauth = { 'identity': defaults[username_variable], 'password': defaults[password_variable] }
        self.session = requests.Session()
        self.session.request( "post", "https://www.space-track.org/ajaxauth/login", data=myauth )

        #  Initiate the catalogue of TLE data. 

        self.catalogue = {}
        self.build_catalogue()


    def build_catalogue( self ): 
        """Build a catalogue of the TLE data currently available on the local 
        file system."""

        #  Scan directory hierarchy looking for data files. 

        for root, dirs, files in os.walk( self.data_root ): 

            files.sort()
            dirs.sort()

            for file in files: 
                m = re.search( r"^sat(\d{5}).*\.txt$", file )
                if not m: continue

                #  Read first line of the file to get the satellite NORAD number. 

                file_path = os.path.join( root, file )
                f = open( file_path, 'r' )
                line = f.readline()
                norad_number_string = line[2:7]
                f.close()

                #  Catalog the file. 

                if norad_number_string not in self.catalogue.keys(): 
                    self.catalogue.update( { norad_number_string: [] } )
                self.catalogue[norad_number_string].append( file_path )

            pass 

        #  Done. 

        pass

    def satellite( self, generic ): 
        """Create an instance of class SpacetrackSatellite given either a 
        NORAD catalogue number (int) or the name of the satellite (str) as 
        given in the table Satellites."""

        #  Define the NORAD catalogue number of the satellite. 
        
        norad_number = 0
        
        if isinstance( generic, int ): 
            norad_number = generic
        elif isinstance( generic, str ): 
            s = [ sat for sat in Satellites if sat['name'] == generic ]
            if len(s) == 1: 
                norad_number = s[0]['norad_number']
                
        #  Convert to string. 
        
        if norad_number != 0: 
            norad_number_string = f'{norad_number:05d}'
        else: 
            norad_number_string = ""
        
        #  Generate the Spacetrack satellite, load TLE data if available. 
        
        ret = SpacetrackSatellite( self, norad_number )

        return ret

    def download_data( self, norad_number, time ): 
        """Download TLEs for a satellite defined by its NORAD number 
        (norad_number) and a time. The default is to obtain one month's 
        worth of data. 

        norad_number - Either a string or an integer representing 
                the NORAD number of the satellite whose TLEs should 
                be obtained from SpaceTrack. 

        time - an instance of datetime.datetime for the time for which 
                TLE is desired. A full month's worth of data will be 
                obtained."""

        #  Initialize return structure. 

        ret = { 'status': None, 'messages': [], 'comments': [] }

        #  Check input. 

        if not isinstance(time,datetime): 
            ret['status'] = "fail"
            ret['messages'].append( "InvalidArgument" )
            ret['comments'].append( "The time must be an instance of datetime.datetime" )
            return ret

        if isinstance(norad_number,str): 
            satID = norad_number[-5:]
        elif isinstance(norad_number,int): 
            satID = f'{norad_number:05d}'
        else: 
            ret['status'] = "fail"
            ret['messages'].append( "InvalidArgument" )
            ret['comments'].append( "The time must be an instance of datetime.datetime" )
            return ret

        #  Define the first and last time of a month. 

        t0 = datetime( year=time.year, month=time.month, day=1 )
        tt = t0 + timedelta(days=31)
        t1 = datetime( year=tt.year, month=tt.month, day=1 )

        #  Post the request. 

        tf = "%Y-%m-%d"
        request_uri = "/".join( [ uri_base, download_command.format( satID, t0.strftime(tf), t1.strftime(tf) ) ] )
        resp = self.session.get( request_uri )

        if resp.status_code != 200: 
            ret['status'] = "fail"
            ret['messages'].append( "FailedDownload" )
            ret['comments'].append( f'failed request_uri = "{request_uri}"' )
            return ret

        #  Write to output file. 

        path = os.path.join( self.data_root, f'sat{satID}', f'sat{satID}_'+time.strftime("%Y-%m")+".txt" )
        os.makedirs( os.path.dirname(path), exist_ok=True )
        with open(path,'w') as f: 
            for line in resp.iter_lines(): 
                f.write( line.decode() + "\n" )

        #  Rebuild catalogue. 

        self.build_catalogue()

        #  Done. 

        ret['status'] = "success"

        return ret


if __name__ == "__main__": 

    data_dir = "../../Data/TLEs"
    cs = Spacetrack( data_dir )

    #  Select a satellite. 

    metopb = cs.satellite(43689)
    tle_data = metopb.select( ( datetime(2022,8,1), datetime(2022,9,1) ), extend=(1,1) )

    pass

