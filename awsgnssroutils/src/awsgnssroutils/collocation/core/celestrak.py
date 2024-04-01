"""Utilities for reading Celestrak TLE data. All TLE data must be 
obtained from celestrak.org as special requests for GP elements: 

    https://celestrak.org/NORAD/archives/request.php

Once the data are received by email, the files must be written into 
a path with data_root as the root. Data files can be arranged in 
subdirectories at the user's discretion, but the file names must 
satisfy the regular expression prescribed by 
"^sat{norad_number:09d}.*\.txt$" where norad_number is an integer 
that defines the NORAD catalogue number of the satellite whose TLE 
data are in the file. THE DATA FOR ONLY ONE SATELLITE IN EACH FILE, 
and these are definitely 2-line TLE files.

Example usage: 

    > from datetime import datetime
    > cs = Celestrak( "/home/user/Data/celestrak_tles" )
    > metopa = cs.satellite( 29499 )
    > tles = metopa.select( ( datetime(2009,8,1), datetime(2009,9,1) ), extend=(1,1) )

which retries all TLE for Metop-A for the month of August, 2009, including 
the last record for July, 2009, and the first for September, 2009. 
    """

#  Imports. 

import os, re, json, stat
import numpy as np
from .timestandards import Time, Calendar
from .constants_and_utils import defaults_file

#  Definitions. 

root_path_variable = "celestrak_data_root"
celestrak_time_convention = "utc"
gps0 = Time( gps=0 )

#  Table of satellites. 

Satellites = [ { 'name': "Metop-A", 'norad_number': 29499 }, 
              { 'name': "Metop-B", 'norad_number': 38771 }, 
              { 'name': "Metop-C", 'norad_number': 43689 }, 
              { 'name': "Suomi-NPP", 'norad_number': 37849 }, 
              { 'name': "JPSS-1", 'norad_number': 43013 }, 
              { 'name': "JPSS-2", 'norad_number': 54234 } ]


#  Exception handling. 

class Error( Exception ): 
    pass

class celestrakError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment


def setdefaults( root_path ):
    """This method sets the default root path for Celestrak TLE data."""

    if os.path.exists( defaults_file ):
        with open( defaults_file, 'r' ) as f:
            defaults = json.load( f )
        defaults.update( { root_path_variable: root_path } ) 
    else: 
        defaults = {}

    with open( defaults_file, 'w' ) as f:  
        json.dump( defaults, f, indent="  " ) 
    os.chmod( defaults_file, stat.S_IRUSR | stat.S_IWUSR )

    #  Done. 

    return





class CelestrakSatellite(): 
    """This class contains the TLE data, as obtained from Celestrak, for one 
    satellite."""

    def __init__( self, Celestrak_instance, norad_number ): 
        """Create an instance of class CelestrakSatellite. It will contain the 
        TLE data for a satellite identified by its NORAD catalogue number 
        (norad_number). The first argument is the instance of class Celestrak 
        from which this class is drawn."""

        if not isinstance( Celestrak_instance, Celestrak ): 
            raise celestrakError( "InvalidArgument", "First argument to " + \
                    "CelestrakSatellite not an instance of class Celestrak" )

        self.celestrak = Celestrak_instance
        norad_number_string = f'{norad_number:05d}'
        data = {}

        if norad_number_string not in self.celestrak.catalogue.keys(): 
            print( f'NORAD number {norad_number_string} not in Celestrak local database' )
            self.status = "fail"
            return

        for file in self.celestrak.catalogue[norad_number_string]: 

            f = open( file, 'r' )
            lines = f.readlines()
            f.close()

            if len(lines) % 2 == 1: 
                raise celestrakError( "InvalidData", f'file {file} has an odd number of lines' )

            for iline in range(0,len(lines),2): 

                tle_line1 = lines[iline]
                tle_line2 = lines[iline+1]

                if tle_line1[0] != "1": 
                    raise celestrakError( "InvalidData", 
                            f'Line {iline+1} in file {file} should be the first line of a TLE but is not' )

                if tle_line2[0] != "2": 
                    raise celestrakError( "InvalidData", 
                            f'Line {iline+2} in file {file} should be the second line of a TLE but is not' )

                #  Get the time of the record. 

                year = 2000 + int( tle_line1[18:20] )
                doy = int( tle_line1[20:23] )
                fday = float( tle_line1[23:32] )

                time = Time( **{ celestrak_time_convention: Calendar(year,1,1) } ) + 86400 * ( doy + fday - 1 )
                rec = { 'time': time, 'tle': ( tle_line1, tle_line2 ) }
                data.update( { time.calendar(celestrak_time_convention).isoformat(timespec="seconds"): rec } )

        #  Sort the records. 

        sorted_key_list = sorted( list( data.keys() ) )

        #  Create data contents. 

        self.nrecs = len( sorted_key_list )
        self.datetimes = [ data[key]['time'] for key in sorted_key_list ]
        self.tles = [ data[key]['tle'] for key in sorted_key_list ]

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

        data = CelestrakSatellite.select( 
                timerange = [ timestandards.Time( utc=timestandards.Calendar(2023,6,7) ), 
                                timestandards.Time( utc=timestandards.Calendar(2023,6,8) ) ], 
                extend = [0,1] )

        The statement generates a list of TLEs for the satellite defined by CelestrakSatllite
        for the day of June 7, 2023, and appends to the list the first TLE for the succeeding 
        day."""

        if not isinstance(timerange,list) and not isinstance(timerange,tuple): 
            raise celestrakError( "InvalidArgument", 'timerange must be a tuple or list' )

        if len( timerange ) != 2: 
            raise celestrakError( "InvalidArgument", 'timerange must containing two elements' )

        if not isinstance(timerange[0],Time) or not isinstance(timerange[1],Time): 
            raise celestrakError( "InvalidArgument", 'timerange must consist of timestandards.Time instances' )

        if extend is not None: 

            if not isinstance(extend,list) and not isinstance(extend,tuple): 
                raise celestrakError( "InvalidArgument", 'extend must be a tuple or list' )

            if len( extend ) != 2: 
                raise celestrakError( "InvalidArgument", 'extend must containing two elements' )

            if not isinstance(extend[0],int) or not isinstance(extend[1],int): 
                raise celestrakError( "InvalidArgument", 'extend must consist of two int' )

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
            ret['messages'].append( "InvalidArgument" )
            ret['comments'].append( f'The argument to Celestrak.nearest must be an instance of timestandards.Time' )
            ret['status'] = "fail"
            return ret

        datetimes_gps = np.array( [ dt-gps0 for dt in self.datetimes ] )
        time_gps = time - gps0

        i = np.argmin( np.abs( datetimes_gps - time_gps ) ) 

        #  Check for valid return value. The nearest time must be better than 12 hours. 

        if np.abs( datetimes_gps[i] - time_gps ) < 12 * 3600: 
            ret.update( { 'data': self.tles[i] } )
            ret['status'] = "success"
        else: 
            ret['status'] = "fail"

        return ret



class Celestrak(): 
    """This is a portal to handling TLE data obtained from Celestrak."""

    def __init__( self ): 
        """Create a portal to access TLE downloaded from celestrak.org. Data must be 
        strict two-line element files, with each file containing data for only one 
        satellite."""

        with open( defaults_file, 'r' ) as f: 
            defaults = json.load( f )
            self.data_root = defaults[root_path_variable]

        if not os.path.isdir( self.data_root ): 
            raise celestrakError( "PathNotFound", f'"{root_path_variable} does not exist or is not a directory' )

        #  Scan directory hierarchy looking for data files. 

        self.catalogue = {}

        for root, dirs, files in os.walk( self.data_root ): 

            files.sort()
            dirs.sort()

            for file in files: 
                m = re.search( "^sat(\d{9}).*\.txt$", file )
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
        """Create an instance of class CelestrakSatellite given either a 
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
        
        #  Generate the Celestrak satellite, load TLE data. 
        
        if norad_number_string not in self.catalogue.keys(): 
            ret = None
        else: 
            ret = CelestrakSatellite( self, norad_number )

        return ret



if __name__ == "__main__": 

    data_dir = "../../Data/TLEs"
    cs = Celestrak( data_dir )

    #  Select a satellite. 

    metopb = cs.satellite(43689)
    tle_data = metopb.select( ( datetime(2022,8,1), datetime(2022,9,1) ), extend=(1,1) )

    pass

