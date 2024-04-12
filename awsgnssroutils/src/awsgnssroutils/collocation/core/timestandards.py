#  Coordinate module for timekeeping and Earth reference frames.
#
#  Author: Stephen Leroy (sleroy@aer.com)
#  Version: 2.3.0
#  Date: 15 February 2024
#
#  This package contains two classes for use in converting between time standards:
#  Calendar and Time. The Calendar class uses calendar notation to define a time
#  that is not anchored in a time standard. The Time class categorizes a Calendar
#  instance by a specific time standard, GPS time, TAI time, or UTC time, and
#  performs all conversions. For example:
#
#  >>> calendar_time = Calendar( year=2019, month=12, day=18, hour=16, minute=36 )
#  >>> standard_time = Time( gps=calendar_time )
#
#  which defines a variable standard_time that corresponds to the date and time
#  prescribed in calendar_time as a GPS time. The standard_time also contains
#  the TAI time and UTC time corresponding the GPS time defined in calendar_time.
#
#  The "calendar" method of a Time instance produces a Calendar instance of any
#  of TAI time, GPS time, or UTC time, depending on the argument. For example,
#
#  >>> utc_calendar_time = standard_time.calendar( "utc" )
#  >>> tai_calendar_time = standard_time.calendar( "tai" )
#
#  The "gpstime" method of a Time instance produces a 5-element tuple giving
#  the GPS week, day of week, hour, minute, and second corresponding to the
#  Time instance.
#
#  >>> gps_week, day_of_week, hour, minute, second = standard_time.gpstime()
#

#  Revisions.
#  25 March 2020:
#    * Increase precision of time-keeping by redefining t1900 class by counting
#      seconds and fractional seconds separately.
#    * Update leap-second service get_leapseconddata by checking if the existing
#      file is beyond expiration.
#
#  14 April 2020:
#    * Time() returns an instance representing the current time if called
#      without arguments.
#
#  17 November 2021: 
#    * Class Calendar will be initialized by year, month, day, etc. by default. 
#    * Add datetime and isoformat methods to class Calendar. 
#    * Remove obstime method from class Time. 
#    * Fix bug in Calendar class that rendered an incorrect day-of-year 
#      computation. 
#
#  17 October 2023: 
#    * Update the site from where the leap second data are downloaded. The 
#      IERS site has been discontinued. The new table of leap seconds is 
#      updated to https://cdf.gsfc.nasa.gov/html/CDFLeapSeconds.txt. 
#
#  12 February 2024: 
#    * Fully functional Time.juliandate capability. Linked with astropy. 
#
#  13 February 2024: 
#    * Calendar.isoformat is a tunnel to datetime.datetime.isoformat. 
#
#  15 February 2024: 
#    * Allow the Time class to accept datetime.datetime and numpy.datetime64
#      objects to the tai, gps, and utc keywords at initialization. 
#

import numpy as np
import requests
import os
import re
import pathlib
import datetime as Datetime
from astropy.time import Time as astropyTime

#  Global parameters.

pi = np.pi
rads = pi/180

#  Exception handling.

class Error(Exception):
    pass


################################################################################
#                              Class Calendar
################################################################################

class CalendarError(Error):
    def __init__(self,expression,message):
        self.expression = expression
        self.message = message

class t1900Error(Error):
    def __init__(self,expression,message):
        self.expression = expression
        self.message = message

class t1900():
    def __init__(self,ix,fx=0.0):
        """Instance of class t1900, meant to count the number of (Gregorian) seconds elapsed
        since 1 Jan 1900 at 00:00 UTC without accounting for leap seconds. It splits the
        number of seconds into an integer part and a fractional part."""

        self._scale = 2**64

        if not isinstance(ix,int) and not isinstance(ix,np.int32) and not isinstance(ix,np.int64) :
            raise t1900Error( "InvalidArgument", "First argument must be an int." )
        self._integerSeconds = int( ix )

        if not isinstance(fx,float) and not isinstance(fx,np.float32) and not isinstance(fx,np.float64):
            raise t1900Error( "InvalidArgument", "Second argument must be a float." )
        self._fractionalSeconds = int( fx * self._scale + 0.5 )

    def seconds(self):
        """Return a two-tuple, the first element containing the integer number of
        seconds and the second element containing the fractional number of seconds
        as a float."""

        return self._integerSeconds, self._fractionalSeconds / self._scale

    def __int__(self):
        return self._integerSeconds

    def __eq__(self,x):
        return ( self._integerSeconds == x._integerSeconds and self._fractionalSeconds >= x._fractionalSeconds )

    def __gt__(self,x):
        return self._integerSeconds > x._integerSeconds \
            or ( self._integerSeconds == x._integerSeconds and self._fractionalSeconds > x._fractionalSeconds )

    def __ge__(self,x):
        return self._integerSeconds > x._integerSeconds \
            or ( self._integerSeconds == x._integerSeconds and self._fractionalSeconds >= x._fractionalSeconds )

    def __lt__(self,x):
        return self._integerSeconds < x._integerSeconds \
            or ( self._integerSeconds == x._integerSeconds and self._fractionalSeconds < x._fractionalSeconds )

    def __le__(self,x):
        return self._integerSeconds < x._integerSeconds \
            or ( self._integerSeconds == x._integerSeconds and self._fractionalSeconds <= x._fractionalSeconds )

    def __add__(self,x):
        if not isinstance( x, int ) and not isinstance( x, float ):
            raise t1900Error( "InvalidOperation", "Addition requires adding a t1900 to an int or float.." )
        ix, fx = int( x ), int( ( x - int(x) ) * self._scale )
        if fx + self._fractionalSeconds >= self._scale:
            return t1900( self._integerSeconds + ix + 1, ( self._fractionalSeconds + fx - self._scale )/self._scale )
        else:
            return t1900( self._integerSeconds + ix, ( self._fractionalSeconds + fx )/self._scale )

    def __sub__(self,x):
        if isinstance( x, int ) + isinstance( x, float ) + isinstance( x, t1900 ) != 1:
            raise t1900Error( "InvalidOperation", "Subtraction requires subtracting a float, int, " + \
                "or t1900 instance from a t1900 instance." )

        if isinstance( x, float ):
            ix, fx = int( x ), int( ( x - int(x) ) * self._scale )
            if self._fractionalSeconds >= fx:
                return t1900( self._integerSeconds - ix, ( self._fractionalSeconds - fx )/self._scale )
            else:
                return t1900( self._integerSeconds - ix - 1, ( self._fractionalSeconds - fx + self._scale )/self._scale )

        if isinstance( x, int ):
            return t1900( self._integerSeconds - x, self._fractionalSeconds/self._scale )

        if isinstance( x, t1900 ):
            if x._fractionalSeconds < self._fractionalSeconds:
                return ( self._integerSeconds - x._integerSeconds ) + \
                    ( self._fractionalSeconds - x._fractionalSeconds ) / self._scale
            else:
                return ( self._integerSeconds - x._integerSeconds - 1 ) + \
                    ( self._fractionalSeconds - x._fractionalSeconds + self._scale) / self._scale

#  Magic methods. 

    def __iadd__(self,x):
        if not isinstance( x, int ) and not isinstance( x, float ):
            raise t1900Error( "InvalidOperation", "Addition requires adding a t1900 to an int or float.." )
        ix, fx = int( x ), int( ( x - int(x) ) * self._scale )
        self._integerSeconds += ix
        self._fractionalSeconds += fx
        if self._fractionalSeconds > self._scale:
            self._integerSeconds += 1
            self._fractionalSeconds -= self._scale
        return self

    def __isub__(self,x):
        if not isinstance( x, int ) and not isinstance( x, float ):
            raise t1900Error( "InvalidOperation", "Addition requires adding a t1900 to an int or float.." )
        ix, fx = int( x ), int( ( x - int(x) ) * self._scale )
        self._integerSeconds -= ix
        self._fractionalSeconds -= fx
        if self._fractionalSeconds < 0:
            self._integerSeconds -= 1
            self._fractionalSeconds += self._scale
        return self


class Calendar():
    """This class provides methods for calendar operations, converting between calendar
    notation (year, month, day, etc.) and t1900 time, the latter of which counts
    seconds from 1 Jan 1900 at 00:00:00 (with 86400 seconds in every day). It also
    keeps track of the day-of-year and day-of-week."""

    def __init__(self, year=None, month=None, day=None, hour=None, minute=None, second=None, 
                 t1900t=None, system=None ):
        """Initialization takes either an instance of t1900 as a single argument or year,
        month, day in order to form a Calendar object. In the latter case, hour, minute
        and second can also be given optionally."""

        self.year = 0
        self.month = 0
        self.day = 0
        self.hour = 0
        self.minute = 0
        self.second = 0.0
        self.t1900 = None
        self.dow = 0			# 0 for Sunday, 1 for Monday, etc.
        self.doy = 0			# 1 for January 1, etc.
        self.datetimestring = ''
        self.datestring = ''

        if system is None: 
            self.system = None
        elif system.lower() in [ "utc", "tai", "gps" ]: 
            self.system = system.lower()
        else: 
            self.system = None

        #  Work with t1900.

        if t1900t is not None:

        #  Check the argument.

            if not isinstance( t1900t, t1900 ):
                raise CalendarError( "InvalidArguments", "Argument must be a t1900 class instance" )

            #  Check if keyword arguments were sent.

            if not ( year is None and month is None or day is None \
                or hour is None or minute is None or second is None ):
                raise CalendarError( "InvalidArguments", "Date specified multiple ways" )

            self.t1900 = t1900t
            self.get_date()

        else:

            if year is None or month is None or day is None:
                raise CalendarError( "InvalidArguments", "Must specify at least year, month, and day" )

            self.year = int( year )
            self.month = int( month )
            self.day = int( day )
            if hour is not None: self.hour = int(hour)
            if minute is not None: self.minute = int(minute)
            if second is not None: self.second = float(second)

            self.get_t1900()

        self.compose_strings()

    #  Compute t1900 for New Years Day given a year.

    def newyearsday_t1900(self,year):
        t = t1900( ( year*365 + int((year-1)/4) - int((year-1)/100) + int((year-1)/400) - 693960 ) * 86400 )
        return t

    #  Determine whether or not it is a leap year.

    def ndays(self,year):
        if ( ( year%4 == 0 ) and ( year%100 != 0 ) or ( year%400 == 0 ) ):
            return 366
        else:
            return 365

    #  Get the date from t1900

    def get_date(self):

        #  Determine year.

        res = self.t1900.seconds()[0]

        year = int( res / 365.0 / 86400.0 ) + 1900
        et1900 = self.newyearsday_t1900(year)
        while ( et1900 > self.t1900 ):
            year -= 1
            et1900 = self.newyearsday_t1900(year)
        res -= et1900.seconds()[0]
        self.year = year

        #  Determine day-of-year.

        self.doy = int( res / 86400.0 ) + 1

        #  Determine month.

        if ( self.ndays(year) == 365 ):
            days = [ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ]
        else:
            days = [ 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ]

        for month in range(12):
            if ( res < days[month] * 86400.0 ): break
            res -= days[month] * 86400.0
        self.month = month + 1

        #  Determine day, hour, minute, second, day-of-week.

        self.day = int( res / 86400.0 ) + 1
        res -= ( self.day - 1 ) * 86400.0
        self.hour = int( res / 3600.0 )
        res -= self.hour * 3600.0
        self.minute = int( res / 60.0 )
        res -= self.minute * 60.0
        self.second = int( res ) + self.t1900.seconds()[1]
        self.dow = ( int(self.t1900.seconds()[0]/86400.0) + 1 ) % 7		# Check this.

        #  Compose date strings.

    def compose_strings(self):
        months = 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()
        self.datetimestring = '{2:02d} {1} {0} {3:02d}:{4:02d}:{5:02d}'.format( self.year,
            months[self.month-1], self.day, self.hour, self.minute, int(self.second) )
        self.datestring = '{2:02d} {1} {0}'.format( self.year,
            months[self.month-1], self.day, self.hour, self.minute, self.second )

    #  Get t1900 from the date.

    def get_t1900(self):

        et1900 = self.newyearsday_t1900(self.year)
        self.t1900 = et1900 + 0
        if ( self.ndays(self.year) == 365 ):
            days = [ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ]
        else:
            days = [ 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ]
        for month in range(self.month-1): self.t1900 += days[month] * 86400
        self.t1900 += (self.day-1)*86400 + self.hour*3600 + self.minute*60
        self.t1900 += self.second
        self.doy = int( (self.t1900.seconds()[0] - et1900.seconds()[0])/86400 ) + 1
        self.dow = ( int(self.t1900.seconds()[0]/86400) + 1 ) % 7		# Check this.

    #  Output a datetime.datetime instance.

    def datetime( self ):
        """Output an instance of datetime.datetime."""

        microseconds = int( ( self.second - int(self.second) ) * 1.0e6 )
        dt = Datetime.datetime( year=self.year, month=self.month, day=self.day,
                hour=self.hour, minute=self.minute, second=int(self.second), 
                microsecond=microseconds )

        return dt

    #  Output an isotime instance. 

    def isoformat( self, **kwargs ): 
        """Output an string in ISO time format."""

        dt = self.datetime()
        ret = dt.isoformat( **kwargs )
        return ret

    #  Magic methods. 

    def __iadd__(self,x):
        self.t1900 += x
        self.get_date()
        self.compose_strings()
        return self

    def __isub__(self,x):
        self.t1900 -= x
        self.get_date()
        self.compose_strings()
        return self


################################################################################
#                              Class Time
################################################################################

class TimeError(Error):
    def __init__(self,expression,message):
        self.expression = expression
        self.message = message

class Time():
    """This class provide for conversions between TAI (atomic time), UTC, and GPS time.
    It is also helpful for generating GPS format time and Julian day."""

    def __init__(self,tai=None,gps=None,utc=None,leapsecondfile=None,system=None):
        """Create a time-conversion object. "leapsecondfile" should point toward a leap
        second list as obtained from https://www.ietf.org/timezones/data/leap-seconds.list.
        Its default behavior is to obtain the list by http request. "system" should be
        either 1 or 2, defining how UTC is computed from TAI. If system 1, then the time
        23:59:59 UTC is repeated immediately before the leap day. If system 2, then the
        time 23:59:60 is included and proceeds to 0:00:00 on the next day for a leap day
        included. The default is system 2.

        Only one of "tai", "gps", or "utc" should be given. The object given should be
        either an instance of class t1900 or an instance of class Calendar. In the case 
        of "gps", the value can be GPS seconds as a float or a floatable number."""

        self.tai = None
        self.utc = None
        self.gps = None

        #  Get leap second data.

        global __LEAPSECONDDATA__
        if __LEAPSECONDDATA__ is None : get_leapseconddata( leapsecondfile )
        self.leapseconddata = __LEAPSECONDDATA__

        #  Define GPS time origin.

        self.gps0 = t1900( 2524953600, 0.0 )

        #  Define the UTC leap second system.

        if system in {1,2}:
            self.system = system
        else:
            self.system = 2

        if tai is None and gps is None and utc is None:
            now = Datetime.datetime.now( tz=Datetime.timezone.utc )
            utc = Calendar( year=now.year, month=now.month, 
                    day=now.day, hour=now.hour, minute=now.minute, 
                    second=now.second+now.microsecond*1.0e-6 )

        #  Be sure that one of tai, gps, or utc is given.

        if int( tai is None ) + int( gps is None ) + int( utc is None ) != 2:
            raise TimeError( "InvalidArguments", "Must specify only one of tai, gps, or utc" )

        #  Convert tai to gps and utc time. UTC requires interpretation of leap seconds.

        if tai is not None:

            if isinstance(tai,t1900): 
                self.tai = tai
            elif isinstance(tai,Calendar): 
                self.tai = tai.t1900
            elif isinstance(tai,Datetime.datetime): 
                dt = tai
                cal = Calendar( year=dt.year, month=dt.month, day=dt.day, 
                               hour=dt.hour, minute=dt.minute, second=dt.second+dt.microsecond*1.0e-6 )
                self.tai = cal.t1900
            elif isinstance(tai,np.datetime64): 
                dt = Datetime.datetime.fromisoformat( str(tai) )
                cal = Calendar( year=dt.year, month=dt.month, day=dt.day, 
                               hour=dt.hour, minute=dt.minute, second=dt.second+dt.microsecond*1.0e-6 )
                self.tai = cal.t1900
            else: 
                raise TimeError( "InvalidArguments", "tai must be an instance of class t1900, " + \
                        "timestandards.Calendar, datetime.datetime, or numpy.datetime64" )

            self.gps = self.tai - 19.0
            for leapseconddatum in self.leapseconddata:
                self.utc = self.tai - leapseconddatum['leapseconds']
                if self.system==1 and self.utc >= leapseconddatum['t1900']-1: break
                if self.system==2 and self.utc >= leapseconddatum['t1900']: break

        #  Convert gps to tai and utc time. UTC requires interpretation of leap seconds.

        if gps is not None:

            if isinstance(gps,t1900): 
                self.gps = gps
            elif isinstance(gps,Calendar): 
                    self.gps = gps.t1900
            elif isinstance(gps,Datetime.datetime): 
                dt = gps
                cal = Calendar( year=dt.year, month=dt.month, day=dt.day, 
                               hour=dt.hour, minute=dt.minute, second=dt.second+dt.microsecond*1.0e-6 )
                self.gps = cal.t1900
            elif isinstance(gps,np.datetime64): 
                dt = Datetime.datetime.fromisoformat( str(gps) )
                cal = Calendar( year=dt.year, month=dt.month, day=dt.day, 
                               hour=dt.hour, minute=dt.minute, second=dt.second+dt.microsecond*1.0e-6 )
                self.gps = cal.t1900
            else: 
                try: 
                    self.gps = self.gps0 + float( gps )
                except: 
                    raise TimeError( "InvalidArguments", "gps must be an instance of class t1900, " + \
                        "timestandards.Calendar, datetime.datetime, numpy.datetime64, or a " + \
                        "floatable object." )

            self.tai = self.gps + 19.0
            for leapseconddatum in self.leapseconddata:
                self.utc = self.tai - leapseconddatum['leapseconds']
                if self.system==1 and self.utc >= leapseconddatum['t1900']-1: break
                if self.system==2 and self.utc >= leapseconddatum['t1900']: break

        #  Convert utc to gps and tai time. Because of leap seconds, at the instances of
        #  leap seconds, the conversions are ill defined.

        if utc is not None:
            if isinstance(utc,t1900): 
                self.utc = utc
            elif isinstance(utc,Calendar): 
                self.utc = utc.t1900
            elif isinstance(utc,Datetime.datetime): 
                dt = utc
                cal = Calendar( year=dt.year, month=dt.month, day=dt.day, 
                               hour=dt.hour, minute=dt.minute, second=dt.second+dt.microsecond*1.0e-6 )
                self.utc = cal.t1900
            elif isinstance(utc,np.datetime64): 
                dt = Datetime.datetime.fromisoformat( str(utc) )
                cal = Calendar( year=dt.year, month=dt.month, day=dt.day, 
                               hour=dt.hour, minute=dt.minute, second=dt.second+dt.microsecond*1.0e-6 )
                self.utc = cal.t1900
            else: 
                raise TimeError( "InvalidArguments", "utc must be an instance of class t1900, " + \
                        "timestandards.Calendar, datetime.datetime, or numpy.datetime64" )

            for leapseconddatum in self.leapseconddata:
                if self.utc >= leapseconddatum['t1900']:
                    self.tai = self.utc + leapseconddatum['leapseconds']
                    break
            if self.tai is None: self.tai = self.utc + self.leapseconddata[-1]['leapseconds'] - 1
            self.gps = self.tai - 19.0

    def juliandate(self):
        """Compute the Julian date."""

        #  Use astropy.Time. 

        atime = astropyTime( [self.calendar("utc").isoformat()], format='isot', scale='utc' )[0]
        jd = atime.jd

        return jd

    def gpsweekday(self):
        """Generate a two-tuple of integers. The first elemtn is the GPS week, the second
        element is the day-of-week."""

        gpsdays = int( ( self.gps - self.gps0 ) / 86400 )
        gpsweek = int( gpsdays / 7 )
        dow = gpsdays % 7

        return gpsweek, dow

    def calendar(self, arg):
        """Generate a Calendar instance of "utc", "tai", or "gps", which are the only valid
        values of arg."""

        args = { 'system': arg }
        if arg == 'utc': 
            args.update( { 't1900t': self.utc } )
        elif arg == 'tai': 
            args.update( { 't1900t': self.tai } )
        elif arg == 'gps': 
            args.update( { 't1900t': self.gps } )
        else: 
            raise TimeError( "InvalidArgument", "Argument must be either utc, tai or gps" )

        ret = Calendar( **args )

        return ret

    def gpstime(self):
        """Generate a tuple of ( gps_week, day_of_week, hour, minute, second ) corresponding
        to GPS time."""

        gps0 = self.gps0.seconds()[0]
        if self.gps.seconds()[0] >= gps0:
            gpsweek = int( ( self.gps.seconds()[0] - gps0 ) / ( 7 * 86400 ) )
            cal = self.calendar("gps")
            ret = ( gpsweek, cal.dow, cal.hour, cal.minute, cal.second )
        else:
            ret = None

        return ret

    #  Magic (operator) methods. The following magic methods are permitted:
    #
    #    ">", ">=", "<", "<=", all of which are based on comparisons of tai time.
    #
    #    "+", which can only add a float to a Time; the float having units of
    #        seconds.
    #
    #    "-", which takes the differences between two instances of Time and
    #        returns a float, which has units of seconds.
    #
    #    "+=", which functions as an increment operator, and can only take a
    #        float with units of seconds, returning another instance of Time.
    #
    #    "-=", which functions as a decrement operator, and con only take a
    #        float with units of seconds, returning another instance of Time.
    #

    def __eq__(self,x):
        return self.tai == x.tai

    def __gt__(self,x):
        return self.tai > x.tai

    def __ge__(self,x):
        return self.tai >= x.tai

    def __lt__(self,x):
        return self.tai < x.tai

    def __le__(self,x):
        return self.tai <= x.tai

    def __add__(self,x):
        new = Time( tai=self.tai+x, system=self.system )
        return new

    def __sub__(self,x):
        if isinstance(x,Time):
            new = self.tai - x.tai
        else:
            new = Time( tai=self.tai-x, system=self.system )
        return new

    def __iadd__(self,x):
        new = Time( tai=self.tai+x, system=self.system )
        return new

    def __isub__(self,x):
        new = Time( tai=self.tai-x, system=self.system )
        return new

    def __repr__(self): 
        x = f'<timestandard.Time object, "{self.calendar("utc").isoformat(timespec="seconds")} UTC">'
        return x


################################################################################
#  Service functions for leap second data.
################################################################################

class LeapSecondError(Error):
    def __init__(self,expression,message):
        self.expression = expression
        self.message = message


def download_leapsecondfile( leapsecondfile ):

    #  Download leap-second file.

    r = requests.get( "https://cdf.gsfc.nasa.gov/html/CDFLeapSeconds.txt" )
    if r.status_code != 200:
        raise LeapSecondError( "InternetFileNotFound", "Cannot retrieve leap second data over internet" )
    f = open( leapsecondfile, "w" )
    f.write( r.text )
    f.close()


def get_leapseconddata( leapsecondfile=None ):

    #  Get leap second data. It is either in file previously downloaded from
    #  NASA GSFC or it is retrieved from GSFC. 

    download = True

    #  Establish the leap-second filename.

    if leapsecondfile is None:
        if os.path.isdir( "/pub" ):
            leapsecondfile = "/pub/.gsfcleapseconds"
        else:
            leapsecondfile = str( pathlib.Path.home() ) + "/.gsfcleapseconds"

    #  Do we need to get a new leap-second file? Yes, if it doesn't exist in the
    #  given path, if it doesn't have an expiration date written in it, or if it
    #  past the expiration date.

    if os.path.isfile( leapsecondfile ):
        stat = os.stat( leapsecondfile )
        cdatetime = Datetime.datetime.fromtimestamp( stat.st_mtime, tz=Datetime.timezone.utc )
        now = Datetime.datetime.now( tz=Datetime.timezone.utc )
        ym_cdatetime = cdatetime.year + 0.5 * int( (cdatetime.month-1) / 6.0 )
        ym_now = now.year + 0.5 * int( (now.month-1) / 6.0 )
        download = ( ym_now > ym_cdatetime )

    #  Get the leap-second file if necessary.

    if download:
        download_leapsecondfile( leapsecondfile )

    f = open( leapsecondfile, "r" )
    alllines = f.readlines()
    f.close()

    #  Retain only the lines with useful leap-second data.

    lines = [ line.strip() for line in alllines if line != "" and not re.search("^;",line) ]

    #  Catalog leap second data. t1900 is the number of UTC seconds since 1 Jan 1900.

    leapseconddata = []
    for line in lines:
        strs = re.split( "\s+", line )[0:4]
        year, month, day, seconds = int(strs[0]), int(strs[1]), int(strs[2]), float( strs[3] )
        t1900i = Calendar( year=year, month=month, day=day ).t1900
        leapseconddata.append( { 't1900':t1900i, 'leapseconds':seconds } )
    leapseconddata.reverse()

    global __LEAPSECONDDATA__
    __LEAPSECONDDATA__ = leapseconddata


#  Initialize global variables.

global __LEAPSECONDDATA__
__LEAPSECONDDATA__ = None

