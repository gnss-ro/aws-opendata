"""
The `constants_and_utils` module contains useful constants relating 
to Earth and conversions between different time standards. It also 
contains utility functions related to these conversions and properties.

Last updated: 9/8/2023 by Alex Meredith
"""

import os
import numpy as np
import xarray as xr
import time
import erfa
import bisect
import netCDF4

### Earth-related constants #############

Earth_equatorial_radius = 6378137.0/1000 #km
Earth_polar_radius = 6356752.3142/1000 #km
km_to_degree = 360/(2*np.pi*Earth_polar_radius)#degree/km
mu = 3.986004418e5 #standard gravitational parameter of Earth [km^3/s^2]
sec_to_sidereal_day = 1/86164.0905 #sidereal day/sec

###  Physical constants ##################

planck_constant = 6.62607015e-34   # J/Hz
boltzmann_constant = 1.380649e-23  # J/K
speed_of_light = 2.99792458e8      # m/s


##########################################

### Simple time constants ###############

hours_to_sec = 3600 #sec/hr
sec_to_hours = 1/hours_to_sec #hr/sec
min_to_sec = 60 #sec/min
microsec_to_sec = 1e-6 #sec/microsec
day_to_sec = 24*hours_to_sec #sec/day

###########################################

### Time conversions #####################

utc_to_tai = 37 #UTC lags TAI because it has leap seconds and TAI doesn't [seconds]
tai_to_utc = -utc_to_tai #seconds
j2000_epoch_to_unix = 946684800 #offset between start of Unix time (1/1/1970) and start of J2000 time (1/1/2000) [seconds]
gps_epoch_to_unix = 315964800 #offset between start of Unix time (1/1/1970) and start of GPS time (6/1/1980) [seconds]
tai_epoch_to_unix = -378691200 #offset between start of Unix time (1/1/1970) and start of TAI epoch (1/1/1958) [seconds]
y2021_epoch_to_unix = 1609459200 #offset between start of Unix time (1/1/1970) and start of 2021

###########################################

#  Parameters. 

HOME = os.path.expanduser( "~" )
defaults_file = os.path.join( HOME, ".collocation" )

## Utility functions #######################


def calculate_km_to_degree(lat_geodetic):
    """
    This function calculates the latitude-dependent conversion factor
    between km on the Earth's surface and degrees swept out from Earth's
    center.

    Parameters
    ------------
        lat_geodetic: float
            Geodetic latitude[radians]

    Returns
    --------
        km_to_degree: float
            Latitude-adjusted conversion factor between km and degrees
    """
    r_E = calculate_radius_of_earth(lat_geodetic)
    return np.rad2deg(1/r_E)


def calculate_radius_of_earth(lat_geodetic):
    """
    This function calculates the latitude-dependent radius of the Earth.

    Parameters
    ------------
        lat_geodetic: float
            Geodetic latitude[radians]

    Returns
    --------
        r_e: float
            Latitude-adjusted radius of Earth, in km
    """
    a = Earth_equatorial_radius
    b = Earth_polar_radius
    r_e = np.sqrt(((a**2*np.cos(lat_geodetic))**2 +
                   (b**2*np.sin(lat_geodetic))**2) /
                  ((a*np.cos(lat_geodetic))**2 +
                   (b*np.sin(lat_geodetic))**2))
    return r_e


def get_unique_soundings(soundings_1, soundings_2, eps=1e-5):
    """
    This function eliminates "rounding errors" from lists of RO soundings.
    Basically, if a sounding exists in both lists with the same time and
    lat/lon differences are within floating point precision, it considers
    this to be the same sounding and removes it from both lists.

    This is used when identifying false positives and false negatives, due
    to an issue with the original version of the rotation-collocation method
    where some collocations were identified with slightly different lat/lon
    from the brute-force method output. It is unclear if this is needed
    anymore.

    Parameters
    -------------
        soundings_1: list
            list of length-3 lists [lat, lon, time] corresponding to
            occultations
        soundings_2: list
            list of length-3 lists [lat, lon, time] corresponding to
            occultations

    Returns
    ---------
        soundings_1_unique: list
            list of length-3 lists [lat, lon, time] corresponding to
            soundings_1, with all soundings that also appear in soundings_2
            removed
        soundings_2_unique: list
            list of length-3 lists [lat, lon, time] corresponding to
            soundings_2, with all soundings that also appear in soundings_1
            removed
    """
    # No overlap if at least one of the lists has zero elements
    if len(soundings_1) == 0 or len(soundings_2) == 0:
        return soundings_1, soundings_2

    # Otherwise look for duplicates
    soundings_1_duplicate = []
    soundings_2_duplicate = []
    for sounding_1 in soundings_1:
        occ_time, lat, lon = sounding_1[2], sounding_1[0], sounding_1[1]
        for sounding_2 in soundings_2:
            occ_time_2 = sounding_2[2]
            lat_2, lon_2 = sounding_2[0], sounding_2[1]
            if np.abs(occ_time_2-occ_time) < eps:
                if np.abs(lat_2-lat) < eps and np.abs(lon_2-lon) < eps:
                    soundings_1_duplicate.append(sounding_1)
                    soundings_2_duplicate.append(sounding_2)
    soundings_1_unique = [sounding for sounding in soundings_1
                          if sounding not in soundings_1_duplicate]
    soundings_2_unique = [sounding for sounding in soundings_2
                          if sounding not in soundings_2_duplicate]
    return soundings_1_unique, soundings_2_unique


def get_false_pos_false_neg(rot_arr, bf_arr):
    """
    This function finds false positives/incorrect predictions and
    false negatives/missed predictions found by the rotation-collocation
    method.

    Arguments
    ------------
        rot_arr: list
            list of length-3 lists [lat, lon, time] corresponding to
            collocations found by the rotation-collocation method
        bf_arr: list
            list of length-3 lists [lat, lon, time] corresponding to
            collocations found by the brute-force method, to use
            as a truth metric

    Returns
    ---------
        rot_false_pos: list
            list of length-3 lists [lat, lon, time] corresponding to
            collocations found by the rotation-collocation method but
            NOT by the brute-force method
        rot_false_neg: list
            list of length-3 lists [lat, lon, time] corresponding to
            collocations found by the brute-force method but NOT by
            the rotation-collocation method
    """
    rot_false_pos = []
    rot_false_neg = []

    for line in rot_arr:
        if line not in bf_arr:
            rot_false_pos.append(line)

    for line in bf_arr:
        if line not in rot_arr:
            rot_false_neg.append(line)
    rot_false_pos, rot_false_neg = get_unique_soundings(rot_false_pos,
                                                        rot_false_neg)
    return rot_false_pos, rot_false_neg


def get_distance(lon_1, lat_1, lon_2, lat_2):
    """
    This function gets the distance between two lat/lon points assumed
    to be on the Earth's surface.

    Arguments
    -----------
        lon_1: float
            Longitude of first point, in radians
        lat_1: float
            Latitude of first point, in radians
        lon_2: float
            Longitude of second point, in radians
        lat_2: float
            Latitude of second point, in radians

    Returns
    ---------
        ang: float
            Distance in terms of angle between vectors from Earth's
            surface to each point, in radians
        dist: float
            Distance, in km
    """
    # Convert lat/lon to 3D vector
    vec_1 = np.array([np.cos(lon_1)*np.cos(lat_1),
                      np.sin(lon_1)*np.cos(lat_1),
                      np.sin(lat_1)])
    vec_2 = np.array([np.cos(lon_2)*np.cos(lat_2),
                      np.sin(lon_2)*np.cos(lat_2),
                      np.sin(lat_2)])

    # Get angle between vectors and convert to distance
    ang = np.arccos(np.dot(vec_1, vec_2))

    # Radius of Earth is latitude-dependent, approximate as avg between lats
    r_e = calculate_radius_of_earth((lat_1+lat_2)/2.0)

    return ang, r_e*ang


def get_closest_nadir_sounding(nadir_times_sorted, nadir_data, ro_time, ro_lat,
                               ro_lon, time_tolerance):
    """
    This function gets the nadir-scanner sounding closest to an RO
    sounding (or other point with associated lat/lon/time, given
    a sorted list of nadir-scanner soundings.

    This is for debugging/plotting and isn't used in the brute-force
    or rotation-collocation methods.

    Arguments
    -----------
        nadir_times_sorted: np.array
            Sorted array-like of nadir-scanner sounding times, with
            times in hours since the Unix epoch
        nadir_data: np.array
            Sorted array-like of nadir scanner data, where nadir_data[i]
            contains [lons, lats, time] for the ith cross-track scan
        ro_time: float
            RO sounding time in hours since the Unix epoch
        ro_lat: float
            RO sounding latitude in radians
        ro_lon: float
            RO sounding longitude in radians
        time_tolerance: int
            Time tolerance in seconds

    Returns
    ----------
        min_distance: float
            Distance between RO sounding and closest nadir sounding, in km
        closest_nadir_lat: float
            Latitude of closest nadir scanner sounding, in radians
        closest_nadir_lon: float
            Longitude of closest nadir scanner sounding, in radians
        closest_nadir_time: float
            Time of closest nadir scanner sounding, in hours since Unix epoch
    """
    min_distance = np.inf
    closest_nadir_lat, closest_nadir_lon, closest_nadir_time = -1, -1, -1
    start_ind = bisect.bisect_left(nadir_times_sorted,
                                   ro_time-time_tolerance*sec_to_hours)
    end_ind = bisect.bisect_right(nadir_times_sorted,
                                  ro_time+time_tolerance*sec_to_hours)

    for i in range(start_ind, end_ind):
        lats, mid_time = nadir_data[i][1], nadir_data[i][2]
        for j in range(len(lats)):
            nadir_lat, nadir_lon = lats[j], nadir_data[i][0][j]
            distance = get_distance(nadir_lon, nadir_lat, ro_lon, ro_lat)
            if distance < min_distance:
                min_distance = distance
                closest_nadir_lat = nadir_lat
                closest_nadir_lon = nadir_lon
                closest_nadir_time = mid_time

        return min_distance, closest_nadir_lat, closest_nadir_lon, closest_nadir_time


def constrain_angle_pair(ang1, ang2, num_full_revs=0):
    """
    Constrains angles such that -2pi < ang1 < 0 and ang1 <
    ang2 and such that |ang2 - ang1| < 2pi*(num_full_revs+1).

    Arguments
    -----------
        ang1: float
            angle in radians
        ang2: float
            angle in radians
        num_full_revs: int (optional)
            number of full revolutions between ang1 and ang2
            (e.g. if 0.75 revolutions between angles, num_full_revs=0)
    Returns
    --------
        ang1: float
            angle in radians constrained to be between -2pi and 0.
        ang2: float
            angle in radians constrained to be between ang1 and
            ang1 + 2pi * (num_full_revs + 1).
    """
    while ang1 < -2*np.pi:
        ang1 = ang1+2*np.pi
    while ang1 > 0:
        ang1 = ang1-2*np.pi
    while ang2 < ang1+2*np.pi*num_full_revs-np.pi:
        ang2 = ang2+2*np.pi
    while ang2 > ang1+2*np.pi*(num_full_revs+1)-np.pi:
        ang2 = ang2-2*np.pi
    return ang1, ang2


def constrain_to_pi_range(ang):
    """
    Constrains an angle (in radians) to between -pi and +pi.

    Parameters
    -------------
        ang: float
            angle in radians
    Returns
    ---------
        ang: float
            angle in radians constrained to be between -pi and pi.
    """
    while ang < -np.pi:
        ang = ang+2*np.pi
    while ang > np.pi:
        ang = ang-2*np.pi
    return ang


def get_data_in_time_window(nadir_sat, ro_time, time_tolerance):
    """
    This function pulls a chunk of nadir-scanner data surrounding 
    a particular time.

    Arguments
    ----------
        nadir_sat: NadirSatelliteInstrument
            Nadir-scanner satellite object
        ro_time: int
            time of interest to pull data for
        time_tolerance: int
            time tol for pulling data

    Returns
    --------
        lats: list
            list of floats, nadir sounding lats in time range
        lons: list
            list of floats, nadir sounding lons in time range
        times: list
            list of times, nadir sounding times in time range
    """
    nadir_data, nadir_times_sorted = nadir_sat.load_sorted_data()
    start_ind = bisect.bisect_left(nadir_times_sorted,
                                   ro_time-((time_tolerance)*sec_to_hours))
    end_ind = bisect.bisect_right(nadir_times_sorted,
                                  ro_time+((time_tolerance)*sec_to_hours))
    lons, lats, times = [], [], []
    for i in range(start_ind, end_ind):
        mid_time = nadir_data[i][2]
        lon, lat = nadir_data[i][0], nadir_data[i][1]
        for j in range(len(lon)):
            lons.append(lon[j])
            lats.append(lat[j])
            times.append(mid_time)
    return lats, lons, times


def unix_time_to_jd(unix_time):
    """
    This function converts a time in seconds since the Unix epoch
    (like TAI time, Unix time ignores leap seconds) to Julian date
    (using UTC for Julian date, so we add leap seconds when
    converting).

    Arguments
    ----------
        unix_time: float
            Time since Unix epoch, in seconds

    Returns
    --------
        jd: float
            Julian date
    """
    utc_time = time.gmtime(unix_time+tai_to_utc)
    jd_day, jd_frac = erfa.dtf2d("UTC",
                                 utc_time.tm_year,
                                 utc_time.tm_mon,
                                 utc_time.tm_mday,
                                 utc_time.tm_hour,
                                 utc_time.tm_min,
                                 utc_time.tm_sec)
    jd = jd_day + jd_frac

    return jd





def write_dataset_to_netcdf( dataset, nc ): 
    """Write an xarray Dataset (dataset) to an open NetCDF file or group (nc)."""

    #  Check input. 

    if not isinstance( dataset, xr.Dataset ): 
        raise collocationError( "InvalidArgument", "First argument must be an instance of xarray.Dataset" )

    if not isinstance( nc, netCDF4.Dataset ): 
        raise collocationError( "InvalidArgument", "Second argument must be an instance or child of numpy.Dataset" )

    #  Create dimensions. 

    for name, size in dataset.sizes.items(): 
        nc.createDimension( name, size )

    #  Create variables and their attributes. 

    variables = {}
    for vname, vobj in dataset.variables.items(): 
        v = nc.createVariable( vname, vobj.dtype, vobj.dims )
        v.setncatts( vobj.attrs )
        variables.update( { vname: v } )

    #  Create global attributes. 

    nc.setncatts( dataset.attrs )

    #  Write data values. 

    for vname, vobj in variables.items(): 
        vobj[:] = dataset.variables[vname].values

    #  Done. 

    return


def masked_dataarray( data, fill_value, attrs={}, **kwargs ): 
    """Create a masked version of an xarray.DataArray. If the input data is a numpy masked array, 
    then the masked slots are filled with fill_value and the fill value is written into the 
    DataArray attributes as _FillValue. An xarray.DataArray is returned."""

    #  Check input. 

    if isinstance( data, np.ma.core.MaskedArray ): 

        xdata = data.data
        xfill = data.dtype.type( fill_value )

        if len( xdata.shape ) == 0: 
            if data.mask: 
                xdata = xfill
        else: 
            i = np.argwhere( data.mask ).flatten()
            xdata[i] = xfill

        attrs.update( { '_FillValue': xfill } )
        ret = xr.DataArray( xdata, attrs=attrs, **kwargs )

    elif isinstance( data, np.ndarray ): 

        ret = xr.DataArray( data, attrs=attrs, **kwargs )

    return ret


def planck_blackbody( frequency, temperature ): 
    """Planck blackbody radiation. The arguments can be scalars or 
    numpy.ndarray's. Frequency is Hz, temperature in K. Output 
    units are W m**-2 Hz**-1 steradian**-1."""

    ret = 2 * planck_constant * frequency**3 / speed_of_light**2 / \
            ( np.exp( ( planck_constant * frequency ) / ( boltzmann_constant * temperature ) ) - 1 )

    return ret

