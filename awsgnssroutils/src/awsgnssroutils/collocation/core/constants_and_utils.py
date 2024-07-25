"""constants_and_utils.py

Author: Alex Meredith (ameredit@mit.edu), Stephen Leroy (sleroy@aer.com)
Version: 
Date: March 13, 2024

This module contains utilities useful for analyzing the geometry of 
nadir scanner soundings and radiation. It also contains attributes 
regarding the Earth's mass, shape, and spin."""


###  Imports #############################

import os
import numpy as np
import xarray as xr
import time
import erfa
import bisect
import netCDF4

### Earth-related constants ##############

Earth_equatorial_radius = 6378137.0/1000 #km
Earth_polar_radius = 6356752.3142/1000 #km
km_to_degree = 360/(2*np.pi*Earth_polar_radius)#degree/km
mu = 3.986004418e5 #standard gravitational parameter of Earth [km^3/s^2]
sec_to_sidereal_day = 1/86164.0905 #sidereal day/sec

### Physical constants ###################

planck_constant = 6.62607015e-34   # J/Hz
boltzmann_constant = 1.380649e-23  # J/K
speed_of_light = 2.99792458e8      # m/s

###  Parameters ##########################

HOME = os.path.expanduser( "~" )
defaults_file = os.path.join( HOME, ".collocation" )


### Utility functions ####################

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


def masked_dataarray( data, fill_value, attrs={}, **kwargs ): 
    """Create a masked version of an xarray.DataArray. If the input data is a numpy masked array, 
    then the masked slots are filled with fill_value and the fill value is written into the 
    DataArray attributes as _FillValue. An xarray.DataArray is returned."""

    #  Check input. 

    numpy_scalar = False

    for stype in [ np.int8, np.int16, np.int32, np.int64, np.float32, np.float64 ]: 
        if isinstance( data, stype ): 
            numpy_scalar = True
            break 

    if isinstance( data, np.ndarray ) or numpy_scalar: 

        ret = xr.DataArray( data, attrs=attrs, **kwargs )

    elif isinstance( data, np.ma.core.MaskedArray ): 

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

    return ret


def planck_blackbody( frequency, temperature ): 
    """Planck blackbody radiation. The arguments can be scalars or 
    numpy.ndarray's. Frequency is Hz, temperature in K. Output 
    units are W m**-2 Hz**-1 steradian**-1."""

    ret = 2 * planck_constant * frequency**3 / speed_of_light**2 / \
            ( np.exp( ( planck_constant * frequency ) / ( boltzmann_constant * temperature ) ) - 1 )

    return ret


def inverse_planck_blackbody( frequency, radiance ): 
    """Inverse of Planck blackbody radiation. The input arguments 
    can be scalars or numpy.ndarray's. Frequency is Hz, radiance 
    is W m**-2 Hz**-1 steradian**-1. Brightness temperature [K] 
    is output."""

    z1 = np.log( planck_constant ) + np.log( frequency ) - np.log( boltzmann_constant )
    z2 = np.log( planck_constant ) + 3 * np.log( frequency ) - np.log( radiance ) - 2 * np.log( speed_of_light )

    # ret = planck_constant * frequency / boltzmann_constant / \
    #         np.log( 1 + 2 * ( planck_constant * frequency**3 ) / ( radiance * speed_of_light**2 ) )

    ret = np.exp( z1 ) / np.log( 1 + 2 * np.exp( z2 ) )


    return ret

