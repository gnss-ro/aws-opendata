# Module: gravitymodel.py
# Version: 1.0
# Author: Stephen Leroy (sleroy@aer.com)
# Date: February 7, 2020

# Implementation of gravity model JGM3_OSU91a. It computes geopotential 
# energy per unit mass, including the centrifugal potential, given geodetic
# longitudes, latitudes, and altitudes. 

import numpy as np
from scipy.special import lpmv, factorial
from Utilities.jgm3_osu91a import *

rad = np.pi / 180


def geopotential( longitudes, latitudes, altitudes, \
    equatorialradius=6378.1363, polarradius=6356.7516, ndegrees=30, \
    geoidref=False ): 
    """
Compute the geopotential energy per unit mass at positions defined by 
longitudes, latitudes and altitudes [km]. Each must be either a scalar
or a numpy ndarray. If more than one is an array, then the arrays must 
have matching dimensions. 

equatorialradius and polarradius are the equatorial and polar radii
of the Earth, in km. 

ndegrees specifies the maximum degree of the spherical harmonic 
expansion of the Earth's gravity field. If it is None, than all terms
are used. 

Geopotential energy per unit mass [J/kg] is output. 

Set geoidref to True if altitudes are given with respect to the geoid
rather than the best fit ellipsoid. 
    """

#  Compute the undulation. The undulation is the altitude of the mean 
#  sea level geoid above the meann sea level ellipsoid. The "alts" upon 
#  which the gravity model below depends must be altitude above the 
#  mean sea level ellipsoid, and so the undulation must be added to 
#  altitudes if the altitudes are given with respect to the geoid. 

    if geoidref: 
        undulation = 0.0
        dundulation = 1.e20
        while np.abs( dundulation ).max() > 0.001: 
            dundulation = geopotential( longitudes, latitudes, undulation, ndegrees=ndegrees ) / gravity / 1.0e3
            undulation = undulation - dundulation
        alts = altitudes + undulation
    else:
        alts = altitudes

#  Geocentric latitude at local Earth's surface. 

    phic = np.arctan( ( equatorialradius / polarradius )**2 \
        * np.tan( latitudes * rad ) ) / rad

#  Radius of Earth at local Earth's surface. 

    Rsurface = np.sqrt( 1.0 / ( ( np.cos(phic*rad) / equatorialradius )**2 \
        + ( np.sin(phic*rad) / polarradius )**2 ) )

#  Earth-fixed coordinates. 

    x = np.cos(longitudes*rad) * ( Rsurface * np.cos(phic*rad) + alts * np.cos(latitudes*rad) )
    y = np.sin(longitudes*rad) * ( Rsurface * np.cos(phic*rad) + alts * np.cos(latitudes*rad) )
    z = Rsurface * np.sin(phic*rad) + alts * np.sin(latitudes*rad)

    r = np.sqrt( x*x + y*y + z*z )
    sinlats = z / r
    coslats = np.sqrt( 1 - sinlats**2 )
    rho = Rreference / r

#  Initialize loop over zonal harmonic. We perform the loop over 
#  the zonal harmonic first in order to take advantage of a 
#  recurrence relation for associated Legendre polynomials. 

    coslons = np.cos( longitudes * rad )
    sinlons = np.sin( longitudes * rad )
    cosmlons = 1.0e0
    sinmlons = 0.0e0
    expansion = 0.0e0

#  Perform the expansion. 

    if ndegrees is not None: 
        n = np.min( [ ndegrees, np.max( gravity_degree ) ] )
    else:
        n = np.max( gravity_degree )

    for m in range(0,n+1): 

        l = m

        while l <= n: 

#  Perform the expansion. 

            if m == 0: 
                norm = np.sqrt( 2*l + 1 )
            else: 
                norm = np.sqrt( 2 * (2*l+1) ) / np.exp( 0.5 * np.log( np.arange(l-m+1,l+m+1) ).sum() )

            icoeffs = np.argwhere( np.logical_and( gravity_order == m, gravity_degree == l ) )
            if icoeffs.size == 1: 
                i = icoeffs[0,0]
                plm = lpmv( m, l, sinlats )
                ylm = plm * norm
                expansion += ( cosmlons * gravity_cosineCoeff[i] + sinmlons * gravity_sineCoeff[i] ) * rho**l * ylm
#               print( "m={}, l={}, norm2={}, expansion={}".format( m, l, norm2, expansion ) )

#  Next degree; update the normalization. 

            l += 1

#  Next order. 

        cosmlons, sinmlons = cosmlons*coslons - sinmlons*sinlons, sinmlons*coslons + cosmlons*sinlons

#  Centrifugal potential. 

    geop = -( 1 + expansion ) * GM / ( 1.0e3 * r ) - 0.5 * Omega**2 * (x*x + y*y) * 1.0e6
    geop -= msl_geopotential

    return geop


if __name__ == "__main__": 
    from IPython.core.debugger import Tracer
    Tracer()()
    phi = geopotential( 33, -40, 3.422 )

