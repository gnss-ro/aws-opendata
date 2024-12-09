"""Various useful library utilities. The useful tools are 

class LagrangePolynomialInterpolate
==================================================
Create a callable function that does Lagrange polynomial interpolation. 

function screen
==================================================
Useful for screening input data; it returns indices of valid data in an array. 

function cartesian
==================================================
Converts an astropy SkyCoord object to a numpy ndarray for a Cartesian 
representation of the unit vector direction.

function normalize
==================================================
Return a normalized version of the vector input.

function transformcoordinates
==================================================
Transform between ECF and ECI coordinates. 

function tangentpoint_radii
==================================================
Calculate tangent point radii for straight line between transmitter and 
receiver satellites. 
"""

#  Imports. 

import os
import numpy as np
from astropy.coordinates import SkyCoord
from .TimeStandards import Calendar, Time

#  Exception handling. 

class Error( Exception ): 
    pass

class LagrangePolynomialInterpolateError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment

class transformcoordinatesError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment

#  Logger. 

import logging
LOGGER = logging.getLogger(__name__)


################################################################################
#  s3fs authentication using environment tokens. 
################################################################################

def s3fsauth(): 
    """Return a dictionary of AWS authentication tokens that exist in the 
    environment. Tokens are added only if they are found in the environment. 
    Otherwise, a null dictionary will be returned."""

    key = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    token = os.getenv("AWS_SESSION_TOKEN")

    ret = {}

    if key != "": 
        ret.update( { 'key': key } )
    if secret != "": 
        ret.update( { 'secret': secret } )
    if token != "": 
        ret.update( { 'token': token } )

    return ret


################################################################################
#  Lagrange polynomial interpolation. 
################################################################################

class LagrangePolynomialInterpolate():
    """
This class creates a Lagrange polynomial interpolator of data of arbitrary 
dimension. Once initiated, it returns a function that returns arrays of 
values at the values of independent coordinates passed into the function. 
For example: 

>>> f = LagrangePolynomialInterpolate( inputTimes, inputValues )
>>> outputValues = f( outputTimes )

If inputTimes has shape (n,), then inputValues must have shape (k,n)
or (n,). If outputTimes has shape (m,), then outputValues has shape (k,m) 
in the former case or (m,) in the latter case. 
"""

    def __init__( self, independentCoordinate, dependentValues ):
        """
Create an object that can be used to interpolate dependentValues at arbitrary 
values of the independentCoordinate. The independentCoordinate should be an 
array of values of the independent coordinate, typically time, of dimension 
n. The dependentValues are the values to be interpolated, and it can be a 
one-dimensional array of shape (n,) or a two-dimensional array of shape (k,n)."""

#  Check arguments.

        if not isinstance( independentCoordinate, np.ndarray ): 
            raise LagrangePolynomialInterpolateError( "InvalidArguments", "First argument must be a numpy array" )

        if len( independentCoordinate.shape ) != 1: 
            raise LagrangePolynomialInterpolateError( "InvalidArguments", "First argument must have one dimension" )

        self._independentCoordinate = independentCoordinate
        self._independentCoordinate_dimension = independentCoordinate.size

        if not isinstance( dependentValues, np.ndarray ): 
            raise LagrangePolynomialInterpolateError( "InvalidArguments", "Second argument must be a numpy array" )

        shape = dependentValues.shape 
        ndims = len( shape )
        if ndims not in { 1, 2 } : 
            raise LagrangePolynomialInterpolateError( "InvalidArguments", "First argument must have one dimension" )

        if ndims == 1: 
            if shape[0] != self._independentCoordinate_dimension :
                raise LagrangePolynomialInterpolateError( "InvalidArguments", "Second argument must have final " + 
                    "dimension the same as the first argument's dimension" )
        else: 
            if shape[1] != self._independentCoordinate_dimension :
                raise LagrangePolynomialInterpolateError( "InvalidArguments", "Second argument must have final " + 
                    "dimension the same as the first argument's dimension" )

        if ndims == 1: 
            self._dependentValues = np.reshape( (1,self._independentCoordinate_dimension), dependentValues )
        else: 
            self._dependentValues = dependentValues


    def __call__( self, x, n=8, derivative=False ): 
        """
Return values interpolated using Lagrange polynomials to independent coordinate values in array x. 
The polynomial will be of degree n. Optionally, the derivative can be computed instead by setting
derivative=True."""

#  Check that the requested satID exists.

#  Accept scalar x or numpy array x.

        if not isinstance( x, np.ndarray ) and not isinstance( x, float ) and not isinstance( x, int ):
            raise LagrangePolynomialInterpolateError( "InvalidArgument", "x must class numpy.ndarray, float, or int" )

        if isinstance( x, np.ndarray ): axs = x
        if isinstance( x, float ) or isinstance( x, int ): axs = np.array( [x], dtype='d' )

        if axs.min() < self._independentCoordinate.min() or axs.max() > self._independentCoordinate.max() :
            raise LagrangePolynomialInterpolateError( "InvalidCoordinates", "The input x values fall outside the valid range" )

#  Check for valid number of degrees.

        if n > self._independentCoordinate_dimension : 
            raise LagrangePolynomialInterpolateError( "InvalidExpansion", "Interpolation degree exceeds dimension of input arrays." )

#  Determine which time interval we are in.

#  Alternate form of computing irecs (matching records) without the
#  large array allocations of the prior method
#  (commented out above to preserve for historical reasons).

        irecs = np.zeros( axs.size, dtype='i' )

#  Loop over axs to determine irecs

        di = max( [ 1, int( 100000 / self._independentCoordinate_dimension ) ] )

        for i0 in range( 0, axs.size, di ): 
            i1 = min( [ i0 + di, axs.size ] )

            x1 = np.repeat( self._independentCoordinate, i1-i0 ).reshape( ( self._independentCoordinate_dimension, i1-i0 ) )
            x2 = np.repeat( axs[i0:i1], self._independentCoordinate_dimension-1 ).reshape( ( i1-i0, self._independentCoordinate_dimension-1 ) ).T
            matchups = np.argwhere( np.logical_and( x2 >= x1[:-1,:], x2 <= x1[1:,:] ) )

            for m in matchups: 
                irecs[ m[1] + i0 ] = m[0]

#  Collect the records to use in the polynomial interpolation.

        irecs0 = irecs - int(n/2) + 1
        irecs0[ irecs0 < 0 ] = 0
        irecs1 = irecs0 + (n-1)
        irecs1[ irecs1 >= self._independentCoordinate_dimension ] = self._independentCoordinate_dimension - 1
        irecs0 = irecs1 - (n-1)

        neff = n

#  Initialize the polynomial coefficients for determining position
#  and velocity.

        alphas = np.zeros( (neff,axs.size), dtype='d' )
        dalphadts = np.zeros( (neff,axs.size), dtype='d' )

        for i in range(neff):

           irecs = irecs0 + i

#  The numerator (num) is used for computing coefficients for
#  interpolating position. The denominator (denom) is used for
#  computing coefficients for both the positions and the
#  velocities.

           denom, num = 1.0, 1.0
           for k in range(neff):
               if k == i: continue
               krecs = irecs0 + k
               num *= ( axs - self._independentCoordinate[krecs] )
               denom *= ( self._independentCoordinate[irecs] - self._independentCoordinate[krecs] )

#  This numerator (dnum) is used for computing coefficients
#  for interpolating velocities

           dnum = 0.0
           for l in range(neff):
               if l == i: continue
               prod = 1.0
               for k in range(neff):
                   if k in { i, l }: continue
                   krecs = irecs0 + k
                   prod *= ( axs - self._independentCoordinate[krecs] )
               dnum += prod

           alphas[i,:] = num / denom
           dalphadts[i,:] = dnum / denom

#  Compute interpolants or derivatives. 

        y = 0.0
        yd = 0.0

        for i in range(neff):
            y += self._dependentValues[:,irecs0+i] * alphas[i,:]
            yd += self._dependentValues[:,irecs0+i] * dalphadts[i,:]

        y = y.squeeze()
        yd = yd.squeeze()

#  Format the output correctly. 

        if isinstance( x, np.ndarray ): 

            if len( self._dependentValues.shape ) == 1: 
                y = y 
                yd = yd
            else: 
                y = np.reshape( y, ( self._dependentValues.shape[0], x.shape[0] ) )
                yd = np.reshape( yd, ( self._dependentValues.shape[0], x.shape[0] ) )

        else: 

            if len( self._dependentValues.shape ) == 1: 
                y = y[0]
                yd = yd[0]
            else: 
                y = y 
                yd = yd

#  Done. 

        if derivative: 
            return yd
        else: 
            return y


    def close(self):
        pass


################################################################################
#  screen tool
################################################################################

def screen( netcdfvar ): 
    """This function screens a UCAR NetCDF variable for good values. It 
    returns an array of indices pointing to valid values. Values are 
    considered invalid if they are found to be NaN or _FillValue."""

    x = netcdfvar[:].data.squeeze() 
    good = np.logical_not( np.isnan( x ) )

    if "_FillValue" in netcdfvar.ncattrs(): 
        _FillValue = netcdfvar.getncattr( "_FillValue" )
        good = np.logical_and( good, x != _FillValue )

    indices = np.argwhere( good ).flatten()

    return indices



################################################################################
#  transformcoordinates
################################################################################

#  General tools for vector and Astropy manipulation.

def cartesian( coord ):
    """This function converts an astropy SkyCoord object to a numpy ndarray
for a Cartesian representation of the unit vector direction."""

    c = coord.cartesian
    return np.array( [ c.x, c.y, c.z ] )

def normalize( vector ):
    """Return a normalized version of the vector input."""

    out = vector / np.linalg.norm( vector )
    return out

def transformcoordinates( inputPositions, times, epoch, direction='eci2ecf', ecisystem="teme" ):
    """This function transforms an ndarray of inputPositions with shape = (ntimes,3)
at times (in seconds) with respect to epoch from ECI to ECF coordinates or vice
versa. The output will be another ndarray with shape = (ntimes,3). The epoch must be
an instance of class Time. The direction must be either eci2ecf or ecf2eci. The 
ecisystem is a specification of the particular ECI coordinate system to reference. 
The default is teme (true equator mean equinox). 
"""

    atimes = np.array( times )
    ntimes = atimes.size

#  Check dimensions of arguments.

    if len( inputPositions.shape ) == 1:
        if inputPositions.size != 3:
            raise transformcoordinatesError( "InvalidArgument", "Need 3 coordinates in inputPositions" )

    elif len( inputPositions.shape ) == 2:
        if inputPositions.shape[0] != ntimes or inputPositions.shape[1] != 3:
            raise transformcoordinatesError( "InvalidArgument", "inputPositions has incorrect shape" )

    else: 
        raise transformcoordinatesError( "InvalidArgument", "inputPositions has too many dimensions" )

    if direction not in { "eci2ecf", "ecf2eci" }:
        raise transformcoordinatesError( "InvalidArgument", "Unrecognized value for direction" )

    if ecisystem not in [ "teme", "icrs" ]: 
        raise transformcoordinatesError( "InvalidArgument", "Unrecognized value for ECI coordinate system" )

    positions = inputPositions.reshape( ( ntimes, 3 ) )

#  First, define three times (e0, e1, e2) at which to compute ECI direction coordinates for a
#  fixed point on the Earth.

    cal = ( epoch + float( atimes[0] ) ).calendar("utc")
    e0 = Time( utc=Calendar(year=cal.year, month=cal.month, day=cal.day, \
        hour=cal.hour, minute=cal.minute ) )
    e1 = e0 + 2*3600.0
    e2 = e0 + 4*3600.0

#  Fixed location on the Earth.

    obstime0 = e0.calendar("utc").isoformat() 
    obstime1 = e1.calendar("utc").isoformat() 
    obstime2 = e2.calendar("utc").isoformat() 

    ECFx = SkyCoord( 1.0, 0.0, 0.0, frame="itrs", obstime=obstime0 )
    ECI_of_ECFx_at_e0 = normalize( np.array( ECFx.transform_to( ecisystem ).represent_as("cartesian").xyz ) )

    ECFx = SkyCoord( 1.0, 0.0, 0.0, frame="itrs", obstime=obstime1 )
    ECI_of_ECFx_at_e1 = normalize( np.array( ECFx.transform_to( ecisystem ).represent_as("cartesian").xyz ) )

    ECFx = SkyCoord( 1.0, 0.0, 0.0, frame="itrs", obstime=obstime2 )
    ECI_of_ECFx_at_e2 = normalize( np.array( ECFx.transform_to( ecisystem ).represent_as("cartesian").xyz ) )

#  Define pole direction in ECI direction-coordinates.

    de1 = normalize( ECI_of_ECFx_at_e1 - ECI_of_ECFx_at_e0 )
    de2 = normalize( ECI_of_ECFx_at_e2 - ECI_of_ECFx_at_e1 )
    pole = normalize( np.cross( de1, de2 ) )
    ECIpole = SkyCoord( *pole, frame=ecisystem, obstime=obstime0 )

#  Pole direction in ECF direction-coordintes.

    ECFpole = ECIpole.transform_to( "itrs" )

#  Compute the sidereal spin rate.

    dangle = np.arcsin( np.dot( pole, np.cross(de1,de2) ) )
    spin = dangle / ( 0.5 * ( ( e2 - e1 ) + ( e1 - e0 ) ) )

#  Construct the new basis at e0 in both ECI coordinates and in ECF coordinates.

    ECIbasisz = pole
    ECIbasisx = normalize( ECI_of_ECFx_at_e0 - np.dot( ECI_of_ECFx_at_e0, pole ) * pole )
    ECIbasisy = np.cross( ECIbasisz, ECIbasisx )
    ECIbasis = np.array( [ ECIbasisx, ECIbasisy, ECIbasisz ] ).T

    ECFbasisz = normalize( cartesian( ECFpole ) )
    x = normalize( cartesian( ECFx ) )
    ECFbasisx = normalize( x - np.dot( x, ECFbasisz ) * ECFbasisz )
    ECFbasisy = np.cross( ECFbasisz, ECFbasisx )
    ECFbasis = np.array( [ ECFbasisx, ECFbasisy, ECFbasisz ] ).T

#  Compute the array of rotation angles.

    angles = ( atimes + ( epoch - e0 ) ) * spin
    cosangles = np.cos( angles )
    sinangles = np.sin( angles )

#  ECF to ECI transformation.

    if direction == "ecf2eci":

        ECFpositions = positions

#  Transform coodinates to "true pole" ECF cartesian coordinates.

        nECFpositions = np.matmul( ECFpositions, ECFbasis )

#  Transform to ECI basis as at e0.

        rnECIpositions = nECFpositions

#  Rotate.

        nECIpositions = np.array( [ rnECIpositions[:,0] * cosangles - rnECIpositions[:,1] * sinangles, \
            rnECIpositions[:,0] * sinangles + rnECIpositions[:,1] * cosangles, \
            rnECIpositions[:,2] ] ).T

#  Expand in ECI space.

        ECIpositions = np.matmul( nECIpositions, ECIbasis.T )

#  Done with ECF-to-ECI.

        outputPositions = ECIpositions

    else:

        ECIpositions = positions

#  Project onto ECI basis.

        nECIpositions = np.dot( ECIpositions, ECIbasis )

#  Rotate.

        rnECIpositions = np.array( [ nECIpositions[:,0] * cosangles + nECIpositions[:,1] * sinangles, \
            -nECIpositions[:,0] * sinangles + nECIpositions[:,1] * cosangles, \
            nECIpositions[:,2] ] ).T

#  Transform to ECF basis as at e0.

        nECFpositions = rnECIpositions

#  Transform coodinates to ECF coordinates from ECF basis.

        ECFpositions = np.matmul( nECFpositions, ECFbasis.T )

#  Output.

        outputPositions = ECFpositions

#  Done.

    return outputPositions



def tangentpoint_radii( positionLEO, positionGNSS, centerOfCurvature=np.zeros(3) ): 
    """This function computes the distance from the Earth's center to the 
    straight-line path tangent points given the receiver's position(s) 
    positionLEO and the transmitter's position(s) positionGNSS. Each satellite 
    position must have shape (n,3) or (3,) with n the number of instances 
    of time for the satellites' positions. A numpy ndarray of distances 
    to the center of the Earth (radii) with dimension n."""

    ret = { "status": None, "messages": [], "comments": [] }

    #  Check input. 

    ndims = len( positionLEO.shape )
    if ndims not in [1,2]: 
        ret['status'] = "fail"
        comment = "Input LEO positions must be one- or two-dimensional"
        ret['messages'].append( "FaultyInput" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    if len( positionGNSS.shape ) not in [1,2]: 
        ret['status'] = "fail"
        comment = "Input GNSS positions must be one- or two-dimensional"
        ret['messages'].append( "FaultyInput" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    if ( np.array(positionLEO.shape) == np.array(positionGNSS.shape) ).sum() != ndims: 
        ret['status'] = "fail"
        comment = "Dimensions of LEO positions and GNSS positions do not match"
        ret['messages'].append( "FaultyInput" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    if positionLEO.shape[-1] != 3: 
        ret['status'] = "fail"
        comment = "Last dimension of positionLEO and positionGNSS must be 3"
        ret['messages'].append( "FaultyInput" )
        ret['comments'].append( comment )
        LOGGER.error( comment )
        return ret

    if ndims == 2: 
        ntimes = positionLEO.shape[0]
    else: 
        ntimes = 1

    #  Orbits with respect to center of curvature. 

    p1 = positionGNSS.reshape( (ntimes,3) ) - centerOfCurvature
    p2 = positionLEO.reshape( (ntimes,3) ) - centerOfCurvature

    #  Compute tangent points. 

    p1sq = ( p1**2 ).sum(axis=1)
    p2sq = ( p2**2 ).sum(axis=1)
    p1p2 = ( p1 * p2 ).sum(axis=1)
    t = ( p1sq - p1p2 ) / ( p1sq + p2sq - 2*p1p2 )
    points = p1 + ( (p2-p1).T * t ).T

    #  Compute tangent point radii. 

    radii = np.sqrt( ( points**2 ).sum(axis=1) )

    #  Done. 

    ret['status'] = "success"
    ret.update( { 'value': radii } )

    return ret

