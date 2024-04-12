"""
Module: nadir_satellite.py
Authors: Alex Meredith, Stephen Leroy
Contact: sleroy@aer.com
Date: April 10, 2024

This module contains two classes that define nadir-scanning satellite 
instruments and provide a context for storing geolocation metadata from 
their data files. 

NadirSatelliteInstrument
========================
Provides information on the details of how a nadir-scanning satellite 
instrument goes about scanning. It also includes a suite of methods 
that can be used in applying the rotation-collocation algorithm. Those 
methods are...

    - get_geolocations: Gets geolocation metadata for a specified 
            time range; it populates an instance of ScanMetadata
    - get_current_tle: Gets two-line element for the host satellite 
            that is nearest to a specified time; it accesses 
            Spacetrack
    - calculate_ds_from_xi: Calculate approximate maximum scan width 
            given a geodetic latitude; it only computes the local 
            radius of the Earth to estimate the scan width ds 
    - calculate_ds_with_sgp4: Calculate precise maximum scan width
            given time, latitude, etc., based on SGP4 orbit 
            propgation 
    - get_inertial_lat_lon: Calculate the longitude-latitude 
            coordinates of a set of occultations in ECI coordinates. 
    - rotate_to_nadir_frame: Rotate (sub-)occultations into the 
            reference frame of a nadir scanner's scan pattern; it is 
            the heart of the rotation-collocation algorithm. 

ScanMetadata
============
Stores geolocation information and metadata useful for retrieving 
radiance data for the satellite instrument. It is a callable object, 
retrieving radiance data corresponding to a scan index and a 
footprint index for the geolocation data stored in the object. 

In order for the object to retrieve nadir-scanner radiance data, 
it must connect to a "get_data" method to be provided by the 
satellite instrument object that inherits this class. 
"""

import erfa
import numpy as np
from abc import ABC, abstractmethod
from sgp4.ext import rv2coe
from sgp4.api import Satrec
from astropy.coordinates.builtin_frames.utils import get_polar_motion

from .awsro import get_occ_times
from .constants_and_utils import sec_to_sidereal_day, calculate_km_to_degree, mu, calculate_radius_of_earth
from .spacetrack import Spacetrack

#  Exception handling. 

class Error( Exception ): 
    pass

class nadirSatelliteError( Error ): 
    def __init__( self, message, comments ): 
        self.message = message
        self.comments = comments 


class NadirSatelliteInstrument(ABC):
    """Generalized nadir-scanning satellite object. This class contains methods
    key to the function of the rotation-collocation method -- a method to
    rotate points into the coordinate frame given by the nadir-scanner
    satellite, and several helper methods for this coordinate frame
    transformation.

    Arguments
    ------------
        name: string
            Name of satellite (ex. NOAA-20)
        max_scan_angle: float
            Nadir-scanning instrument maximum scan angle [degrees]
        time_between_scans: float
            Time taken to complete one cross-track scan [seconds]
        scan_points_per_line: int
            Number of scan points in each cross-track scan
        scan_angle_spacing: float
            Angle between scan points in each cross-track scan [degrees]
        spacetrack: spacetrack.Spacetrack
            Portal to Spacetrack TLE data on the local file system

    Attributes
    ------------
        satellite_name: string
            Name of satellite (ex. NOAA-20)
        spacetrack_satellite: instance of spacetrack.SpacetrackSatellite
            An instance of spacetrack.SpacetrackSatellite for access to TLEs
        xi: float
            Nadir-scanning instrument maximum scan angle [radians]
        time_between_scans: float
            Time between cross-track scans [seconds]
        scan_points_per_line: int
            Number of individual footprints per cross-track scan
        max_scan_angle: float
            Maximum scan angle [radians]
        scan_angle_spacing: float
            Angle between scan footprints [radians]"""

    def __init__(self, satellite_name, max_scan_angle, 
                 time_between_scans, scan_points_per_line, scan_angle_spacing, 
                 spacetrack=None):
        """
        Constructor for NadirSatelliteInstrument.
        """
        self.satellite_name = satellite_name
        self.xi = np.deg2rad(max_scan_angle)

        self.time_between_scans = time_between_scans
        self.scan_points_per_line = scan_points_per_line
        self.scan_angle_spacing = np.deg2rad(scan_angle_spacing)
        self.max_scan_angle = np.deg2rad(max_scan_angle)

        if isinstance( spacetrack, Spacetrack ): 
            self.spacetrack_satellite = spacetrack.satellite( satellite_name )
        else: 
            self.spacetrack_satellite = None

    #====================================================================
    #  Abstract methods. These must be defined by the child class that 
    #  inherits this base class. 
    #====================================================================

    @abstractmethod
    def get_geolocations( self, timerange ):
        """This method retrieves the geolocations of nadir-scanner soundings 
        that fall within a user-prescribed timerange. It returns an instance 
        of class ScanMetadata. 

        Arguments
        ---------
            timerange: tuple/list of timestandards.Time objects
                This defines the time range of nadir-scanner geolocations 
                to be retrieved. 

        Returns
        ---------
            geolocations: ScanMetadata
                An object containing information on longitudes, latitudes, and 
                and times of nadir-scanner soundings. It also contains pointers 
                to data files and a method to retrieve observational (radiance) 
                data from those files."""

    @abstractmethod
    def get_data( self, file, scan_index, footprint_index, **kwargs ): 
        """A function which returns nadir-scan satellite data for requested scan and
        footprint indices within the file *file*. The function itself must be a
        Method to fetch data for a nadir-scan satellite instrument corresponding to
        a scalar integer indicating the scan number [0:nscans] and a scalar integer
        indicating the footprint number [0:nfootprints].

        It should return a data dictionary according taken from file *file* and the
        data location within the file should correspond to scans
        data[scan_index,footprint_index].

        Optional arguments are passed to get_data as the dictionary kwargs."""


    #====================================================================
    #  Defined methods. Mostly used in the rotation-collocation 
    #  algorithm. 
    #====================================================================

    def get_current_tle(self, occultation_time):
        """This method gets the current TLE for the satellite at a given time.
        It should be implemented by child classes appropriately, depending
        on how TLEs are stored.

        Parameters
        ------------
            occultation_time: timestandards.Time
                Time of RO sounding 

        Returns
        ---------
            tle_1: string
                first line of TLE
            tle_2: string
                second line of TLE"""

        ret = self.spacetrack_satellite.nearest( occultation_time )
        return ret

    def calculate_ds_with_xi(self, lat_geodetic, alt ):
        """This method calculates the latitude-dependent scan angle for use
        in the rotation method, with varying max scan distance. This
        method is used for debugging/visualizing only.

        Arguments
        -----------
            lat_geodetic: float
                Geodetic latitude [radians]
            alt: float
                Altitude [m], default to satellite semi-major axis

        Returns
        ---------
            ds: float
                Latitude-dependent scan angle [radians]"""

        if alt is None:
            alt = self.a
        r = calculate_radius_of_earth(lat_geodetic)
        ds = np.arcsin((alt/r)*np.sin(self.xi)) - self.xi

        return ds

    def calculate_ds_with_sgp4(self, sounding_time, sounding_lat, spatial_tolerance, sgp4_sat=None):
        """This method gets the max scan angle for a nadir-scanning satellite
        very precisely by propagating the satellite position with SGP4, getting
        nadir-scanner altitude and latitude at the necessary time, and then
        calculating the scan angle from there.

        Arguments
        ------------
            sounding_time: timestandards.Time
                time to calculate max scan angle
            sounding_lat: float
                sounding latitude for calculation, in radians
            spatial_tolerance: float
                spatial tolerance, for soundings just past true range, in m
            sgp4_sat: sgp4.wrapper.Satrec (optional)
                sgp4 satellite object with TLE already loaded, for efficiency

        Returns
        ----------
            max_scan_angle: float
                max scan angle in radians"""

        jd = sounding_time.juliandate()
        if sgp4_sat is None:
            tle = self.get_current_tle( sounding_time )
            sgp4_sat = Satrec.twoline2rv( *tle )

        e, r, v = sgp4_sat.sgp4(jd, 0)
        r_norm = (r[0]**2 + r[1]**2 + r[2]**2)**0.5
        nadir_scanner_lat = np.arcsin(r[2]/r_norm)

        ds = self.calculate_ds_with_xi(nadir_scanner_lat, r_norm)
        km_to_degree = calculate_km_to_degree(sounding_lat)
        max_scan_angle = abs(ds) + np.deg2rad(spatial_tolerance*1.0e-3*km_to_degree)

        return max_scan_angle

    def get_inertial_lat_lon(self, jd, polarmat, lon, lat):
        """Method to find the inertial latitude and longitude of an occultation.

        Arguments 
        -----------
            jd: float
                Julian date representing occultation time
            polarmat: np array
                Matrix representing polar motion
            lon: float
                Longitude of occultation [radians]
            lat: float
                Latitude of occultation [radians]
        Returns
        --------
            lon_inert: float
                Inertial longitude of occultation [radians]
            lat_inert: float
                Inertial latitude of occultation [radians]"""

        # Convert to position vector in ECEF coordinates

        pos_ITRS = np.array([[np.cos(lon)*np.cos(lat)],
                             [np.sin(lon)*np.cos(lat)],
                             [np.sin(lat)]])

        # Convert to TEME

        gst = erfa.gmst82(jd, 0)
        teme_to_itrs_mat = erfa.c2tcio(np.eye(3), gst, polarmat)
        occ_TEME = teme_to_itrs_mat.T@pos_ITRS

        # Find inertial latitude and longitude from TEME

        lon_inert = np.arctan2(occ_TEME[1][0], occ_TEME[0][0])
        lat_inert = np.arcsin(occ_TEME[2][0])

        return lon_inert, lat_inert

    def rotate_to_nadir_frame( self, occs, time_tolerance, nsuboccs ):
        """Method to get the position in the coordinate frame given by the
        nadir sounder for a list of occultations. Used by the
        rotation-collocation method.

        Arguments
        -----------
            occs: OccList
                A set of occultations given as an OccList. 
            time_tolerance: float
                Time matchup tolerance, in seconds. The two furthest-
                apart sub-occultations will be separated by
                2*time_tolerance seconds.
            nsuboccs: int
                Number of sub-occultations

        Returns
        -----------
            nadir_positions: np.ndarray (m x N x 3 x 1)
                Position rotated into the nadir-scanner frame for each
                sub-occultation. nadir_positions[m][n] is the position
                vector in the nadir-scanner frame for the nth
                sub-occultation of the mth occultation."""

        ret = { 'status': None, 'messages': [], 'comments': [], 'data': None }

        #  Get spacing between sub-occultations in sec

        if nsuboccs == 1:
            subocc_spacing = 0
        elif nsuboccs > 1:
            subocc_spacing = 2 * time_tolerance / (nsuboccs-1)

        nadir_positions = np.zeros( ( occs.size, nsuboccs, 3, 1 ), dtype=np.float64 )

        s, tline = "", ""
        polarmat = np.zeros((3, 3))

        for iocc in range(occs.size): 

            occ = occs[iocc]
            lon = np.deg2rad( occ.values("longitude")[0] )
            lat = np.deg2rad( occ.values("latitude")[0] )
            time = get_occ_times( occ )[0]

            # Get polar motion and TLE (only once per day; this
            # doesn't need to be done for each occultation)

            if iocc == 0:
                xp, yp = get_polar_motion( time.juliandate() )
                polarmat = erfa.pom00( xp, yp, 0 )
                ret_tle = self.get_current_tle( time )
                ret['messages'] += ret_tle['messages']
                ret['comments'] += ret_tle['comments']
                if ret_tle['status'] == "fail": 
                    ret['status'] = "fail"
                    return ret
                tle = ret_tle['data']
                sat = Satrec.twoline2rv( *tle )

            # Find inertial longitude & latitude for occultation

            lon_inert, lat_inert = self.get_inertial_lat_lon( time.juliandate(), polarmat, lon, lat)
            sidereal_spin_per_sec = 2*np.pi * sec_to_sidereal_day

            # Loop over sub-occultations

            for isubocc in range(nsuboccs):

                # Find sub-occultation time and longitude (we assume the
                # atmosphere rotates with the Earth, so the longitude
                # changes with Earth's rotation, which can be approximated
                # by sidereal spin). 

                subocc_offset = (isubocc-((nsuboccs-1)/2)) * subocc_spacing
                subocc_time = time + subocc_offset
                subocc_lon = lon_inert + sidereal_spin_per_sec * subocc_offset

                # Use sgp4 to get orbital position and orbital elements. 

                jd = subocc_time.juliandate()
                r, v = sat.sgp4(jd, 0)[1:3]
                incl, raan_lon, argp, nu = rv2coe(r, v, mu)[3:7]
                arglat = argp + nu
                lon_diff = subocc_lon - raan_lon

                # Construct matrix to rotate into nadir-scanner frame. 

                arglat_matrix = np.array([[np.cos(arglat), np.sin(arglat), 0],
                                          [-np.sin(arglat), np.cos(arglat), 0],
                                          [0, 0, 1]])

                incl_matrix = np.array([[1, 0, 0],
                                        [0, np.cos(incl), np.sin(incl)],
                                        [0, -np.sin(incl), np.cos(incl)]])

                raan_matrix = np.array([[np.cos(lon_diff)*np.cos(lat_inert)],
                                        [np.sin(lon_diff)*np.cos(lat_inert)],
                                        [np.sin(lat_inert)]])

                pos_subocc = arglat_matrix @ incl_matrix @ raan_matrix

                nadir_positions[iocc, isubocc, :, :] = pos_subocc

        #  Done. 

        ret['status'] = "success"
        ret['data'] = nadir_positions

        return ret



class ScanMetadata(): 
    """This class defines the geolocations and data pointer information for a set of 
    scans from a nadir-scan satellite. The various elements are 

    'get_data': NadirSatelliteInstrument.get_data -> Pointer to the get_data 
            method in the NadirSatelliteInstrument *child* class. 

    'longitudes': numpy.ndarray -> Array of longitudes [rads] of nadir-scan soundings. 
            The dimension of the array is nscans x nfootprints, where nscans is the 
            number of scans in the returned data set and nfootprints is the number of 
            footprints in each scan.

    'latitudes': numpy.ndarray -> Array of latitudes [rads] of nadir-scan soundings. 
            The dimension of the array is nscans x nfootprints, where nscans is the 
            number of scans in the returned data set and nfootprints is the number of 
            footprints in each scan.

    'mid_times': list -> numpy.ndarray of numpy.datetime64 objects corresponding to 
            the middle of the scans in the returned data set. It represents UTC, and 
            its length is nscans. 

    'files': list -> List of file names containing the data. These are the paths to the 
            files containing the nadir-scan satellite data. Its length is nfiles. 

    'file_index': list -> List of indices pointing to the file (in 'files') containing 
            the data for a particular sounding. For example files[file_index[iscan]] is 
            the path to the file containing data for scan iscan in the returned data. 
            The length of the list is nscans. 

    'scan_index': list -> List of scan indices pointing to the scan index in the data file 
            indicating where to find the data. 

            For example, data[scan_index[iscan],:] in 
            file files[file_index[iscan]] is the data for scan iscan in an instance of 
            this class."""

    def __init__( self, get_data, longitudes, latitudes, mid_times, 
                 files, file_indices, scan_indices ): 
        """Longitudes and latitudes must be list-like objects in units of radians. 
        mid_times must be a list of timestandards.Time instances. files is a list of 
        strings; file_indices a list-like object of integers, as is scan_indices."""

        self.get_data = get_data
        self.longitudes = np.array( longitudes )
        self.latitudes = np.array( latitudes )
        self.mid_times = mid_times
        self.files = list( files )
        self.file_indices = np.array( file_indices )
        self.scan_indices = np.array( scan_indices )

    def __call__( self, iscan, ifootprint, **kwargs ): 
        """Retrieve a dictionary containing the radiance data for iscan and ifootprint 
        in this instance of ScanMetadata. The retrieved radiance data should correspond to 
        file self.files[iscan] with values data[self.scan_indices[iscan],self.footprint_indices[ifootprint]]. 
        The arguments iscan and ifootprint must be integers."""

        #  Check input. 

        integer_types = [ int, np.int8, np.int16, np.int32, np.int64 ]

        if not any( [ isinstance(iscan,t) for t in integer_types] ) or \
                not any( [ isinstance(ifootprint,t) for t in integer_types ] ) : 

            raise nadirSatelliteError( "InvalidArguments", 
                    "Arguments iscan and ifootprint must be integers." )

        file = self.files[ self.file_indices[ iscan ] ]
        scan_index = self.scan_indices[iscan]
        footprint_index = ifootprint

        ret = self.get_data( file, scan_index, footprint_index, **kwargs )

        return ret

