"""
The `rotation_collocation` module contains the core implementation 
of the rotation-collocation methods detailed in Meredith, Leroy,
Halperin, and Cahoy 2023. It also includes code to extract 
nadir-scanner soundings associated with collocations.

Last updated: 9/8/2023 by Alex Meredith
"""

import bisect
import numpy as np
from sgp4.api import Satrec
from .nadir_satellite import NadirSatelliteInstrument
from .awsro import get_occ_times
from .collocation import Collocation, CollocationList
from .constants_and_utils import km_to_degree, constrain_to_pi_range, constrain_angle_pair


#  Exception handling. 

class Error( Exception ): 
    pass

class rotationCollocationError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment



def rotation_collocation( nadir_sat, occs, time_tolerance, spatial_tolerance, 
                         nsuboccs=2 ): 
    """This function performs collocation-finding using the rotation-collocation
    method. If nsuboccs > 2, this is equivalent to the rotation-collocation method
    with sub-occultations. If nsuboccs = 2, this is equivalent to the linearized
    rotation-collocation method.

    Arguments
    -----------
        nadir_sat: subclass of NadirSatelliteInstrument
            Nadir-scanning satellite object
        occs: OccList
            A list of occultations (and their metadata)
        time_tolerance: float
            Time tolerance, in seconds
        spatial_tolerance: float
            Spatial tolerance, in m
        nsuboccs: int
            Number of sub-occultations (use 2 for linearized)

    Returns
    ---------
        A dictionary with keyword value pairs as follows: 

        status -> "success" or "fail"
        messages -> a list of mnemonic messages regarding performance
        comments -> a list of commentary messages regarding performance
        data -> an instance of CollocationList containing found collocations
    """

    #  Initialize return dictionary. 

    ret = { 'status': None, 'messages': [], 'comments': [], 'data': None }

    #  Check input. 

    if not isinstance( nadir_sat, NadirSatelliteInstrument ): 
        ret['messages'].append( "InvalidArgument" )
        ret['comments'].append( "nadir_sat must be a subclass of NadirSatelliteInstrument" )
        ret['status'] = "fail"
        return ret

    if nsuboccs < 2:
        ret['messages'].append( "InvalidArgument" )
        ret['comments'].append( 
            """Rotation-collocation method needs at least 2 
            points for interpolation. Use nsuboccs=2 for the linearized 
            rotation-collocation method and use nsuboccs>2 for the 
            rotation-collocation method with sub-occultations""" )
        ret['status'] = "fail"
        return ret

    ret_rotate = nadir_sat.rotate_to_nadir_frame( occs, time_tolerance, nsuboccs )
    if ret_rotate['status'] == "fail": 
        ret['status'] = "fail"
        ret['messages'] += ret_rotate['messages']
        ret['comments'] += ret_rotate['comments']
        return ret
    nadir_frame_positions = ret_rotate['data']

    dt = 2 * time_tolerance/(nsuboccs-1)

    colocs = []

    #  Get satellite period (assume it doesn't change much over the day)

    time = get_occ_times( occs[0] )[0]
    resp = nadir_sat.get_current_tle( time )

    if resp['status'] == "fail": 
        ret['status'] == "fail"
        ret['messages'] += resp['messages']
        ret['comments'] += resp['comments']
        return ret
    else: 
        tle = resp['data']

    sat = Satrec.twoline2rv( *tle )
    sat_period = 2*np.pi/sat.nm * 60        #  Convert to seconds from minutes

    for iocc in range(occs.size): 

        occ = occs[iocc]
        time = get_occ_times( occ )[0]
        latitude = np.deg2rad( occ.values("latitude")[0] )
        longitude = np.deg2rad( occ.values("longitude")[0] )

        #  Get max scan angle (depends on latitude + nadir-scanner altitude)

        max_scan_angle = nadir_sat.calculate_ds_with_sgp4( time, latitude, spatial_tolerance, sat )

        for isubocc in range(1,nsuboccs):

            sub_occ_pos_prev = nadir_frame_positions[iocc, isubocc-1, :, 0]
            sub_occ_time_prev = time + (isubocc-1-(nsuboccs-1)/2)*dt
            sub_occ_pos = nadir_frame_positions[iocc, isubocc, :, 0]
            sub_occ_time = time + (isubocc-(nsuboccs-1)/2)*dt

            x_prev = sub_occ_pos_prev[0]
            y_prev = sub_occ_pos_prev[1]
            z_prev = sub_occ_pos_prev[2]
            x, y, z = sub_occ_pos[0], sub_occ_pos[1], sub_occ_pos[2]

            #  Number of expected orbits between sub-occultations. This is
            #  important for making sure we don't miss collocations when
            #  the time tolerance is quite long.

            num_revs = ( sub_occ_time - sub_occ_time_prev ) / sat_period
            ret_compare = compare_points_in_nadir_frame(x_prev, y_prev, z_prev,
                    x, y, z, max_scan_angle, num_revs, spatial_tolerance)

            if ret_compare is None: 
                continue

            frac = ( ret_compare['estimated_scan_angle'] - ret_compare['start_scan_angle'] ) \
                        / ( ret_compare['end_scan_angle'] - ret_compare['start_scan_angle'] )

            est_coloc_time = sub_occ_time_prev
            est_coloc_time += frac * ( sub_occ_time - sub_occ_time_prev )

            max_scan_angle = nadir_sat.calculate_ds_with_sgp4( est_coloc_time, 
                    latitude, spatial_tolerance, sat )

            # Double check to make sure max scan angle is exactly right
            # at estimated collocation time

            if ret_compare['estimated_scan_angle'] <= max_scan_angle:
                coloc = Collocation( occ, nadir_sat, 
                                    longitude=np.rad2deg(longitude), 
                                    latitude=np.rad2deg(latitude), 
                                    scan_angle=np.rad2deg(ret_compare['estimated_scan_angle']), 
                                    time=est_coloc_time )
                colocs.append( coloc )
                break

    ret['status'] = "success"
    ret['data'] = CollocationList( colocs )

    return ret


def compare_points_in_nadir_frame(x_prev, y_prev, z_prev, x_j, y_j, z_j,
                                  max_scan_angle, num_revs, spatial_tolerance):
    """
    This function checks if two points in the nadir satellite frame are
    collocated. It is a helper used by the rotation-collocation method.

    Arguments
    ----------
        x_prev : float
            x-coordinate of start point in nadir satellite frame
        y_prev : float
            y-coordinate of start point in nadir satellite frame
        z_prev : float
            z-coordinate of start point in nadir satellite frame
        x_j : float
            x-coordinate of end point in nadir satellite frame
        y_j : float
            y-coordinate of end point in nadir satellite frame
        z_j : float
            z-coordinate of end point in nadir satellite frame
        max_scan_angle : float
            maximum scan angle of the nadir satellite
        num_revs: float
            fractional number of orbits completed between t_prev and t_j
        spatial_tolerance: float
            spatial tolerance for colocation in m

    Returns
    ----------
        None if no collocation is found. If a collocation is found, then a 
        dictionary is returned with the following keyword-value pairs: 

        estimated_scan_angle -> float
            Estimated scan angle (radians) of the collocated sounding
        start_scan_angle -> float
            Scan angle (radians) of the sub-occultation preceding the collocation
        end_scan_angle -> float
            Scan angle (radians) of the sub-occultation immediately succeding the 
            collocation. 
    """

    is_coloc = False

    # Find start and end arglat
    start_delta_arglat = np.arctan2(y_prev, x_prev)
    end_delta_arglat = np.arctan2(y_j, x_j)

    # Shift delta arglats such that end < start and |end-start| < 2pi
    (end_delta_arglat,
     start_delta_arglat) = constrain_angle_pair(end_delta_arglat,
                                                start_delta_arglat,
                                                num_full_revs=num_revs)
    min_delta_arglat = end_delta_arglat
    max_delta_arglat = start_delta_arglat

    # Find start and end scan angle
    start_scan_angle = np.arcsin(z_prev)
    end_scan_angle = np.arcsin(z_j)

    # Shift scan angles to be within -pi and pi
    start_scan_angle = constrain_to_pi_range(start_scan_angle)
    end_scan_angle = constrain_to_pi_range(end_scan_angle)

    # Find scan angle in terms of arglat (linear fit)
    rise = end_scan_angle-start_scan_angle
    run = end_delta_arglat-start_delta_arglat
    slope = rise/run

    # Need to check for 2pi crossing if num_full_revs is big enough, etc.
    i = 0
    zero_crossing = 0
    while i*2*np.pi < start_delta_arglat:
        y_int = end_scan_angle - slope*end_delta_arglat
        zero_crossing = y_int + i*slope*2*np.pi

        # Check for zero crossing within scan range
        if min_delta_arglat < 0 and max_delta_arglat > 0:
            if np.abs(zero_crossing) < max_scan_angle:
                is_coloc = True
                break
        i += 1

    # Check for near-zero delta arglat within scan range
    spatial_tol_rad = np.deg2rad(spatial_tolerance*1.0e-3*km_to_degree)
    if np.cos(start_delta_arglat) > np.cos(spatial_tol_rad):
        if np.abs(start_scan_angle) < max_scan_angle:
            is_coloc = True
            zero_crossing = start_scan_angle
    if np.cos(end_delta_arglat) > np.cos(spatial_tol_rad):
        if np.abs(end_scan_angle) < max_scan_angle:
            is_coloc = True
            zero_crossing = end_scan_angle

    if is_coloc: 
        ret = { 'estimated_scan_angle': zero_crossing, 
                'start_scan_angle': start_scan_angle, 
                'end_scan_angle': end_scan_angle }
    else: 
        ret = None

    return ret

