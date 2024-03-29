"""
The `brute_force` module implements two different brute-force 
collocation-finding methods, which are used as base methods 
to generate truth data against which the results of the 
rotation-collocation methods can be compared.

Last updated: 9/8/2023 by Alex Meredith
"""

from .collocation import Collocation, CollocationList
from .constants_and_utils import calculate_radius_of_earth
from .awsro import get_occ_times
from .timestandards import Time
from datetime import datetime
import numpy as np
from tqdm import tqdm 


def brute_force( nadir_satellite_instrument, occs, time_tolerance, spatial_tolerance,
                use_sorting=True, progressbar=True ):
    """This function is a wrapper that performs brute-force colocation finding.
    It loops over all occultation/nadir-scanner sounding pairs and checks each
    pair for cotemporality and spatial overlap, in order to find colocations
    that overlap in both time and space.

    Arguments
    ------------
        nadir_satellite_instrument: NadirSatelliteInstrument
            An instance of a child of the NadirSatelliteInstrument base class. 
        nadir_scanner_geolocations: ScanMetadata
            An object containing geolocation information on the nadir-scan 
            footprints. It also contains methods to retrieve observation data
            given scan and footprint indices. 
        occs: awsgnssroutils.database.OccList
            An object containing geolocation information on RO soundings and 
            pointers to relevant data files where RO data can be retrieved. 
        time_tolerance: float
            Time tolerance for colocations in seconds
        spatial_tolerance: float
            Spatial tolerance for colocations in m

    Returns
    -----------
        A dictionary with the following keyword, value information: 

        status -> "success" or "fail"
        messages -> A list of mnemonic messages regarding performance
        comments -> A list of comments regarding performance
        data -> An instance of CollocationList containing collocations found
    """

    #  Initialize return dictionary. 

    ret = { 'status': None, 'messages': [], 'comments': [], 'data': None }

    #  Set parameters. 

    time_convention = "gps"

    #  Load nadir-scanner geolocation data. Define time range for nadir-scanner 
    #  sounding geolocation data. 

    occs_datetime64 = np.array( [ np.datetime64( t.calendar(time_convention).isoformat() ) for t in get_occ_times( occs ) ] )
    tmin = occs_datetime64.min() - np.timedelta64( time_tolerance, "s" )
    tmax = occs_datetime64.max() + np.timedelta64( time_tolerance, "s" )
    timerange = ( Time( **{ time_convention: tmin } ), Time( **{ time_convention: tmax } ) )

    nadir_scanner_geolocations = nadir_satellite_instrument.get_geolocations( timerange )

    #  Initialize and loop over occultation. 

    colocs = []

    if progressbar: 
        iterator = tqdm( range(occs.size), desc="Brute force" )
    else: 
        iterator = range(occs.size)

    for iocc in iterator: 

        occ = occs[iocc]
        resp = brute_force_sorted( nadir_scanner_geolocations, occ, 
                    time_tolerance, spatial_tolerance )

        if resp['status'] == "fail": 
            ret['status'] == "fail"
            ret['messages'] += resp['messages']
            ret['comments'] += resp['comments']
            return ret

        if resp['data'] is not None:

            iscan, ifootprint = resp['data']['iscan'], resp['data']['ifootprint']

            coloc = Collocation( occ, nadir_satellite_instrument, 
                          longitude = np.rad2deg( nadir_scanner_geolocations.longitudes[iscan,ifootprint] ), 
                          latitude = np.rad2deg( nadir_scanner_geolocations.latitudes[iscan,ifootprint] ), 
                          time = nadir_scanner_geolocations.mid_times[iscan], 
                          scan_metadata = nadir_scanner_geolocations, 
                          iscan = iscan, 
                          ifootprint = ifootprint )

            colocs.append( coloc )

    #  Finalize. 

    ret['status'] = "success"
    ret['data'] = CollocationList( colocs )

    return ret


def brute_force_sorted( nadir_scanner_geolocations, occ, 
        time_tolerance, spatial_tolerance ): 
    """This function uses a brute-force method to find nadir-scanner soundings
    colocated with a single radio occultation. This is the 'efficient' version
    of the brute-force method, which relies on sorting nadir-scanner data
    to shrink the search window.

    Arguments
    ------------
        nadir_scanner_geolocations: ScanMetadata
            An object containing geolocation information on the nadir-scan 
            footprints. It also contains methods to retrieve observation data
            given scan and footprint indices. 
        occ: awsgnssroutils.database.OccList
            An object containing geolocation information on just one RO 
            sounding with pointers to relevant data files where RO data can 
            be retrieved. 
        time_tolerance: float
            Time tolerance for colocations in seconds
        spatial_tolerance: float
            Spatial tolerance for colocations in m

    Returns
    -----------
        A dictionary with the following keyword, value information: 

        status -> "success" or "fail"
        messages -> A list of mnemonic messages regarding performance
        comments -> A list of comments regarding performance
        data -> None if no collocation found; a dictionary containing the 
                'iscan' and 'ifootprint' of the collocated sounding
    """

    #  Initialize return dictionary. 

    ret = { 'status': None, 'messages': [], 'comments': [], 'data': None }

    #  Get nadir-scanner scans that fall within time tolerance. 

    time_convention = "gps"

    #  Occultation geolocation processing. 

    occ_datetime64 = np.datetime64( get_occ_times(occ)[0].calendar(time_convention).isoformat() )
    occ_longitude = np.deg2rad( occ.values("longitude")[0] )
    occ_latitude = np.deg2rad( occ.values("latitude")[0] )

    #  Nadir scanner geolocation processing. 

    mid_times_datetime64 = np.array( [ np.datetime64( t.calendar(time_convention).isoformat() ) \
            for t in nadir_scanner_geolocations.mid_times ] )

    #  Time tolerance. 

    time_tolerance_timedelta64 = np.timedelta64( time_tolerance, "s" )

    indices = np.argwhere( np.logical_and( 
            mid_times_datetime64 >= occ_datetime64-time_tolerance_timedelta64, 
            mid_times_datetime64 <= occ_datetime64+time_tolerance_timedelta64 ) ).squeeze()

    nadir_lons = nadir_scanner_geolocations.longitudes[indices,:]
    nadir_lats = nadir_scanner_geolocations.latitudes[indices,:]

    #  Establish x,y,z coordinates of the nadir soundings. 

    p_nadir = np.array( [ np.cos(nadir_lons)*np.cos(nadir_lats), 
                             np.sin(nadir_lons)*np.cos(nadir_lats), 
                             np.sin(nadir_lats) ] ).transpose( (1,2,0) )

    #  Establish x,y,z coordinates of the occultation sounding. 

    p_occ = np.array( [ np.cos(occ_longitude)*np.cos(occ_latitude), 
                           np.sin(occ_longitude)*np.cos(occ_latitude), 
                           np.sin(occ_latitude) ] )

    #  Find angular distances to all nadir soundings (sorted in time). Find the 
    #  index of the minimum distance. 

    dist_ang = np.arccos( np.inner( p_nadir, p_occ ) )
    arg = dist_ang.argmin()
    iiscan = int( arg / dist_ang.shape[1] ) 
    iscan, ifootprint = indices[iiscan], arg % dist_ang.shape[1]

    #  Find distance of closest nadir sounding. Test against spatial tolerance. 

    r_e = calculate_radius_of_earth( 0.5 * ( occ_latitude + nadir_scanner_geolocations.latitudes[iscan,ifootprint] ) )

    #  Finalize. 

    ret['status'] = "success"

    if dist_ang[iiscan,ifootprint] <= spatial_tolerance/r_e * 1.0e-3: 
        ret['data'] = { 'iscan': iscan, 'ifootprint': ifootprint }
    else: 
        ret['data'] = None

    return ret

