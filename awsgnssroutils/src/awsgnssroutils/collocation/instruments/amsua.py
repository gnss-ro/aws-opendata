"""metop_amsua.py

Authors: Alex Meredith, Stephen Leroy
Contact: Stephen Leroy (sleroy@aer.com)
Date: February 27, 2024

This contains the definition of an AMSU-A instrument class on board 
a Metop satellite with data hosted at the EUMETSAT Data Store. The class 
provided is named by class_name, which inherits the NadirSatelliteInstrument
class. 
"""

from netCDF4 import Dataset
import numpy as np
from datetime import datetime
import xarray 

from ..core.nadir_satellite import NadirSatelliteInstrument, ScanMetadata
from ..core.timestandards import Time
from ..core.eumetsat import eumetsat_time_convention
from ..core.constants_and_utils import masked_dataarray


#  REQUIRED attributes

instrument = "AMSU-A"
instrument_aliases = [ "AMSU-A", "Metop AMSU-A" ]
valid_satellites = [ "Metop-A", "Metop-B", "Metop-C" ]

#  REQUIRED methods of the class: 
#   - get_geolocations
#   - get_data


#  Parameters. 

fill_value = -1.0e20

#  Exception handling. 

class Error( Exception ): 
    pass

class metop_amsua_error( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment


#  Buffer for data files. 

open_data_file = { 'path': None, 'pointer': None }


class AMSUA(NadirSatelliteInstrument):
    """Metop AMSU-A satellite instrument object, inheriting from NadirSatelliteInstrument. This
    represents an AMSU-A scanner aboard a Metop satellite.

    Parameters
    ------------
        name: str
            Name of the nadir satellite, drawn from awsgnssroutils.collocation.core.spacetrack.Satellites[:]['name']
        eumetsat_access: collocation.core.eumetsat.EUMETSATDataStore
            An object that interfaces with the EUMETSAT Data Store. 
        spacetrack: collocation.core.spacetrack.Spacetrack
            Portal to the Spacetrack TLE data

    Attributes
    ------------
        name: str
            Name of the nadir satellite, drawn from awsgnssroutils.collocation.core.spacetrack.Satellites[:]['name']
        spacetrack_satellite: instance of spacetrack.SpacetrackSatellite
            Define the satellite, for access to TLEs
        xi: float
            MWR instrument maximum scan angle [radians]
        data: string
            Filepath to sounding data
        time_between_scans: float
            Time between cross-track scans [seconds]
        scan_points_per_line: int
            Number of individual footprints per cross-track scan
        scan_angle_spacing: float
            Angle between scan footprints [radians]
        eumetsat_access: collocation.core.eumetsat.EUMETSATDataStore
            An object that interfaces with the EUMETSAT Data Store. 
        spacetrack_satellite: collocation.core.spacetrack.SpacetrackSatellite
            TLE data for the satellite
    """

    def __init__(self, name, eumetsat_access, spacetrack=None ):
        """Constructor for MetopAMSUA class."""

        self.instrument_name = instrument

        if name not in valid_satellites: 
            print( f'No {instrument} on satellite {name}. Valid satellites are ' + ", ".join( valid_satellites ) )
            self.status = "fail"

        else: 
            max_scan_angle = 48.285             # degrees
            time_between_scans = 8.0            # seconds
            scan_points_per_line = 30           # footprints in a scan
            scan_angle_spacing = 3.33           # degrees

            super().__init__( name, max_scan_angle, time_between_scans,
                    scan_points_per_line, scan_angle_spacing, spacetrack=spacetrack )

            self.eumetsat_access = eumetsat_access
            self.status = "success"

        pass

    def populate( self, timerange ): 
        """Populate (download) all Metop AMSU-A data for this satellite in a 
        time range defined by timerange. timerange can be a 2-tuple/list of 
        instances of timestandards.Time or datetime.datetime with the 
        understanding that the latter is defined as UTC times."""

        self.eumetsat_access.populate_metop_amsua( self.satellite_name, timerange )
        return

    def get_geolocations_from_file( self, filename ):
        """Get geolocation information from a single input file filename."""

        #  Define time standard for timing information in EUMETSAT Data Store 
        #  AMSU-A level 1a files. Possibilities are "utc", "tai", "gps". 

        #  Open file. 

        d = Dataset( filename, 'r' )

        #  Get dimensions. 

        nx = d.dimensions['x'].size
        ny = d.dimensions['y'].size

        #  Get longitudes and latitudes. Convert to radians. 

        latitudes = np.deg2rad( d.variables['lat'][:] )
        longitudes = np.deg2rad( d.variables['lon'][:] )

        #  Get start and stop time of scans in file. 

        dt = datetime.strptime( d.getncattr("start_sensing_time"), "%Y%m%dT%H%M%S.%fZ" )
        cal = datetime( dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second )
        args = { eumetsat_time_convention: cal }
        start_sensing_time = Time( **{ eumetsat_time_convention: cal } ) + dt.microsecond * 1.0e-6

        dt = datetime.strptime( d.getncattr("stop_sensing_time"), "%Y%m%dT%H%M%S.%fZ" )
        cal = datetime( dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second )
        stop_sensing_time = Time( **{ eumetsat_time_convention: cal } ) + dt.microsecond * 1.0e-6

        d.close()

        seconds_between_scans = int( stop_sensing_time - start_sensing_time + 0.5 ) / ny
        break_between_scans = seconds_between_scans * ny - ( stop_sensing_time - start_sensing_time ) 

        mid_times = [ start_sensing_time + (iy+0.5) * seconds_between_scans for iy in range(ny) ] 

        return { 'longitudes': longitudes, 'latitudes': latitudes, 'mid_times': mid_times }

    def get_geolocations( self, timerange ):
        """Load AMSU-A data from a Metop satellite as obtained from the EUMETSAT Data Store
        using collocation.core.eumetsat.EUMETSATDataStore.satellite.populate_metop_amsua. The 
        timerange is a two-element tuple/list containing instances of timestandards.Time 
        that prescribe the time range over which to obtain AMSU-A geolocations. An instance 
        of class ScanMetadata containing the footprint geolocations is returned upon successful 
        completion."""

        #  The EUMETSAT data is stored by orbit. In order to find all relevant AMSU-A 
        #  soundings, be sure to subtract one orbital period from the first time and 
        #  add one orbital period to the last time. 

        data_files = self.eumetsat_access.get_metop_amsua_paths( self.satellite_name, timerange )
        gps0 = Time(gps=0)
        dt = ( timerange[0]-gps0, timerange[1]-gps0 )

        #  Loop over data files. Keep geolocations only for soundings within the timerange. 
        #  Initialize geolocation variables. 

        longitudes, latitudes, mid_times, scan_indices, file_indices = [], [], [], [], [] 

        for ifile, data_file in enumerate(data_files): 

            #  Read all geolocation information from data_file. 

            ret = self.get_geolocations_from_file( data_file )

            #  Convert mid_times to an np.ndarray of GPS times.  Find times for soundings 
            #  within the prescribed timerange. 

            file_gps_times = np.array( [ t-gps0 for t in ret['mid_times'] ] )
            good = np.argwhere( np.logical_and( dt[0] <= file_gps_times, file_gps_times < dt[1] ) ).flatten()

            #  Keep only those soundings within the prescribed timerange. 

            if good.size > 0: 
                longitudes += [ ret['longitudes'][iy,:] for iy in good ]
                latitudes += [ ret['latitudes'][iy,:] for iy in good ]
                mid_times += [ Time(gps=file_gps_times[iy]) for iy in good ]
                scan_indices += list( good )
                file_indices += [ifile] * good.size

        #  Convert to ndarrays. 

        longitudes = np.array( longitudes )
        latitudes = np.array( latitudes )

        #  Generate output object. 

        ret = ScanMetadata( self.get_data, longitudes, latitudes, mid_times, data_files, file_indices, scan_indices )

        return ret

    def get_data( self, file, scan_index, footprint_index, 
                 longitude=None, latitude=None, time=None ): 
        """A function which returns nadir-scan satellite data for requested scan and 
        footprint indices within the file *file*. The function itself must be a 
        Method to fetch data for a nadir-scan satellite instrument corresponding to 
        a scalar integer indicating the scan number [0:nscans] and a scalar integer 
        indicating the footprint number [0:nfootprints].

        It returns an xarray.Dataset according taken from file *file* and the 
        data location within the file should correspond to scans 
        data[scan_index,footprint_index].

        Longitude (degrees), latitude (degrees), and time (timestandards.Time) will 
        be included in the returned xarray.Dataset if provided."""

        #  Check input. 

        integer_types = [ int, np.int8, np.int16, np.int32, np.int64 ]

        if not isinstance(file,str): 
            raise metop_amsua_error( "InvalidArgument", "file argument must be type str" )

        if not any( [ isinstance(scan_index,t) for t in integer_types ] ): 
            raise metop_amsua_error( "InvalidArgument", "scan_index argument must be an integer type" )

        if not any( [ isinstance(footprint_index,t) for t in integer_types ] ): 
            raise metop_amsua_error( "InvalidArgument", "footprint_index argument must be an integer type" )

        #  Open data file. 

        global open_data_file

        if open_data_file['pointer'] is not None: 
            if open_data_file['path'] == file: 
                #  Data file already open. 
                d = open_data_file['pointer']
            else: 
                #  Close previously opened file. 
                open_data_file['pointer'].close()
                open_data_file['path'] = None
                open_data_file['pointer'] = None

        if open_data_file['path'] is None: 
            #  Open new file. 
            open_data_file['path'] = file
            open_data_file['pointer'] = Dataset( file, 'r' )
            d = open_data_file['pointer']

        dim_nscans, dim_nfootprints = d.dimensions['y'].size, d.dimensions['x'].size
        nchannels = 15

        #  Get data values. 

        data = np.array( [ d.variables["channel_"+str(ichannel+1) ][scan_index,footprint_index] \
                    for ichannel in range(nchannels) ] )
        data = np.ma.masked_where( data <= 0, data )

        zenith = d.variables['satellite_zenith'][scan_index,footprint_index]

        #  Convert to np.ndarrays. 

        radiance_dataarray = masked_dataarray( data, fill_value, 
                dims=("channel",), 
                coords = { 'channel': np.arange(nchannels,dtype=np.int32)+1 } )
        radiance_dataarray.attrs.update( {
            'description': "Microwave radiance from Metop AMSU-A instrument", 
            'units': "mW m**-2 (cm**-1)**-1 steradian**-1" } )

        zenith_dataarray = masked_dataarray( zenith, fill_value )
        zenith_dataarray.attrs.update( {
            'description': "Zenith angle from surface to satellite", 
            'units': "degrees" } )

        ds_dict = { 
            'data': radiance_dataarray, 
            'zenith': zenith_dataarray } 

        ds_attrs_dict = { 
            'satellite': self.satellite_name, 
            'instrument': self.instrument_name, 
            'data_file_path': file, 
            'scan_index': np.int16( scan_index ), 
            'footprint_index': np.int16( footprint_index ) } 

        if longitude is not None: 
            longitude_dataarray = xarray.DataArray( longitude )
            longitude_dataarray.attrs.update( { 
                'description': "Longitude of sounding, eastward", 
                'units': "degrees" } )
            ds_dict.update( { 'longitude': longitude_dataarray } )

        if latitude is not None: 
            latitude_dataarray = xarray.DataArray( latitude )
            latitude_dataarray.attrs.update( { 
                'description': "Latitude of sounding, northward", 
                'units': "degrees" } )
            ds_dict.update( { 'latitude': latitude_dataarray } )

        if time is not None: 
            ds_dict.update( { 'time': time.calendar("utc").isoformat(timespec="milliseconds")+"Z" } )

        ds = xarray.Dataset( ds_dict )
        ds.attrs.update( ds_attrs_dict )

        return ds

