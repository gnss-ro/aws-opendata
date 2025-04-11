##################################################################
#  NOAA module for rotation-collocation utility. 
##################################################################

# Convert ATMS NOAA HDF5 granules into AER's standard radiance format.
#
# Usage example:
# python convert_satms_hdf5torad.py SATMS_j02_d20250410_t0000298_e0001014_b12509_c20250410004550539000_oeac_ops.h5 GATMO_j02_d20250410_t0000298_e0001014_b12509_c20250410004550980000_oeac_ops.h5 --outfile SATMS_j02_d20250410_t0000298_e0001014_b12509_c20250410004550539000_oeac_ops_rad.nc --log test_convert.log -vv
#
# Developed by: AER, Inc., Apr. 2025
# Copyright: AER, Inc., 2025
##################################################################

import numpy as np
from netCDF4 import Dataset
from .timestandards import Time

def ATMSsdrReader(sdrfile, geofile ):
    """Read SDR and GEO data files in HDF5 format. They correspond 
    to SATMS and GATMO files in the AWS Registry of Open Data. Data 
    are loaded into an output dictionary."""

    #  Based on code convert_satms_hdf5torad.py composed by Pan Liang
    #  (AER, pan.liang@janusresearch.us). 

    sdr = {}

    with Dataset(sdrfile, 'r') as ncf:

        # ncf.set_auto_mask(False)

        grpSDR = ncf['/All_Data/ATMS-SDR_All']

        # Dimensions

        natrack = grpSDR.dimensions['phony_dim_0'].size
        nxtrack = grpSDR.dimensions['phony_dim_1'].size
        nchannels = grpSDR.dimensions['phony_dim_2'].size

        sdr.update( { 'natrack':natrack, 'nxtrack':nxtrack, 'nchannels':nchannels } )

        # Global attributes

        sdr.update( { 
                     'Distributor': ncf.Distributor, 
                     'Mission_Name': ncf.Mission_Name, 
                     'N_GEO_Ref': ncf.N_GEO_Ref, 
                     'N_HDF_Creation_Date': ncf.N_HDF_Creation_Date, 
                     'N_HDF_Creation_Time': ncf.N_HDF_Creation_Time, 
                     'Platform_Short_Name': ncf.Platform_Short_Name, 
                     'Input_SDR_Filename': os.path.basename(sdrfile) 
                    } )

        # Read SDR variables

        grpSDR = ncf['/All_Data/ATMS-SDR_All']

        # Tb dimension (atrack, xtrack, channel)

        tbshort =  grpSDR.variables['BrightnessTemperature'][:].

        # Tb scale+offset factors dimension 2

        tbfactors = grpSDR.variables['BrightnessTemperatureFactors'][:]

        # Apply the scale+offset factors

        tb = np.full_like(tbshort, fill_value=fill_value, dtype=np.float32)
        tb[:] = tbshort[:]*tbfactors[0] + tbfactors[1]

        # Acquisition time for each FOV, dimension (atrack, xtrack)
        # Microsecond since IET(1/1/1958), convert to seconds by 1e-6

        BeamTime = grpSDR.variables['BeamTime'][:]*1e-6

        # Generate the epoch for the times. 

        epoch = Time( utc=Calendar(year=1958, month=1, day=1) )

        # NEdt diemension (atrack, channel)

        NEdTCold = grpSDR.variables['NEdTCold'][:]
        NEdTWarm = grpSDR.variables['NEdTWarm'][:]

        # Add output variables. 

        sdr.update( { 
                     'tb': tb, 
                     'epoch': epoch, 
                     'BeamTime': BeamTime, 
                     'NEdTCold': NEdTCold, 
                     'NEdTWarm': NEdTWarm
                    } )

    # Read GEO variables

    with Dataset(geofile,'r') as ncf: 

        grpGEO = ncf['/All_Data/ATMS-SDR-GEO_All']

        # Uncomment the next line to print out all the parameters
        # print(grpGEO)

        # Variables for each FOV, dimension (atrack, xtrack)

        Latitude = grpGEO.variables['Latitude'][:]
        Longitude = grpGEO.variables['Longitude'][:]
        SatelliteAzimuthAngle = grpGEO.variables['SatelliteAzimuthAngle'][:]
        SatelliteZenithAngle = grpGEO.variables['SatelliteZenithAngle'][:]
        SolarAzimuthAngle = grpGEO.variables['SolarAzimuthAngle'][:]
        SolarZenithAngle = grpGEO.variables['SolarZenithAngle'][:]

        # Add output variables. 

        sdr.update( { 
                     'Latitude': Latitude, 
                     'Longitude': Longitude, 
                     'SatelliteAzimuthAngle': SatelliteAzimuthAngle, 
                     'SatelliteZenithAngle': SatelliteZenithAngle, 
                     'SolarAzimuthAngle': SolarAzimuthAngle, 
                     'SolarZenithAngle': SolarZenithAngle
                    } )

    return sdr

