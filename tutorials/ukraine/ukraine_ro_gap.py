from awsgnssroutils.database import RODatabaseClient, setdefaults

import numpy as np
import xarray as xr
import json

import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import cartopy.crs as ccrs
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER 
from cartopy.feature import BORDERS 

import os
from datetime import datetime, timedelta
from time import time


#  Pyplot settings. 

axeslinewidth = 0.5
plt.rcParams.update( {
  'font.family': "Times New Roman", 
  'font.size': 8, 
  'font.weight': "normal", 
  'text.usetex': True, 
  'xtick.major.width': axeslinewidth, 
  'xtick.minor.width': axeslinewidth, 
  'ytick.major.width': axeslinewidth, 
  'ytick.minor.width': axeslinewidth, 
  'axes.linewidth': axeslinewidth } )

#  Physical constants. 

Re = 6378.0e3           # Equatorial radius of the Earth

#  RO database portal. 

db = RODatabaseClient()



def compute_ukraine_counts( monthrange=("2020-01","2024-04"), analysisfile="ukraine_counts.json" ): 

    time1 = time()

    longituderange = ( 20, 55 )
    latituderange = ( 40, 65 )

    ukraine_counts = {}
    control_counts = {}
    comment = "The Ukraine counts are the number of RO soundings in the region " + \
            "defined by the longituderange and the latituderange. The control counts " + \
            "are the number of RO soundings in the longitude band defined only by " + \
            "the latituderange."

    month = monthrange[0]

    while month <= monthrange[1]: 

        #  Define the datetimerange and query the database. 

        firstday = month + "-01"
        dayp31 = datetime.fromisoformat( firstday ) + timedelta(days=31)
        lastday = datetime( year=dayp31.year, month=dayp31.month, day=1 ).strftime( "%Y-%m-%d" )
        occs = db.query( datetimerange=(firstday,lastday), silent=True )

        #  Control and regional (Ukraine) occs. 

        control_occs = occs.filter( latituderange=latituderange )
        ukraine_occs = control_occs.filter( longituderange=longituderange )
        missions = occs.info( "mission" )

        countdict = { mission: control_occs.filter( missions=mission ).size for mission in missions }
        control_counts.update( { month: countdict } )

        countdict = { mission: ukraine_occs.filter( missions=mission ).size for mission in missions }
        ukraine_counts.update( { month: countdict } )

        print( "{:}: {:}".format( month, countdict ) )

        #  Next month. 

        month = dayp31.strftime("%Y-%m")

    #  Save results. 

    ret = { 
           'ukraine': ukraine_counts, 
           'control': control_counts, 
           'longituderange': longituderange, 
           'latituderange': latituderange, 
           'comment': comment, 
           }

    print( f'Writing to {analysisfile}.' )

    with open( analysisfile, 'w' ) as f: 
        json.dump( ret, f, indent="  " )

    time2 = time()
    print( f'Elapsed time = {int(time2-time1)} seconds.' )

    return 


def plot_ukraine_counts( analysisfile, output ): 

    with open( analysisfile, 'r' ) as f: 
        d = json.load( f )

    #  Organize data, by mission and month. 

    missions = []
    months = []

    for key, val in d['ukraine'].items(): 
        months.append( key )
        missions += list( val.keys() )

    months = sorted( months )
    missions = sorted( set( missions ) )

    #  Develop timeseries. 

    data = { mission: [] for mission in missions }
    control = { mission: [] for mission in missions }

    for month in months: 
        for mission in missions: 

            if mission in d['ukraine'][month].keys(): 
                data[mission].append( d['ukraine'][month][mission] )
            else: 
                data[mission].append( 0 )

            if mission in d['control'][month].keys(): 
                control[mission].append( d['control'][month][mission] )
            else: 
                control[mission].append( 0 )

    for mission in missions: 
        data[mission] = np.array( data[mission] )
        control[mission] = np.array( control[mission] )

    #  Table of mission names. 

    mission_names = [ 
        { 'aws': "cosmic1", 'presentation': "COSMIC-1" }, 
        { 'aws': "champ", 'presentation': "CHAMP" }, 
        { 'aws': "cosmic2", 'presentation': "COSMIC-2" }, 
        { 'aws': "grace", 'presentation': "GRACE" }, 
        { 'aws': "kompsat5", 'presentation': "KompSat-5" }, 
        { 'aws': "metop", 'presentation': "Metop" }, 
        { 'aws': "tsx", 'presentation': "TerraSAR-X" }, 
        { 'aws': "tdx", 'presentation': "TanDEM-X" }, 
        { 'aws': "paz", 'presentation': "rohp-PAZ" }, 
        { 'aws': "spire", 'presentation': "Spire" }, 
        { 'aws': "planetiq", 'presentation': "PlanetIQ" }, 
        { 'aws': "sacc", 'presentation': "SAC-C" }, 
        { 'aws': "cnofs", 'presentation': "C/NOFS" }, 
        { 'aws': "geoopt", 'presentation': "GeoOptics" }, 
    ]

    for mission in missions: 
        if mission not in [ m['aws'] for m in mission_names ]: 
            print( f'AWS mission {mission} not in mission_names table' )
            return

    mnames = []
    for mission in missions: 
        mname = [ m['presentation'] for m in mission_names if m['aws']==mission ][0]
        mnames.append( mname )

    #  Time array. 

    xtimes = []
    for month in months: 
        year, mmonth = month.split("-")
        xtimes.append( int(year) + (int(mmonth)-1)/12 )
    xtimes = np.array( xtimes )

    #  Define colors. 

    cmap = plt.get_cmap( "turbo" )
    colors = [ cmap((i+0.5)/len(missions)) for i in range(len(missions)) ]

    #  Plot. 

    fig = plt.figure( figsize=(6,2) )

    #  Yield vs. time by mission. 

    ax = fig.add_axes( [0.09,0.18,0.70,0.80] )

    x = np.deg2rad( d['longituderange'][1] - d['longituderange'][0] )
    theoretical_yield = ( np.arctan2( -np.sin(x), -np.cos(x) ) + np.pi ) / ( 2 * np.pi )

    xdata = np.zeros( (len(missions),len(months)), np.float32 )
    mask = np.zeros( (len(missions),len(months)), np.int8 )

    cutoff = 10
    for imission, mission in enumerate(missions): 
        i = np.argwhere( control[mission] > 0 ).flatten()
        if i.size > cutoff: 
            xdata[imission,i] = data[mission][i] / control[mission][i]
        mask[imission,:] = ( control[mission] <= cutoff )

    xdata = np.ma.masked_where( mask, xdata )

    #  Apply 3-month smoothing. 

    # kernel = np.ones(3) / 3.0
    # for imission in range(len(missions)): 
        # xdata[imission,:] = np.convolve( xdata[imission,:], kernel, mode="same" )

    #  Set up plot. 

    ylim = [ 0, 1.4 ]
    ax.set_xlim( xtimes[0], xtimes[-1]+1/12 )
    ax.set_ylim( *ylim )
    xticks = np.arange( 2007, 2024.1, 1 ).astype( np.int32 )
    ax.set_xticks( xticks )
    xticklabels = []
    for xtick in xticks: 
        if xtick % 2 == 1: 
            xticklabels.append( f'{xtick:4d}' )
        else: 
            xticklabels.append( "" )
    ax.set_xticklabels( xticklabels )
    ax.xaxis.set_minor_locator( MultipleLocator(0.25) )
    ax.set_yticks( np.arange(0,1.401,0.5) )
    ax.yaxis.set_minor_locator( MultipleLocator(0.1) )
    ax.tick_params(axis='x', labelrotation=-70)
    ax.set_ylabel( "Yield" )

    ax.plot( [xtimes.min()-1,xtimes.max()+1], np.array([1,1]), ls="--", lw=0.5, color="#808080" )
    for imission in range(len(missions)): 
        ax.plot( xtimes, xdata[imission,:]/theoretical_yield, color=colors[imission], label=mnames[imission], lw=0.8 )

    #  Legend. 

    ax.legend( loc="center left", bbox_to_anchor=(1.04,0.50), fontsize="x-small" )

    #  Write to output. 

    print( f'Creating {output}.' )
    fmt = output.split(".")[-1]
    fig.savefig( output, format=fmt )
    
    return


def yield_by_mission( analysisfile, monthrange=("2023-01","2023-12"), 
                     outputtable="yield_by_mission.csv" ): 
    """Compute the yield, number of soundings over Ukraine divided by the number 
    of soundings in the corresponding longitude band, by RO mission within a 
    prescribed range of months (monthrange). The output of compute_ukraine_counts 
    is input. A CSV table is written to output (outputtable)."""

    print( f'Reading {analysisfile}.' )

    with open( analysisfile, 'r' ) as f: 
        d = json.load( f )

    #  Theoretical yield. 

    x = np.deg2rad( d['longituderange'][1] - d['longituderange'][0] )
    theoretical_yield = ( np.arctan2( -np.sin(x), -np.cos(x) ) + np.pi ) / ( 2 * np.pi )

    #  Organize data, by mission and month. 

    missions = set()
    months = []

    for month, val in d['ukraine'].items(): 
        months.append( month )
        missions = missions.union( set( val.keys() ) )

    months = sorted( months )
    missions = sorted( list( missions ) )

    #  Develop timeseries. 

    data = { mission: [] for mission in missions }
    control = { mission: [] for mission in missions }

    for month in months: 
        for mission in missions: 

            if mission in d['ukraine'][month].keys(): 
                data[mission].append( d['ukraine'][month][mission] )
            else: 
                data[mission].append( 0 )

            if mission in d['control'][month].keys(): 
                control[mission].append( d['control'][month][mission] )
            else: 
                control[mission].append( 0 )

    for mission in missions: 
        data[mission] = np.array( data[mission] )
        control[mission] = np.array( control[mission] )

    #  Average yield for each mission in a specified time range. First 
    #  get the indices of the months to include in the counting. The 
    #  month range in defined by monthrange, each element YYYY-MM. 

    print( f'Computing yield for months {monthrange[0]} through {monthrange[1]}.' )

    imonths = np.array( [ imonth for imonth, month in enumerate(months) \
            if month >= monthrange[0] and month <= monthrange[1] ] )

    #  Do the counting by mission; counting both those occultations 
    #  over the Ukraine region (total_data) and those in the control 
    #  region. 

    total_data = { mission: data[mission][imonths].sum() for mission in missions }
    total_control = { mission: control[mission][imonths].sum() for mission in missions }

    #  Calculate the yield and the uncertainty in the yield by mission. 

    myield = { mission: total_data[mission] / total_control[mission] for mission in missions if total_control[mission] > 100 }
    myield_uncertainty = { mission: np.sqrt( total_data[mission] ) / total_control[mission] for mission in missions if total_control[mission] > 100 }

    #  Write to output. 

    print( f'Writing to {outputtable}.' )

    all_data = np.array( [ val for mission, val in total_data.items() ] ).sum()
    all_control = np.array( [ val for mission, val in total_control.items() ] ).sum()
    all_yield = all_data / all_control
    all_uncertainty = np.sqrt( all_data ) / all_control

    with open( outputtable, 'w' ) as f: 
        f.write( f'Theoretical yield = {theoretical_yield:7.5f}\n\n' )
        f.write( "Mission    Ukraine Control   yield  uncertainty\n" )
        f.write( "========   ======= =======  ======= ===========\n" )
        for mission in myield.keys() : 
            f.write( f"{mission:<10s}{total_data[mission]:>8d}{total_control[mission]:>8d}" )
            f.write( f"  {myield[mission]:7.5f}   {myield_uncertainty[mission]:7.5f}\n" )
        f.write( f"all       {all_data:>8d}{all_control:>8d}" )
        f.write( f"  {all_yield:7.5f}   {all_uncertainty:7.5f}\n" )


################################################################################


def compute_sounding_density( monthrange=("2023-01","2023-12"), outputfile="sounding_density.nc" ): 
    """Compute an RO sounding density map in the vicinity of Ukraine 
    over a time period defined by monthrange. Output is saved to the 
    NetCDF file outputfile."""

    time1 = time()

    #  Define map longitude-latitude bounds and bin size. 

    longitude_bounds = np.arange( -20, 100.01, 4.0 )
    latitude_bounds = np.arange( 30, 80.01, 3.0 )

    nx, ny = longitude_bounds.size-1, latitude_bounds.size-1
    dx, dy = longitude_bounds[1] - longitude_bounds[0], latitude_bounds[1] - latitude_bounds[0]
    sounding_density = np.zeros( (nx,ny), np.float32 )

    #  Settings. 

    subset = { 
            'availablefiletypes': "ucar_refractivityRetrieval" , 
            'longituderange': ( longitude_bounds[0], longitude_bounds[-1] ), 
            'latituderange': ( latitude_bounds[0], latitude_bounds[-1] )
        }

    month = monthrange[0]
    while month <= monthrange[1]: 

        print( f'Processing {month}' )

        #  Establish datetimerange. 

        firsttime = datetime.fromisoformat( month+"-01" )
        dayp31 = firsttime + timedelta(days=31)
        lasttime = datetime( year=dayp31.year, month=dayp31.month, day=1 )
        datetimerange = ( firsttime.strftime("%Y-%m-%d"), lasttime.strftime("%Y-%m-%d") )

        #  Query database. 

        occs = db.query( datetimerange=datetimerange, **subset, silent=True )
        lons, lats = occs.values("longitude"), occs.values("latitude")
        
        #  Compute binning information. Justify longitudes. 

        dlons = lons - longitude_bounds[0]
        dlats = lats - latitude_bounds[0]

        x = np.deg2rad( dlons )
        dlons = np.rad2deg( np.arctan2( np.sin(x), np.cos(x) ) )

        ixs = ( dlons / dx ).astype(np.int32)
        iys = ( dlats / dy ).astype(np.int32)

        #  Do the binning. 

        for ix,iy in zip(ixs,iys): 
            if ix==nx: ix=nx-1
            if iy==ny: iy=ny-1
            sounding_density[ix,iy] += 1

        #  Next month. 

        month = dayp31.strftime("%Y-%m")

    #  Convert to sounding density. 

    sinlats = np.sin( np.deg2rad( latitude_bounds ) )
    dsinlats = np.abs( sinlats[1:] - sinlats[:-1] )

    t0 = datetime.fromisoformat( monthrange[0]+"-01" )
    tt = datetime.fromisoformat( monthrange[1]+"-01" ) + timedelta(days=31)
    t1 = datetime( year=tt.year, month=tt.month, day=1 )
    ndays = ( t1 - t0 ).days

    for iy, dsinlat in enumerate( dsinlats ): 
        conv = ( 500.0e3 )**2 * ( 30.0 ) / ( ndays * np.deg2rad(dx) * dsinlat * Re**2 )
        sounding_density[:,iy] *= conv

    #  Write results to output. 

    sounding_density_da = xr.DataArray( sounding_density, dims=["lon","lat"] )
    sounding_density_da.attrs.update( { 
            'description': "Sounding density", 
            'units': "(500 km)**-2 (30 days)**-1"
        } )

    lonbounds_da = xr.DataArray( longitude_bounds, dims=["lon_bound"] )
    lonbounds_da.attrs.update( { 
            'description': "Longitude edges of sounding density histogram cells", 
            'units': "degrees east"
        } )

    latbounds_da = xr.DataArray( latitude_bounds, dims=["lat_bound"] )
    latbounds_da.attrs.update( { 
            'description': "Latitude edges of sounding density histogram cells", 
            'units': "degrees north"
        } )

    ds = xr.Dataset( data_vars={
        'sounding_density': sounding_density_da, 
        'longitude_bounds': lonbounds_da, 
        'latitude_bounds': latbounds_da }, 
        attrs={ 'monthrange': monthrange } )

    #  Write to output. 

    print( f'Writing output to {outputfile}.' )
    ds.to_netcdf( outputfile )

    #  Done. 

    time2 = time()
    print( f'Elapsed time = {int(time2-time1)} seconds' )

    return


def plot_sounding_density( analysisfile, output ): 

    #  Read in analysis dataset. 

    d = xr.open_dataset( analysisfile )
    lon_bounds = d.longitude_bounds[:]
    lat_bounds = d.latitude_bounds[:]
    lons = 0.5 * ( lon_bounds[1:] + lon_bounds[:-1] )
    lats = 0.5 * ( lat_bounds[1:] + lat_bounds[:-1] )

    #  Cartopy projection. 

    proj = ccrs.PlateCarree

    #  Create figure, axes. 

    fig = plt.figure( figsize=(5,2) )
    ax = fig.add_axes( [0.07,0.01,0.78,0.98], projection=proj() )
    ax.set_extent( [ lons.min(), lons.max(), lats.min(), lats.max() ], crs=proj() )

    #  Contour plot. 

    clevels = np.arange( 0.0, 60.001, 0.5 )
    cticks = np.arange( clevels.min(), clevels.max()+0.001, 20.0 )

    cax = ax.contourf( lons, lats, d.sounding_density[:].T, 
                levels=clevels, cmap="hot_r", extend="max" )
    # ax.contour( lons, lats, d.sounding_density[:].T, levels=cticks, linewidths=1.0, colors="#000000" )

    #  Add coastlines and national borders. 

    color = "#B0B0FF"
    ax.coastlines( resolution="50m", linewidth=0.6, color=color )
    ax.add_feature( BORDERS, linewidth=0.4, color=color )

    #  Add gridlines. 

    gl = ax.gridlines( crs=proj(), draw_labels=True, color=color, linewidth=0.2, alpha=0.5 )
    gl.top_labels = False
    gl.right_labels = False
    gl.xformatter = LONGITUDE_FORMATTER
    gl.xlabel_style = { 'size': 8 }
    gl.yformatter = LATITUDE_FORMATTER
    gl.ylabel_style = { 'size': 8 }

    #  Add colorbar. 

    cbar = fig.add_axes( [0.88,0.10,0.02,0.80] )
    fig.colorbar( cax, cbar, orientation="vertical", ticks=cticks, label="Sounding Density" )

    print( "The sounding density is in units of (500 km)**-2 (30 days)**-1." )

    #  Write to output. 

    print( f'Creating {output}.' )

    fmt = output.split(".")[-1]
    fig.savefig( output, format=fmt )

    return


if __name__ == "__main__": 

    #  Set awsgnssroutils defaults if necessary. 

    if False: 
        HOME = os.path.expanduser( "~" )
        metadata_root = os.path.join( HOME, "Data", "awsro", "metadata" )
        data_root = os.path.join( HOME, "Data", "awsro", "data" )
        setdefaults( metadata_root=metadata_root, data_root=data_root )

    #  Sounding density analysis/figure. 

    analysisfile = "ukraine_sounding_density.2023.nc" 
    compute_sounding_density( monthrange=("2023-01","2023-12"), outputfile=analysisfile )
    plot_sounding_density( analysisfile, "ukraine_sounding_density.2023.eps" )

    #  Monthly timeseries of yield. 

    analysisfile = "ukraine_counts.json"
    compute_ukraine_counts( monthrange=("2007-01","2024-04"), analysisfile=analysisfile )
    plot_ukraine_counts( analysisfile, "ukraine_counts.eps" )
    yield_by_mission( analysisfile )

    pass

