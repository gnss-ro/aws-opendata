"""Tutorial Demonstration Code

Code examples for consulting the DynamoDB database for GNSS radio occultation 
data in the AWS Open Data Registry and performing some basic data analysis, 
which, in this case, is a processing center inter-comparison of retrieved 
RO variables bending angle, refractivity, temperature and pressure for 
occultations as processed by two different retrieval centers.

The instructive/tutorial methods are...

The prerequisite nonstandard python modules that must be installed are 
  * netCDF4
  * numpy
  * scipy
  * matplotlib
  * cartopy
  * boto3

Before any of this code is implemented, it is first necessary to manifest 
the DynamoDB database using the utilities in ???. The user only need 
modify a few parameters in the "IMPORTANT: Configuration" section below 
in order for the code to function. 


Version: 1.0
Author: Stephen Leroy (sleroy@aer.com)
Date: May 16, 2022

"""

##################################################
#  IMPORTANT: Configuration
##################################################

#  Define the name of the AWS profile to be used for authentication 
#  purposes. The authentication will be needed to access the DynamoDB 
#  database table. 

aws_profile = "aernasaprod"

#  Define the AWS region where the gnss-ro-data Open Data Registry 
#  S3 bucket is hosted *and* where the DynamoDB database is manifested. 

aws_region = "us-east-1"

#  Define the name of the DynamoDB data base table. 

dynamodb_table = "gnss-ro-data-staging"

##################################################
#  Configuration complete. 
##################################################


#  Import python standard modules. 

import os
import sys
import json
from datetime import datetime, timedelta

#  Import installed modules. 

from netCDF4 import Dataset
import numpy as np
from scipy.interpolate import interp1d
import cartopy
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import boto3
from boto3.dynamodb.conditions import Key, Attr

#  The RO missions in the data archive with pointers to the names 
#  of the satellites in each RO mission. This dictionary is a summary 
#  of Table 5 in the Data-Description.pdf document. 

valid_missions = {
    'gpsmet': [ "gpsmet", "gpsmetas" ],
    'grace': [ "gracea", "graceb" ],
    'sacc': [ "sacc" ],
    'champ': [ "champ" ],
    'cosmic1': [ "cosmic1c{:1d}".format(i) for i in range(1,7) ],
    'tsx': [ "tsx" ],
    'tdx': [ "tdx" ],
    'cnofs': [ "cnofs" ],
    'metop': [ "metopa", "metopb", "metopc" ],
    'kompsat5': [ "kompsat5" ],
    'paz': [ "paz" ],
    'cosmic2': [ "cosmic2e{:1d}".format(i) for i in range(1,7) ]
    }

#  Matplotlib default settings. 

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

#  GNSS constellations used as RO transmitters for this analysis. This must 
#  include "R" in addition to "G" when cosmic2 data or data from the 
#  commercial RO data providers become available. 

# valid_constellations = [ "G", "R" ]
valid_constellations = [ "G" ]

#  Intermediate files: RO data count by mission and mission color table. 

alldata_json_file = "occultation_count_by_mission.json"
colors_json_file = "color_table_by_mission.json"

#  Define month labeling strings. 

monthstrings = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()

#  Logger. 

import logging 
LOGGER = logging.getLogger( __name__ )

#  Physical constants. 

gravity = 9.80665           # WMO standard gravity [J/kg/m]
k1 = 77.6e-2                # First term in refractivity equation [N-units K/Pa]

################################################################################
#  Useful utilities. 
################################################################################

def latlabels( lats ): 
    """Create a list of tick labels for latitutes based on a list/array of 
    tick values."""

    ylabels = []
    for lat in lats: 
        if lat < 0: 
            ylabel = "{:}$^\circ$S".format( np.abs( lat ) )
        elif lat > 0: 
            ylabel = "{:}$^\circ$N".format( np.abs( lat ) )
        else: 
            ylabel = "Eq"
        ylabels.append( ylabel )
    
    return ylabels

def get_transmitters(): 
    """Return a list of possible transmitter names as 3-character PRNs"""

    transmitters = []
    for constellation in valid_constellations: 
        if constellation == "G": 
            nprns = 32
        elif constellation == "R": 
            nprns = 24
        elif constellation == "E": 
            nprns = 36
        elif constellation == "C": 
            nprns = 61
        transmitters += [ f"{constellation}{prn:02d}" for prn in range(1,nprns+1) ]

    transmitters.sort()

    return transmitters

def masked_interpolate( x_in, y_in, x_out )
    """Performed a masked cubic spline interpolation. The output array is masked 
    where an interpolation is impossible."""

    #  Strip out masked values in the input arrays. 

    i = np.argwhere( np.logical_not( np.logical_or( x_in.ma, y_in.ma ) ) ).squeeze()
    xi, yi = x_in[i], y_in[i]

    #  Form the interpolator. 

    fill_value = -1.332e21
    finterp = interp1d( xi, yi, kind='cubic', bounds_error=False, fill_value=fill_value )

    #  Do the interpolation. 

    y = finterp( x_out )

    #  Mask the output array. 

    y_out = np.ma.masked_where( y == fill_value, y )

    #  Done. 

    return y_out

def radius_of_curvature( ae, ap, lond, latd, coc ): 
    """Compute the radius of curvature [m] given the equatorial radius (ae) and 
    polar radius (ap) of the Earth, the geodetic longitude (lond) and latitude 
    (latd) of the sounding, and the center of curvature (coc[3], meters) of the 
    sounding."""

    #  Compute the geocentric longitude and latitude in radians. 

    lonc = np.deg2rad( lond )
    latc = np.arctan2( polarRadius**2 * np.sin( np.deg2rad( latd ) ), 
            equatorialRadius**2 * np.cos( np.deg2rad( latd ) ) ) 

    #  Radius of the Earth at this latitude. 

    re = 1.0 / np.sqrt( np.cos( latc )**2 / ae**2 + np.sin( latc )**2 / ap**2 )

    #  Compute the ECEF position of the surface location. 

    s = re * np.array( [ np.cos(lond) * np.cos(latd), np.sin(lond) * np.cos(latd), np.sin(latd) ] )

    #  Radius of curvature of the Earth. 

    Rcurv = np.linalg.norm( s - coc )

    return Rcurv


################################################################################
#  Methods. 
################################################################################

def occultation_count_by_mission( first_year, last_year ): 
    """Count occultations by mission and by month, then output to 
    alldata_json_file."""

    #  AWS access. Be sure to establish authentication for profile aws_profile 
    #  for successful use. 

    session = boto3.Session( profile_name=aws_profile, region_name=aws_region )
    resource = session.resource( "dynamodb" )
    table = resource.Table( dynamodb_table )

    #  Set up logging output. 

    handlers = [ logging.FileHandler( filename="count_occultations.log" ), 
            logging.StreamHandler( sys.stdout ) ]

    logging.basicConfig( handlers=handlers, level=logging.INFO )

    #  Get list of transmitters. 

    transmitters = get_transmitters()

    #  Initialize and loop over year-month. 

    alldata = []

    for year in range(first_year,last_year+1): 
        for month in range(1,13): 

            #  Define sort key range. 

            dtime1 = datetime( year, month, 1 )
            dtime2 = dtime1 + timedelta( days=31 )
            dtime2 = datetime( dtime2.year, dtime2.month, 1 ) - timedelta( minutes=1 )

            sortkey1 = "{:4d}-{:02d}-{:02d}-{:02d}-{:02d}".format( 
                    dtime1.year, dtime1.month, dtime1.day, dtime1.hour, dtime1.minute )
            sortkey2 = "{:4d}-{:02d}-{:02d}-{:02d}-{:02d}".format( 
                    dtime2.year, dtime2.month, dtime2.day, dtime2.hour, dtime2.minute )

            #  Initialize new year-month record. 

            rec = { 'year': year, 'month': month, 'noccs': {} }

            #  Loop over partition keys. The first element of the partition key is a 
            #  satellite identifier, which are given in the valid_missions definitions. 

            for mission, satellites in valid_missions.items(): 

                LOGGER.info( f"Working on {year=}, {month=}, {mission=}" )

                #  Initialize the occultation counter for this mission-year-month. 

                rec['noccs'].update( { mission: 0 } )

                for satellite in satellites: 
                    for transmitter in transmitters: 
                        partitionkey = f"{satellite}-{transmitter}"

                        #  Query the database and count the soundings. 

                        ret = table.query( 
                                KeyConditionExpression = 
                                    Key('leo-ttt').eq( partitionkey ) & 
                                    Key('date-time').between( sortkey1, sortkey2 ) 
                                )
                        rec['noccs'][mission] += ret['Count'] 

            LOGGER.info( "Record: " + json.dumps( rec ) )

            #  Append the month record to the alldata list. 

            alldata.append( rec )

    with open( alldata_json_file, 'w' ) as out: 
        LOGGER.info( f"Writing data counts to {alldata_json_file}." )
        json.dump( alldata, out, indent="  " )

    return alldata



def occultation_count_figure( epsfile ): 
    """Plot of timeseries stackplot of the counts of occultations per day by 
    mission with monthly resolution. Save encapsulated postscript file to 
    epsfile."""

    #  Read data computed previously by count_occultations.  

    with open( alldata_json_file, 'r' ) as d: 
        alldata = json.load( d )

    #  Find the start dates of counts for each mission. 

    missions = list( alldata[0]['noccs'].keys() )
    start_months = [ None for m in missions ]

    for rec in alldata: 
        for im, m in enumerate( missions ): 
            if start_months[im] is None and rec['noccs'][m] != 0: 
                start_months[im] = rec['year'] + ( rec['month'] - 0.5 ) / 12.0

    #  Eliminate missions without data. 

    smissions, sstarts = [], []
    for i, m in enumerate( missions ): 
        if start_months[i] is not None: 
            smissions.append( m )
            sstarts.append( start_months[i] )
    
    #  Sort start months. 

    isort = np.argsort( sstarts )
    missions = []
    for i in isort: 
        missions.append( smissions[i] )
    
    nmissions = isort.size

    #  Now resort the data. 

    counts = np.zeros( (nmissions, len(alldata)), dtype='f' )
    times = np.array( [ rec['year'] + ( rec['month'] - 0.5 ) / 12.0 for rec in alldata ] )

    for irec, rec in enumerate( alldata ): 
        for imission, mission in enumerate( missions ): 
            counts[imission,irec] = rec['noccs'][mission]

    #  Normalize counts by days in each month. 

    ndays = np.array( [ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ], dtype='f' )
    for i, rec in enumerate( alldata ): 
        imonth = rec['month'] - 1
        counts[:,i] /= ndays[imonth]

    #  Now do the stack plot. 

    fig = plt.figure( figsize=[6,3] )
    ax = fig.add_axes( [ 0.12, 0.15, 0.86, 0.83 ] )

    #  x axis. 

    ax.set_xlim( int( times[0] ), int( times[-1] ) + 1 )
    ax.set_xticks( np.arange( int(times[0]), times[-1]+1/12.0, 5 ) )
    ax.set_xticklabels( ax.get_xticks().astype('i'), rotation=-60, fontsize="large" )
    ax.xaxis.set_minor_locator( MultipleLocator( 1.0 ) )

    #  y axis. 

    ax.set_ylabel( 'Mean daily counts', fontsize="large" )
    ax.set_ylim( 0, 5000 )
    ax.set_yticks( np.arange( 0, 5001, 1000 ) )
    ax.set_yticklabels( ax.get_yticks().astype('i'), fontsize="large" )
    ax.yaxis.set_minor_locator( MultipleLocator( 200 ) )

    #  Now, stack plot. 

    ps = ax.stackplot( times, counts, labels=missions )
    ax.legend( loc="upper left", ncol=2 )
    leg = ax.get_legend()

    #  Get colors. 

    colormap = {}
    for im, m in enumerate(missions): 
        colormap.update( { m: list( ps[im].get_facecolor()[0] ) } )

    #  Save colors for future use. 

    with open( colors_json_file, 'w' ) as c: 
        print( f"Writing mission colors to {colors_json_file}." )
        json.dump( colormap, c, indent="    " )

    #  Generate plot. 

    print( f"Saving to {epsfile}." )
    plt.savefig( epsfile, format="eps" )

    return



def distribution_solartime_figure( year, month, day, epsfile ): 
    """Create a figure showing the distribution of occultations, by mission, for 
    one day of soundings as specified by year, month, day. The figure is saved 
    as encapsulated output to epsfile."""

    #  Set up session and dynamodb table. 

    session = boto3.Session( profile_name=aws_profile, region_name=aws_region )
    resource = session.resource( "dynamodb" )
    table = resource.Table( dynamodb_table )

    #  Which missions have data for this month? 

    with open( alldata_json_file, 'r' ) as d: 
        alldata = json.load( d )

    for rec in alldata: 
        if rec['year'] == year and rec['month'] == month: 
            missions = [ m for m in rec['noccs'].keys() if rec['noccs'][m] > 0 ]
            break

    #  Get all soundings for this month by mission. 

    maprecs = []

    #  Get list of transmitters. 

    transmitters = get_transmitters()

    #  Define sort key range. 

    dtime1 = datetime( year, month, day )
    dtime2 = dtime1 + timedelta( minutes=1439 )

    sortkey1 = "{:4d}-{:02d}-{:02d}-{:02d}-{:02d}".format( 
            dtime1.year, dtime1.month, dtime1.day, dtime1.hour, dtime1.minute )
    sortkey2 = "{:4d}-{:02d}-{:02d}-{:02d}-{:02d}".format( 
            dtime2.year, dtime2.month, dtime2.day, dtime2.hour, dtime2.minute )

    #  Loop over missions. 

    for mission, satellites in valid_missions.items(): 

        if mission not in missions: continue

        #  For each mission-year-month, retrieve all soundings, decimate, and retain 
        #  geolocation information. 

        rec = { 'mission': mission, 'longitudes': [], 'latitudes': [], 'solartimes': [] }
        noccs = 0

        for satellite in satellites: 
            for transmitter in transmitters: 
                partitionkey = f"{satellite}-{transmitter}"

                #  Query the database. 

                ret = table.query( 
                        KeyConditionExpression = 
                            Key('leo-ttt').eq( partitionkey ) & 
                            Key('date-time').between( sortkey1, sortkey2 ) 
                        )

                if ret['Count'] != 0: 
                    for item in ret['Items']: 
                        if float( item['longitude'] ) == -999.99 \
                                or float( item['latitude'] ) == -999.99 \
                                or float( item['local_time'] ) == -999.99 : 
                            continue
                        rec['longitudes'].append( float( item['longitude'] ) )
                        rec['latitudes'].append( float( item['latitude'] ) )
                        rec['solartimes'].append( float( item['local_time'] ) )
                        noccs += 1

        maprecs.append( rec )

    #  Read colormap. 

    with open( colors_json_file, 'r' ) as c: 
        colormap = json.load( c )

    #  Execute map. 

    fig = plt.figure( figsize=(6.5,2.0) )

    #  Longitude-latitude map. 

    ax = fig.add_axes( [0.01,0.22,0.45,0.67], projection=cartopy.crs.PlateCarree() )
    ax.coastlines( )

    ax.set_xlim( -180, 180 )
    ax.set_ylim( -90, 90 )

    title = "(a) RO geolocations for {:} {:} {:4d}".format( day, monthstrings[month-1], year )
    ax.set_title( title )

    #  Loop over mission records, plotting occ locations. 

    for rec in maprecs: 
        color = colormap[rec['mission']]
        ax.scatter( rec['longitudes'], rec['latitudes'], color=color, s=0.25 )

    #  Next axis: solar time distribution. 

    ax = fig.add_axes( [0.54,0.22,0.43,0.67] )

    title = "(b) RO solar times for {:} {:} {:4d}".format( day, monthstrings[month-1], year )
    ax.set_title( title )

    ax.set_xlim( 0, 24 )
    ax.set_xticks( np.arange( 0, 24.1, 6 ) )
    ax.xaxis.set_minor_locator( MultipleLocator( 1 ) )
    ax.set_xticklabels( [ f"{v:02d}:00" for v in ax.get_xticks().astype('i') ] )

    yticks = np.arange( -90, 90.1, 30 ).astype('i')
    ax.set_ylim( -90, 90 )
    ax.set_yticks( yticks )
    ax.yaxis.set_minor_locator( MultipleLocator( 10 ) )
    ax.set_yticklabels( latlabels( yticks ) )

    #  Loop over mission records, plotting occ solartimes. 

    for rec in maprecs: 
        color = colormap[rec['mission']]
        ax.scatter( rec['solartimes'], rec['latitudes'], color=color, s=0.25 )

    #  Done with figure. 

    print( f"Writing to {epsfile}." )
    fig.savefig( epsfile, format='eps' )

    return



def compute_center_intercomparison( year, month, day, mission, jsonfile ): 
    """Compute the center inter-comparison for bending angle, refractivity, 
    dry temperature, temperature, and specific humidity for all profiles 
    processed by UCAR and ROM SAF for a particular mission, year, month, 
    and day. The output of the computation is stored as JSON in jsonfile."""

    #  AWS access. Be sure to establish authentication for profile aws_profile 
    #  for successful use. 

    session = boto3.Session( profile_name=aws_profile, region_name=aws_region )

    #  DynamoDB table object. 

    resource = session.resource( "dynamodb" )
    table = resource.Table( dynamodb_table )

    #  AWS Open Data Repository of RO data. 

    resource = session.resource( "s3" )
    s3 = resource.Bucket( "gnss-ro-data" )

    #  Scan for RO soundings processed by UCAR and ROM SAF for mission, 
    #  year, month, day. 

    #  Define sort key range. 

    dtime1 = datetime( year, month, day )
    dtime2 = dtime1 + timedelta( minutes=1439 )

    sortkey1 = "{:4d}-{:02d}-{:02d}-{:02d}-{:02d}".format( 
            dtime1.year, dtime1.month, dtime1.day, dtime1.hour, dtime1.minute )
    sortkey2 = "{:4d}-{:02d}-{:02d}-{:02d}-{:02d}".format( 
            dtime2.year, dtime2.month, dtime2.day, dtime2.hour, dtime2.minute )

            #  Initialize new year-month record. 

    #  Initialize the list of RO sounding database entries. 

    commonsoundings = []

    #  Loop over partition keys. The first element of the partition key is a 
    #  satellite identifier, which are given in the valid_missions definitions. 

    for satellite in valid_missions[mission]: 
        for transmitter in transmitters: 

            partitionkey = f"{satellite}-{transmitter}"

            #  Query the database and count the soundings. 

            ret = table.query( 
                    KeyConditionExpression = 
                        Key('leo-ttt').eq( partitionkey ) & 
                        Key('date-time').between( sortkey1, sortkey2 ), 
                    FilterExpression = 
                        Attr('ucar_refractivityRetrieval').ne("") & 
                        Attr('ucar_atmosphericRetrieval').ne("") & 
                        Attr('romsaf_refractivityRetrieval').ne("") & 
                        Attr('romsaf_atmosphericRetrieval').ne("") 
                    )

            if ret['Count'] > 0: 
                commonsoundings += ret['Items']

    #  Independent coordinate for bending angle, in meters. 

    common_impactHeight = np.arange( 0.0, 60.0001e3, 0.1 ) 

    #  Independent coordinate for refractivity, dry temperature, temperature, 
    #  and specific humidity, in meters. 

    common_geopotentialHeight = np.arange( 0.0, 60.0001e3, 0.1 )

    #  Analysis of refractivityRetrieval files. Download the refractivityRetrieval 
    #  files common to UCAR and ROM SAF and compare bending angle, refractivity, 
    #  dry temperature. 

    #  Initialize difference statistics. 

    diffs_bendingAngle = [ [] for i in range(len(common_impactHeight)) ]
    diffs_refractivity = [ [] for i in range(len(common_geopotentialHeight)) ]
    diffs_dryTemperature = [ [] for i in range(len(common_geopotentialHeight)) ]

    #  Loop over refractivityRetrieval files. 

    for sounding in commonsoundings: 

        diff_bendingAngle = np.zeros( len(common_impactHeight) )
        diff_refractivity = np.zeros( len(common_geopotentialHeight) )
        diff_dryTemperature = np.zeros( len(common_geopotentialHeight) )

        for center in [ "ucar", "romsaf" ]: 

            if center == "ucar": 
                remote_path = sounding['ucar_refractivityRetrieval']
            elif center == "romsaf": 
                remote_path = sounding['romsaf_refractivityRetrieval']

            input_file = os.path.split( remote_path )[1]
            s3.download_file( remote_path, input_file )

            #  Open NetCDF file. 

            data = Dataset( input_file, 'r' )

            #  L1 bending angle and impact parameter. 

            input_bendingAngle = data.variables['rawBendingAngle'][:,0]
            input_impactParameter = data.variables['impactParameter'][:]

            #  Find radius of curvature in order to compute impact height. 

            Rcurv = radius_of_curvature( equatorialRadius, polarRadius, 
                    refLongitude, refLatitude, centerOfCurvature )

            #  Compute impact height from impact parameter by subtracting the local 
            #  radius of curvature for this RO sounding. 

            input_impactHeight = input_impactParameter - Rcurv

            #  Interpolate bending angle onto the common impact heights. 

            bendingAngle = masked_interpolate( input_impactHeight, input_bendingAngle, 
                    common_impactHeight )

            #  Interpolate refractivity onto the common geopotential heights. 

            input_geopotentialHeight = data.variables['geopotential'][:] / gravity
            input_refractivity = data.variables['refractivity'][:]

            #  Interpolate refractivity onto the common geopotential heights. 

            refractivity = masked_interpolate( input_geopotentialHeight, input_refractivity, 
                    common_geopotentialHeight )

            #  Generate dry temperature. 

            input_dryPressure = data.variables['dryPressure'][:]
            input_dryTemperature = input_dryPressure / input_refractivity * k1

            #  Interpolate dry temperature onto the common geopotential heights. 

            dryTemperature = masked_interpolate( input_geopotentialHeight, input_dryTemperature,
                    common_geopotentialHeight )

            #  Close and remove input file. 

            data.close()
            os.unlink( input_file )

            #  Calculate difference between UCAR and ROM SAF. 

            if center == "ucar": 
                diff_bendingAngle += bendingAngle
                diff_refractivity += refractivity
                diff_dryTemperature += dryTemperature
            elif center == "romsaf": 
                diff_bendingAngle -= bendingAngle
                diff_refractivity -= refractivity
                diff_dryTemperature -= dryTemperature

        #  Record the differences between UCAR and ROM SAF for bending angle, 
        #  refractivity, and dry temperature. 

        for i in np.argwhere( np.logical_not( diffs_bendingAngle.mask ) ).squeeze(): 
            diffs_bendingAngle[i].append( diff_bendingAngle[i] )

        for i in np.argwhere( np.logical_not( diffs_refractivity.mask ) ).squeeze(): 
            diffs_refractivity[i].append( diff_refractivity[i] )

        for i in np.argwhere( np.logical_not( diffs_dryTemperature.mask ) ).squeeze(): 
            diffs_dryTemperature[i].append( diff_dryTemperature[i] )

    #  Analysis of atmosphericRetrieval files. Download the atmosphericRetrieval 
    #  files common to UCAR and ROM SAF and compare bending angle, refractivity, 
    #  dry temperature. 

    #  Initialize difference statistics. 

    diffs_temperature = [ [] for i in range(len(common_geopotentialHeight)) ]
    diffs_specificHumidity = [ [] for i in range(len(common_geopotentialHeight)) ]

    #  Loop over atmosphericRetrieval files. 

    for sounding in commonsoundings: 

        diff_temperature = np.zeros( len(common_geopotentialHeight) )
        diff_specificHumidity = np.zeros( len(common_geopotentialHeight) )

        for center in [ "ucar", "romsaf" ]: 

            if center == "ucar": 
                remote_path = sounding['ucar_atmosphericRetrieval']
            elif center == "romsaf": 
                remote_path = sounding['romsaf_atmosphericRetrieval']

            input_file = os.path.split( remote_path )[1]
            s3.download_file( remote_path, input_file )

            #  Open NetCDF file. 

            data = Dataset( input_file, 'r' )

            #  Interpolate temperature onto the common geopotential heights. 

            input_geopotentialHeight = data.variables['geopotential'][:] / gravity
            input_temperature = data.variables['temperature'][:]
            temperature = masked_interpolate( input_geopotentialHeight, input_temperature, 
                    common_geopotentialHeight )

            #  Compute specific humidity. 

            input_pressure = data.variables['pressure'][:]
            input_waterVaporPressure = data.variables['waterVaporPressure'][:]
            input_specificHumidity = ( muvap * input_waterVaporPressure ) / \
                    ( mudry * ( input_pressure - input_waterVaporPressure ) \
                    + muvap * input_waterVaporPressure )

            #  Interpolate specific humidity onto the common geopotential heights. 

            input_geopotentialHeight = data.variables['geopotential'][:] / gravity
            specificHumidity = masked_interpolate( input_geopotentialHeight, 
                    input_specificHumidity, common_geopotentialHeight )

            #  Close and remove input file. 

            data.close()
            os.unlink( input_file )

            #  Calculate difference between UCAR and ROM SAF. 

            if center == "ucar": 
                diff_temperature += temperature
                diff_specificHumidity += specificHumidity
            elif center == "romsaf": 
                diff_temperature -= temperature
                diff_specificHumidity -= specificHumidity

        #  Record the differences between UCAR and ROM SAF for temperature and 
        #  specificHumidity. 

        for i in np.argwhere( np.logical_not( diffs_temperature.mask ) ).squeeze(): 
            diffs_temperature[i].append( diff_temperature[i] )

        for i in np.argwhere( np.logical_not( diffs_specificHumidity.mask ) ).squeeze(): 
            diffs_specificHumidity[i].append( diff_specificHumidity[i] )


    #  Save to output file. 

    output_dict = { 'common_impactHeight': common_impactHeight, 
            'common_geopotentialHeight': common_geopotentialHeight, 
            'diffs_bendingAngle': diffs_bendingAngle, 
            'diffs_refractivity': diffs_refractivity, 
            'diffs_dryTemperature': diffs_dryTemperature, 
            'diffs_temperature': diffs_temperature, 
            'diffs_specificHumidity': diffs_specificHumidity }

    return


#  Plot results of processing center intercomparison. 

def center_intercomparison_figure( jsonfile, pdffile ): 
    """Generate a figure showing the processing center intercomparison statistics. 
    jsonfile is generated by compute_center_comparison. The output is written to 
    PDF file pdffile."""

    #  Read JSON file. 

    with open( jsonfile, 'r' ) as e: 
        data = json.load( e )

    #  Set up figure. 

    fig, axes = plt.subplots( nrows=3, ncols=2, figsize=(9,6.5) )

    #  First axis: bending angle comparison. 

    ax = axes[0,0]

    ytickv = np.arange( 0.0, 60.01, 10 )
    ax.set_yticks( ytickv )
    ax.yaxis.set_minor_locator( MultipleLocator(2) )
    ax.set_ylabel( "Impact Height [km]" )

    xtickv = np.arange( -100, 100.01, 50 )
    ax.set_xticks( xtickv )
    ax.xaxis.set_minor_locator( MultipleLocator(10) )
    ax.set_xlabel( "$\Delta$ Bending [$\mu$-rads]" )

    #  Box and whisker plot. 

    da = np.abs( data['common_impactHeights'][1] - data['common_impactHeights'][0] )
    nskip = int( 5.0e3 / da )

    subset_diffs_bendingAngle = []

    for ia in range( len( data['common_impactHeights'] ), nskip ): 
        subset_impactHeights.append( data['common_impactHeights'][ia] )
        subset_diffs_bendingAngle.append( data['diffs_bendingAngle'][ia] )
    ax.boxplot( subset_diffs_bendingAngle, vert=False, whis=(5,95), 
            positions=subset_impactHeights, sym='k+', manage_ticks=False )

    plt.show()

    return

#  Main program. 

if __name__ == "__main__": 
    alldata = occultation_count_by_mission( 1995, 2020 )
    occultation_count_figure()
    # distribution_solartime_figure( 1997, 1, 10, epsfile="distribution_1997-01-10.eps" )
    # distribution_solartime_figure( 2003, 1, 2, epsfile="distribution_2003-01-02.eps" )
    # distribution_solartime_figure( 2009, 1, 4, epsfile="distribution_2009-01-04.eps" )
    # distribution_solartime_figure( 2020, 1, 3, epsfile="distribution_2020-01-03.eps" )
    jsonfile = "centerintercomparison_2009-01-04_cosmic1.json"
    compute_center_intercomparison( 2009, 1, 4, 'cosmic1', jsonfile )
    pdffile = "centerintercomparison_2009-01-04_cosmic1.pdf"
    center_intercomparison_figure( jsonfile, pdffile )

    pass

