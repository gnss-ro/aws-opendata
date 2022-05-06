"""Tutorial Demonstration Code

Code examples for consulting the DynamoDB database for GNSS radio occultation 
data in the AWS Open Data Registry. The code examples illustrate how to 
manipulate the DynamoDB database, plot results of database inquiries, and basic 
inter-comparison of bending angle, refractivity, temperature and pressure for 
occultations as processed by two different retrieval centers.

The instructive/tutorial methods are...

occultation_count_by_mission: This function queries the DynamoDB database 
    of RO data in the gnss-ro-data Open Data Registry archive in order to 
    count the number of occultation for every month in a specified range of 
    years and do so by RO mission. The results are saved in an output JSON 
    file for later plotting by...

occultation_count_figure: This function generates a stackplot of mean 
    daily counts of RO soundings by mission on monthly intervals. The plot 
    is saved as an encapsulated postscript file. 

distribution_solartime_figure: This function generates two plots, one 
    showing the distribution of RO soundings for a given day in longitude-
    latitude space, and the second showing the distribution of the same 
    sounding in the space of solar time-latitude. It uses cartopy and 
    matplotlib.

The prerequisite nonstandard python modules that must be installed are 
  * netCDF4
  * numpy
  * matplotlib
  * cartopy
  * boto3

Before any of this code is implemented, it is first necessary to manifest 
the DynamoDB database using the utilities in ???. The user only need 
modify a few parameters in the "IMPORTANT: Configuration" section below 
in order for the code to function. 


Version: 1.0
Author: Stephen Leroy (sleroy@aer.com)
Date: May 6, 2022

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

import sys
from datetime import datetime, timedelta
import json

#  Import installed modules. 

from netCDF4 import Dataset
import numpy as np
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
                                    Key('leo-ttt').eq(partitionkey) & 
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
                            Key('leo-ttt').eq(partitionkey) & 
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



def center_intercomparison_figure( year, month, day, mission, epsfile ): 
    """Compare bending angle, refractivity, dry temperature, pressure, 
    temperature, and water vapor for all profiles processed by UCAR and 
    ROM SAF for a particular mission, year, month, and day. The figure 
    is saved to encapsulated postscript file epsfile."""

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
                        Key('leo-ttt').eq(partitionkey) & 
                        Key('date-time').between( sortkey1, sortkey2 ), 
                    FilterExpression = 
                        Attr('ucar_refractivityRetrieval').ne("") & 
                        Attr('ucar_atmosphericRetrieval').ne("") & 
                        Attr('romsaf_refractivityRetrieval').ne("") & 
                        Attr('romsaf_atmosphericRetrieval').ne("") 
                    )

            if ret['Count'] > 0: 
                commonsoundings += ret['Items']

    #  Analysis of refractivityRetrieval files. Download the refractivityRetrieval 
    #  files common to UCAR and ROM SAF and compare bending angle, refractivity, 
    #  dry temperature. 

    #  Independent coordinate for bending angle, in meters. 
    common_impactHeights = np.arange( 0.0, 60.0001e3, 0.1 ) 

    #  Independent coordinate for refractivity, dry temperature, in meters. 
    common_geopotentialHeights = np.arange( 0.0, 60.0001e3, 0.1 )

    #  Initialize difference statistics. 
    diffs_bendingAngle = [ [] for i in range(len(common_impactHeights)) ]
    diffs_refractivity = [ [] for i in range(len(common_geopotentialHeights)) ]
    diffs_dryTemperature = [ [] for i in range(len(common_geopotentialHeights)) ]

    #  Loop over refractivityRetrieval files. 

    for sounding in commonsoundings: 
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

            refLatitude = data.variables['refLatitude'].getValue()
            refLongitude = data.variables['refLongitude'].getValue()
            equatorialRadius = data.variables['equatorialRadius'].getValue()
            polarRadius = data.variables['polarRadius'].getValue()
            centerOfCurvature = data.variables['centerOfCurvature'][:]

        lat = np.deg2rad( refLatitude )
        geocentricLatitude = np.rad2deg( 
                np.arctan2( polarRadius**2 * np.sin(lat), equatorialRadius**2 * np.cos(lat) ) 
                )
        surfacePoint = np.array( [ 

#  Main program. 

if __name__ == "__main__": 
    alldata = occultation_count_by_mission( 1995, 2020 )
    occultation_count_figure()
    distribution_solartime_figure( 1997, 1, 10, epsfile="distribution_1997-01-10.eps" )
    distribution_solartime_figure( 2003, 1, 2, epsfile="distribution_2003-01-02.eps" )
    distribution_solartime_figure( 2009, 1, 4, epsfile="distribution_2009-01-04.eps" )
    distribution_solartime_figure( 2020, 1, 3, epsfile="distribution_2020-01-03.eps" )

    pass

