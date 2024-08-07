"""DynamoDB Database Demonstration Code

Code examples for consulting the DynamoDB database for GNSS radio occultation
data in the AWS Open Data Registry. The code examples illustrate how to
manipulate the DynamoDB database and plot results of database inquiries.

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
  * numpy
  * netCDF4
  * matplotlib
  * cartopy
  * boto3

Before any of this code is implemented, it is first necessary to manifest
the DynamoDB database using the utilities in import_gnss-ro_dynamoDB.py. The user only need
modify a few parameters in the "IMPORTANT: Configuration" section below
in order for the code to function.


Version: 1.0
Author: Stephen Leroy (sleroy@aer.com)
Date: July 22, 2022

"""

##################################################
#  IMPORTANT: Configuration
##################################################

#  Define the name of the AWS profile to be used for authentication
#  purposes. The authentication will be needed to access the DynamoDB
#  database table. If no authentication is required, then set
#  aws_profile to None.

aws_profile = None

#  Define the AWS region where the DynamoDB database is manifested
#  and the name of the DynamoDB database table.

aws_region = "us-east-1"
dynamodb_table = "gnss-ro-data-stagingv1_1"

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
import cartopy
import matplotlib.cm as cm
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
    'cosmic2': [ "cosmic2e{:1d}".format(i) for i in range(1,7) ],
    'spire': [ "spireS{:03d}".format(i) for i in range(99,151) ],
    'geoopt': [ "geooptG{:02d}".format(i) for i in range(1,8) ]
    }

#  Matplotlib default settings.

axeslinewidth = 0.5
plt.rcParams.update( {
  'font.family': "Times New Roman",
  'font.size': 8,
  'font.weight': "normal",
  'text.usetex': False,
  'xtick.major.width': axeslinewidth,
  'xtick.minor.width': axeslinewidth,
  'ytick.major.width': axeslinewidth,
  'ytick.minor.width': axeslinewidth,
  'axes.linewidth': axeslinewidth } )

#  GNSS constellations used as RO transmitters for this analysis. This must
#  include "R" in addition to "G" when cosmic2 data or data from the
#  commercial RO data providers become available.

# valid_constellations = [ "G", "R" ]
valid_constellations = [ "G", "R", "E" ]

#  Intermediate files: mission color table.

colors_json_file = "color_table_by_mission.json"

#  Define month labeling strings.

monthstrings = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()

#  Logger.

import logging

LOGGER = logging.getLogger( __name__ )
handlers = [ logging.FileHandler( filename="dynamodb_demonstration.log" ),
    logging.StreamHandler( sys.stdout ) ]
formatstr = '%(pathname)s:%(lineno)d %(levelname)s: %(message)s'
logging.basicConfig( handlers=handlers, level=logging.INFO, format=formatstr )

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
            ylabel = "{:}S".format( np.abs( lat ) )
        elif lat > 0:
            ylabel = "{:}N".format( np.abs( lat ) )
        else:
            ylabel = "Eq"
        ylabels.append( ylabel )

    return ylabels

def get_transmitters( constellations ):
    """Return a list of possible transmitter names as 3-character PRNs"""

    transmitters = []
    for constellation in constellations:
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


def merge_jsonfiles( jsonfiles, output_jsonfile=None ):
    """Merge together the contents of multiple json files generated by
    count_occultations and return the contents of the merge. Write the
    merged contents to output_jsonfile if a filename is given."""

    #  Read the data.

    alldata_tmp = []
    for jsonfile in jsonfiles:
        with open( jsonfile, 'r' ) as fp:
            alldata_tmp += json.load( fp )

    #  Sort the data.

    yearmonths = [ f"{rec['year']:04d}-{rec['month']:02d}" for rec in alldata_tmp ]
    yearmonths_sorted = sorted( yearmonths )

    alldata = []
    for yearmonth in yearmonths_sorted:
        irec = yearmonths.index( yearmonth )
        alldata.append( alldata_tmp[irec] )

    #  Write to output if requested.

    if output_jsonfile is not None:
        with open( output_jsonfile, 'w' ) as e:
            json.dump( alldata, e )

    #  Done.

    return alldata


################################################################################
#  Methods.
################################################################################

def occultation_count_by_mission( first_year, last_year, output_json ):
    """Count occultations by mission and by month, then output to
    output_json."""

    #  AWS access. Be sure to establish authentication for profile aws_profile
    #  for successful use.

    if aws_profile is None:
        session = boto3.Session( region_name=aws_region )
    else:
        session = boto3.Session( profile_name=aws_profile, region_name=aws_region )

    resource = session.resource( "dynamodb" )
    table = resource.Table( dynamodb_table )

    #  Get list of transmitters.

    transmitters = get_transmitters( [ "G", "R" ] )

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

    with open( output_json, 'w' ) as out:
        LOGGER.info( f"Writing data counts to {output_json}." )
        json.dump( alldata, out, indent="  " )

    return alldata


def occultation_count_figure( alldata, epsfile, yticks=np.arange(0,5001,1000), yminor=200 ):
    """Plot of timeseries stackplot of the counts of occultations per day by
    mission with monthly resolution. Save encapsulated postscript file to
    epsfile. yticks is a numpy array of the major y tick marks, and yminor
    is the interval for minor ticks on the y axis."""

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

    #  Maximum number of occultations per month.

    maxcount = counts.sum(axis=0).max()

    #  Now do the stack plot.

    fig = plt.figure( figsize=[6,3] )
    ax = fig.add_axes( [ 0.12, 0.15, 0.86, 0.81 ] )

    #  x axis.

    ax.set_xlim( int( times[0] ), int( times[-1] ) + 1 )
    ax.set_xticks( np.arange( int(times[0]), times[-1]+1/12.0, 5 ) )
    ax.set_xticklabels( ax.get_xticks().astype('i'), rotation=-60, fontsize="large" )
    ax.xaxis.set_minor_locator( MultipleLocator( 1.0 ) )

    #  y axis.

    ax.set_ylabel( 'Mean daily counts', fontsize="large" )
    ax.set_ylim( 0, yticks.max() )
    ax.set_yticks( yticks )
    ax.set_yticklabels( ax.get_yticks().astype('i'), fontsize="large" )
    ax.yaxis.set_minor_locator( MultipleLocator( yminor ) )

    #  Colors.

    cmap = cm.get_cmap('tab20')
    colors = [ cmap(i*0.05) for i in range(len(missions)) ]

    #  Now, stack plot.

    ps = ax.stackplot( times, counts, labels=missions, colors=colors )
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

    if aws_profile is None:
        session = boto3.Session( region_name=aws_region )
    else:
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

    transmitters = get_transmitters( [ "G", "R" ] )

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

    if not os.path.isfile( colors_json_file ):
        LOGGER.exception( f"File {colors_json_file} does not exist. It should be " +
                "generated by occultation_count_figure beforehand." )
        return

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
