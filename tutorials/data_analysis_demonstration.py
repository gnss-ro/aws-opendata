"""Tutorial Demonstration Code

Code examples for consulting the DynamoDB database for GNSS radio occultation
data in the AWS Open Data Registry and performing some basic data analysis,
which, in this case, is a processing center inter-comparison of retrieved
RO variables bending angle, log-refractivity, temperature and pressure for
occultations as processed by two different retrieval centers.

See the __main__ code at the bottom of this module to see how to execute
the two main functions defined below...

  * compute_center_intercomparison, which computes the inter-center
    differences on common level grids, and

  * center_intercomparison_figure, which plots the results as box-and-
    whisker plots.

The prerequisite nonstandard python modules that must be installed are
  * netCDF4
  * numpy
  * scipy
  * matplotlib
  * cartopy
  * boto3

Before any of this code is implemented, it is first necessary to manifest
the DynamoDB database using the utilities in import_gnss-ro_dynamoDB.py.
The user only need modify a few parameters in the "IMPORTANT: Configuration"
section below in order for the code to function.


Version: 1.1
Author: Stephen Leroy (sleroy@aer.com)
Date: July 25, 2022

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
from scipy.interpolate import interp1d
import cartopy
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.handlers import disable_signing

import pdb

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
valid_constellations = [ "G" ]

#  Intermediate files: RO data count by mission and mission color table.

colors_json_file = "color_table_by_mission.json"

#  Define month labeling strings.

monthstrings = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()

#  Location of AWS Open Data Registry repository of RO data. Do not touch!

aws_opendata_region = "us-east-1"
aws_opendata_bucket = "gnss-ro-data"

#  Logger.

import logging
LOGGER = logging.getLogger( __name__ )
handlers = [ logging.FileHandler( filename="data_analysis_demonstration.log" ),
    logging.StreamHandler( sys.stdout ) ]
formatstr = '%(pathname)s:%(lineno)d %(levelname)s: %(message)s'
logging.basicConfig( handlers=handlers, level=logging.INFO, format=formatstr )

#  Physical constants.

gravity = 9.80665           # WMO standard gravity [J/kg/m]
k1 = 77.6e-2                # First term in refractivity equation [N-units K/Pa]
muvap = 18.015e-3           # Mean molecular mass of water vapor [kg/mole]
mudry = 28.965e-3           # Mean molecular mass of dry air [kg/mole]


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

def masked_interpolate( x_in, y_in, x_out ):
    """Performed a masked cubic spline interpolation. The output array is masked
    where an interpolation is impossible."""

    #  Strip out masked values in the input arrays.

    if x_in.mask.size == 1:
        xmask = np.repeat( x_in.mask, x_in.size )
    else:
        xmask = x_in.mask

    if y_in.mask.size == 1:
        ymask = np.repeat( y_in.mask, y_in.size )
    else:
        ymask = y_in.mask

    i = np.argwhere( np.logical_not( np.logical_or( xmask, ymask ) ) ).squeeze()
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


################################################################################
#  Methods.
################################################################################

def compute_center_intercomparison( year, month, day, mission, jsonfile ):
    """Compute the center inter-comparison for bending angle, log-refractivity,
    dry temperature, temperature, and specific humidity for all profiles
    processed by UCAR and ROM SAF for a particular mission, year, month,
    and day. The output of the computation is stored as JSON in jsonfile."""

    #  AWS access. Be sure to establish authentication for profile aws_profile
    #  for successful use.

    if aws_profile is None:
        dynamodb_session = boto3.Session( region_name=aws_region )
    else:
        dynamodb_session = boto3.Session( profile_name=aws_profile, region_name=aws_region )

    #  DynamoDB table object.

    dynamodb_resource = dynamodb_session.resource( "dynamodb" )
    table = dynamodb_resource.Table( dynamodb_table )

    #  AWS Open Data Repository of RO data.

    opendata_session = boto3.Session( region_name=aws_opendata_region )
    opendata_s3_resource = opendata_session.resource( "s3" )
    opendata_s3_resource.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    s3 = opendata_s3_resource.Bucket( aws_opendata_bucket )

    #  Scan for RO soundings processed by UCAR and ROM SAF for mission,
    #  year, month, day.

    #  Define transmitters.

    transmitters = get_transmitters( [ "G", "R" ] )

    #  Define sort key range.

    dtime1 = datetime( year, month, day )
    dtime2 = dtime1 + timedelta( minutes=1439 )

    sortkey1 = "{:4d}-{:02d}-{:02d}-{:02d}-{:02d}".format(
            dtime1.year, dtime1.month, dtime1.day, dtime1.hour, dtime1.minute )
    sortkey2 = "{:4d}-{:02d}-{:02d}-{:02d}-{:02d}".format(
            dtime2.year, dtime2.month, dtime2.day, dtime2.hour, dtime2.minute )

    #  Initialize the list of RO sounding database entries.

    LOGGER.info( "Searching for soundings common to ucar and romsaf." )

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
                        Attr('ucar_refractivityRetrieval').exists() &
                        Attr('ucar_atmosphericRetrieval').exists() &
                        Attr('romsaf_refractivityRetrieval').exists() &
                        Attr('romsaf_atmosphericRetrieval').exists()
                    )

            if ret['Count'] > 0:
                commonsoundings += ret['Items']

    LOGGER.info( "Number of common soundings = {:d}".format( len( commonsoundings ) ) )

    #  Independent coordinate for bending angle, in meters.

    common_impactHeight = np.arange( 0.0, 60.0001e3, 100 )

    #  Independent coordinate for log-refractivity, dry temperature, temperature,
    #  and specific humidity, in meters.

    common_geopotentialHeight = np.arange( 0.0, 60.0001e3, 200 )

    #  Analysis of refractivityRetrieval files. Download the refractivityRetrieval
    #  files common to UCAR and ROM SAF and compare bending angle, log-refractivity,
    #  dry temperature.

    #  Initialize difference statistics.

    diffs_bendingAngle = [ [] for i in range(len(common_impactHeight)) ]
    diffs_logrefractivity = [ [] for i in range(len(common_geopotentialHeight)) ]
    diffs_dryTemperature = [ [] for i in range(len(common_geopotentialHeight)) ]

    #  Loop over refractivityRetrieval files.

    LOGGER.info( "Analyzing refractivityRetrieval files" )

    for isounding, sounding in enumerate( commonsoundings ):

        if ( isounding + 1 ) % 10 == 0:
            LOGGER.info( f"  sounding {isounding+1:d}" )

        diff_bendingAngle = np.ma.array( np.zeros( len(common_impactHeight) ), mask=False )
        diff_logrefractivity = np.ma.array( np.zeros( len(common_geopotentialHeight) ), mask=False )
        diff_dryTemperature = np.ma.array( np.zeros( len(common_geopotentialHeight) ), mask=False )
        status = "success"

        for center in [ "ucar", "romsaf" ]:

            if center == "ucar":
                remote_path = sounding['ucar_refractivityRetrieval']
            elif center == "romsaf":
                remote_path = sounding['romsaf_refractivityRetrieval']

            input_file = os.path.split( remote_path )[1]
            try:
                s3.download_file( remote_path, input_file )
            except Exception as excpt:
                status = "fail"
                message = f"Failed to download {remote_path} from s3://{aws_opendata_bucket}"
                LOGGER.exception( message )
                LOGGER.exception( json.dumps( excpt.args ) )
                break

            #  Open NetCDF file.

            data = Dataset( input_file, 'r' )

            #  L1 bending angle and impact parameter.

            input_bendingAngle = data.variables['bendingAngle'][:]
            input_impactParameter = data.variables['impactParameter'][:]

            #  Get radius of curvature in order to compute impact height from
            #  impact parameter.

            radiusOfCurvature = data.variables['radiusOfCurvature'].getValue()

            #  Compute impact height from impact parameter by subtracting the local
            #  radius of curvature for this RO sounding.

            input_impactHeight = input_impactParameter - radiusOfCurvature

            #  Interpolate bending angle onto the common impact heights.

            bendingAngle = masked_interpolate( input_impactHeight, input_bendingAngle,
                    common_impactHeight )

            #  Interpolate log-refractivity onto the common geopotential heights.

            input_geopotentialHeight = data.variables['geopotential'][:] / gravity
            input_logrefractivity = np.log( data.variables['refractivity'][:] )

            #  Interpolate log-refractivity onto the common geopotential heights.

            logrefractivity = masked_interpolate( input_geopotentialHeight, input_logrefractivity,
                    common_geopotentialHeight )

            #  Generate dry temperature.

            input_dryPressure = data.variables['dryPressure'][:]
            input_dryTemperature = input_dryPressure / np.exp( input_logrefractivity ) * k1

            #  Interpolate dry temperature onto the common geopotential heights.

            dryTemperature = masked_interpolate( input_geopotentialHeight, input_dryTemperature,
                    common_geopotentialHeight )

            #  Close and remove input file.

            data.close()
            os.unlink( input_file )

            #  Calculate difference between UCAR and ROM SAF.

            if center == "ucar":
                diff_bendingAngle += bendingAngle
                diff_logrefractivity += logrefractivity
                diff_dryTemperature += dryTemperature
            elif center == "romsaf":
                diff_bendingAngle -= bendingAngle
                diff_logrefractivity -= logrefractivity
                diff_dryTemperature -= dryTemperature


        if status == "fail": continue

        #  Record the differences between UCAR and ROM SAF for bending angle,
        #  log-refractivity, and dry temperature.

        for i in np.argwhere( np.logical_not( diff_bendingAngle.mask ) ).squeeze():
            diffs_bendingAngle[i].append( diff_bendingAngle[i] )

        for i in np.argwhere( np.logical_not( diff_logrefractivity.mask ) ).squeeze():
            diffs_logrefractivity[i].append( diff_logrefractivity[i] )

        for i in np.argwhere( np.logical_not( diff_dryTemperature.mask ) ).squeeze():
            diffs_dryTemperature[i].append( diff_dryTemperature[i] )

    #  Analysis of atmosphericRetrieval files. Download the atmosphericRetrieval
    #  files common to UCAR and ROM SAF and compare bending angle, log-refractivity,
    #  dry temperature.

    LOGGER.info( "Analyzing atmosphericRetrieval files" )

    #  Initialize difference statistics.

    diffs_temperature = [ [] for i in range(len(common_geopotentialHeight)) ]
    diffs_specificHumidity = [ [] for i in range(len(common_geopotentialHeight)) ]

    #  Loop over atmosphericRetrieval files.

    for isounding, sounding in enumerate( commonsoundings ):

        if ( isounding + 1 ) % 10 == 0:
            LOGGER.info( f"  sounding {isounding+1:d}" )

        diff_temperature = np.ma.array( np.zeros( len(common_geopotentialHeight) ), mask=False )
        diff_specificHumidity = np.ma.array( np.zeros( len(common_geopotentialHeight) ), mask=False )

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

        for i in np.argwhere( np.logical_not( diff_temperature.mask ) ).squeeze():
            diffs_temperature[i].append( diff_temperature[i] )

        for i in np.argwhere( np.logical_not( diff_specificHumidity.mask ) ).squeeze():
            diffs_specificHumidity[i].append( diff_specificHumidity[i] )


    #  Save to output file.

    output_dict = {
            'common_impactHeight': list( common_impactHeight ),
            'common_geopotentialHeight': list( common_geopotentialHeight ),
            'diffs_bendingAngle': diffs_bendingAngle,
            'diffs_logrefractivity': diffs_logrefractivity,
            'diffs_dryTemperature': diffs_dryTemperature,
            'diffs_temperature': diffs_temperature,
            'diffs_specificHumidity': diffs_specificHumidity }

    LOGGER.info( f"Storing data in {jsonfile}." )

    with open( jsonfile, 'w' ) as out:
        json.dump( output_dict, out, indent="  " )

    return


#  Plot results of processing center intercomparison.

def center_intercomparison_figure( jsonfile, epsfile ):
    """Generate a figure showing the processing center intercomparison statistics.
    jsonfile is generated by compute_center_comparison. The output is written to
    encapsulated postscript file epsfile."""

    #  Read JSON file.

    with open( jsonfile, 'r' ) as e:
        data = json.load( e )

    #  Set up figure.

    fig, axes = plt.subplots( nrows=2, ncols=2, figsize=(9,6.5) )

    #  First axis: log-refractivity comparison.

    green_diamond = { 'markerfacecolor': "g", 'marker': "D", 'markersize': 2.0 }
    kwargs = { 'vert': False, 'whis': (5,95), 'flierprops': green_diamond,
            'manage_ticks': False, 'widths': 1.3 }

    ax = axes[0,0]

    ytickv = np.arange( 0.0, 60.01, 10 )
    ax.set_yticks( ytickv )
    ax.set_ylim( ytickv.min(), ytickv.max() )
    ax.yaxis.set_minor_locator( MultipleLocator(2) )
    ax.set_ylabel( "Geopotential Height [km]" )

    xtickv = np.arange( -10, 10.01, 5 )
    ax.set_xticks( xtickv )
    ax.set_xlim( xtickv.min(), xtickv.max() )
    ax.xaxis.set_minor_locator( MultipleLocator(1) )
    ax.set_xlabel( "Refractivity Diffs [%]" )

    #  Subset the data by level to every 5-km.

    resolution = 2.0e3
    da = np.abs( data['common_geopotentialHeight'][1] - data['common_geopotentialHeight'][0] )
    nskip = int( resolution / da )

    subset_diffs_logrefractivity = []
    subset_geopotentialHeights = []

    for ia in range( 0, len( data['common_geopotentialHeight'] ), nskip ):
        subset_geopotentialHeights.append( data['common_geopotentialHeight'][ia] )
        subset_diffs_logrefractivity.append(
                np.array( data['diffs_logrefractivity'][ia] ) * 100 )
    subset_geopotentialHeights = np.array( subset_geopotentialHeights ) / 1000

    #  Box and whisker plot.

    ax.boxplot( subset_diffs_logrefractivity,
            positions=subset_geopotentialHeights, **kwargs )

    #  Second axis: dry temperature comparison.

    ax = axes[0,1]

    ytickv = np.arange( 0.0, 60.01, 10 )
    ax.set_yticks( ytickv )
    ax.set_ylim( ytickv.min(), ytickv.max() )
    ax.yaxis.set_minor_locator( MultipleLocator(2) )
    ax.set_ylabel( "Geopotential Height [km]" )

    xtickv = np.arange( -20, 20.01, 10 )
    ax.set_xticks( xtickv )
    ax.set_xlim( xtickv.min(), xtickv.max() )
    ax.xaxis.set_minor_locator( MultipleLocator(2) )
    ax.set_xlabel( "Dry Temperature Diffs [K]" )

    #  Subset the data by level to every 5-km.

    resolution = 2.0e3
    da = np.abs( data['common_geopotentialHeight'][1] - data['common_geopotentialHeight'][0] )
    nskip = int( resolution / da )

    subset_diffs_dryTemperature = []
    subset_geopotentialHeights = []

    for ia in range( 0, len( data['common_geopotentialHeight'] ), nskip ):
        subset_geopotentialHeights.append( data['common_geopotentialHeight'][ia] )
        subset_diffs_dryTemperature.append( data['diffs_dryTemperature'][ia] )
    subset_geopotentialHeights = np.array( subset_geopotentialHeights ) / 1000

    #  Box and whisker plot.

    ax.boxplot( subset_diffs_dryTemperature,
            positions=subset_geopotentialHeights, **kwargs )

    #  Third axis: temperature comparison.

    green_diamond = { 'markerfacecolor': "g", 'marker': "D", 'markersize': 1.2 }
    kwargs = { 'vert': False, 'whis': (5,95), 'flierprops': green_diamond,
            'manage_ticks': False, 'widths': 0.75 }

    ax = axes[1,0]

    ytickv = np.arange( 0.0, 20.01, 5 )
    ax.set_yticks( ytickv )
    ax.set_ylim( ytickv.min(), ytickv.max() )
    ax.yaxis.set_minor_locator( MultipleLocator(1) )
    ax.set_ylabel( "Geopotential Height [km]" )

    xtickv = np.arange( -10, 10.01, 5 )
    ax.set_xticks( xtickv )
    ax.set_xlim( xtickv.min(), xtickv.max() )
    ax.xaxis.set_minor_locator( MultipleLocator(1) )
    ax.set_xlabel( "Temperature Diffs [K]" )

    #  Subset the data by level to every 5-km.

    resolution = 1.0e3
    da = np.abs( data['common_geopotentialHeight'][1] - data['common_geopotentialHeight'][0] )
    nskip = int( resolution / da )

    subset_diffs_temperature = []
    subset_geopotentialHeights = []

    for ia in range( 0, len( data['common_geopotentialHeight'] ), nskip ):
        subset_geopotentialHeights.append( data['common_geopotentialHeight'][ia] )
        subset_diffs_temperature.append( data['diffs_temperature'][ia] )
    subset_geopotentialHeights = np.array( subset_geopotentialHeights ) / 1000

    #  Box and whisker plot.

    ax.boxplot( subset_diffs_temperature,
            positions=subset_geopotentialHeights, **kwargs )

    #  Fourth axis: specific humidity comparison.

    ax = axes[1,1]

    ytickv = np.arange( 0.0, 20.01, 5 )
    ax.set_yticks( ytickv )
    ax.set_ylim( ytickv.min(), ytickv.max() )
    ax.yaxis.set_minor_locator( MultipleLocator(1) )
    ax.set_ylabel( "Geopotential Height [km]" )

    xtickv = np.arange( -4, 4.01, 2 )
    ax.set_xticks( xtickv )
    ax.set_xlim( xtickv.min(), xtickv.max() )
    ax.xaxis.set_minor_locator( MultipleLocator(0.5) )
    ax.set_xlabel( "Specific Humidity Diffs [g/kg]" )

    #  Subset the data by level to every 5-km.

    resolution = 1.0e3
    da = np.abs( data['common_geopotentialHeight'][1] - data['common_geopotentialHeight'][0] )
    nskip = int( resolution / da )

    subset_diffs_specificHumidity = []
    subset_geopotentialHeights = []

    for ia in range( 0, len( data['common_geopotentialHeight'] ), nskip ):
        subset_geopotentialHeights.append(
                data['common_geopotentialHeight'][ia] )
        subset_diffs_specificHumidity.append(
                np.array( data['diffs_specificHumidity'][ia] ) * 1000 )
    subset_geopotentialHeights = np.array( subset_geopotentialHeights ) / 1000

    #  Box and whisker plot.

    ax.boxplot( subset_diffs_specificHumidity,
            positions=subset_geopotentialHeights, **kwargs )

    #  Save to encapsulated postscript file.

    LOGGER.info( f"Saving to {epsfile}." )
    fig.savefig( epsfile, format='eps' )

    return



#  Main program.

if __name__ == "__main__":

    #  Compute the differences between UCAR and ROM SAF RO soundings for
    #  4 January 2009.

    jsonfile = "centerintercomparison_2009-01-04_cosmic1.json"
    compute_center_intercomparison( 2009, 1, 4, 'cosmic1', jsonfile )

    #  Plot the results of the inter-center comparison.

    epsfile = "centerintercomparison_2009-01-04_cosmic1.eps"
    center_intercomparison_figure( jsonfile, epsfile )

    pass
