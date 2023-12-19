#  Logging.

import sys
import logging

logging_output_file = "testing.log"
handlers = [ logging.FileHandler( filename=logging_output_file ),
        logging.StreamHandler( sys.stdout ) ]
formatstr = '%(pathname)s:%(lineno)d %(levelname)s: %(message)s'
logging.basicConfig( handlers=handlers, level=logging.WARNING, format=formatstr )

import os
import re
import json
import time
import s3fs
import zarr
from pprint import pprint
from datetime import datetime, timedelta
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

sys.path.append("../../docker")
from Database.dynamodbinterface import ProcessReformat
import Versions
from Reformatters import reformatters
from Utilities.batchprocess import batchprocess
from Utilities.resources import AWSprofile as default_profile, AWSregion as default_region
import pdb

#  Settings. 

version = Versions.get_version( "1.1" )
table_name = version['module'].dynamodbTable


def test_couplets( couplets, output_prefix="s3://gnss-ro-data-test" ):
    """Test the translation code provided a list of dictionaries that define jobs. Each
    "couplet" dictionary in the couplets should contain the following keywords and values:

    'processing_center':      "ucar", "romsaf", "jpl", "eumetsat"
    'processing_center':      "ucar", "romsaf", "jpl", "eumetsat"
    'input_prefix':           root path of the incoming data, with our without "s3://" as prefix
    'atmPhs/atmPrf/wetPrf/atm/wet/etc.':
                            relative path to the incoming RO file.
    """

    session = boto3.session.Session( profile_name=default_profile, region_name=default_region )
    meta = { 'session': session, 'workingdir': "workingdir", 'purge': False }

    for couplet in couplets:

        input_prefix = couplet['input_prefix']
        processing_center = couplet['processing_center']

        #  calibratedPhase

        file_type = "level1b"

        if processing_center == "ucar" and "atmPhs" in couplet.keys():
            input_relative_path = couplet['atmPhs']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )

        elif processing_center == "jpl" and "calibratedPhase" in couplet.keys(): 
            input_relative_path = couplet['calibratedPhase']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )

        elif processing_center == "eumetsat" and "level1b" in couplet.keys(): 
            input_relative_path = couplet['level1b']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )

        elif processing_center == "eumetsat" and "level1b" in couplet.keys(): 
            input_relative_path = couplet['level1b']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )


        #  refractivityRetrieval

        file_type = "level2a"

        if processing_center == "ucar" and "atmPrf" in couplet.keys():
            input_relative_path = couplet['atmPrf']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )

        elif processing_center == "romsaf" and "atm" in couplet.keys():
            input_relative_path = couplet['atm']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )

        elif processing_center == "jpl" and "refractivityRetrieval" in couplet.keys():
            input_relative_path = couplet['refractivityRetrieval']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )

        #  atmosphericRetrieval

        file_type = "level2b"

        if processing_center == "ucar" and "wetPrf" in couplet.keys():
            input_relative_path = couplet['wetPrf']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )

        elif processing_center == "romsaf" and "wet" in couplet.keys():
            input_relative_path = couplet['wet']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )

        elif processing_center == "jpl" and "atmosphericRetrieval" in couplet.keys():
            input_relative_path = couplet['atmosphericRetrieval']
            print( f"{processing_center=}, {file_type=}, {table_name=}" )
            process = ProcessReformat( file_type, processing_center, table_name, version, **meta )
            print( f"{input_prefix=}, {output_prefix=},\n  {input_relative_path=}" )
            t0 = time.time()
            ret = process( input_prefix, input_relative_path, output_prefix, clobber=True )
            t1 = time.time()
            print( "ret = " )
            pprint( ret )
            print( f"  elapsed time = {t1-t0:6.3f} s\n" )


def test_metop():

    if True:
        couplets = [
            {   'input_prefix': "s3://ucar-earth-ro-archive-untarred",
                'processing_center': "ucar",
                'atmPhs': "metopb/repro2016/level1b/2014/001/atmPhs_repro2016_2014_001/atmPhs_MTPB.2014.001.04.05.G01_2016.0120_nc",
                'atmPrf': "metopb/repro2016/level2/2014/001/atmPrf_repro2016_2014_001/atmPrf_MTPB.2014.001.04.05.G01_2016.0120_nc",
                'wetPrf': "metopb/repro2016/level2/2014/001/wetPrf_repro2016_2014_001/wetPrf_MTPB.2014.001.04.05.G01_2016.0120_nc" },
            {   'input_prefix': "s3://romsaf-earth-ro-archive-untarred",
                'processing_center': "romsaf",
                'atm': "romsaf/download/metop/2014/atm_20140101_metop_R_2305_0010/2014-01-01/atm_20140101_040257_METB_G001_R_2305_0010.nc",
                'wet': "romsaf/download/metop/2014/wet_20140101_metop_R_2305_0010/2014-01-01/wet_20140101_040257_METB_G001_R_2305_0010.nc"}
            ]
    else:
        couplets = [
            {   'input_prefix': "s3://romsaf-earth-ro-archive-untarred",
                'processing_center': "romsaf",
                'atm': "romsaf/download/metop/2014/atm_20140101_metop_R_2305_0010/2014-01-01/atm_20140101_040257_METB_G001_R_2305_0010.nc",
                'wet': "romsaf/download/metop/2014/wet_20140101_metop_R_2305_0010/2014-01-01/wet_20140101_040257_METB_G001_R_2305_0010.nc"}
            ]

    test_couplets( couplets )


def test_bad():
    couplets = [
            {   'input_prefix': "s3://ucar-earth-ro-archive-untarred",
                'processing_center': "ucar",
                'atmPhs': "cosmic1/repro2013/level1b/2012/090/atmPhs_repro2013_2012_090/atmPhs_C001.2012.090.00.03.G22_2013.3520_nc",
                'atmPrf': "cosmic1/repro2013/level2/2012/090/atmPrf_repro2013_2012_090/atmPrf_C001.2012.090.00.03.G22_2013.3520_nc",
                'wetPrf': "cosmic1/repro2013/level2/2012/090/wetPrf_repro2013_2012_090/wetPrf_C001.2012.090.00.03.G22_2013.3520_nc" }
        ]

    test_couplets( couplets )

def test_champ():

    couplets = [
            {   'input_prefix': "s3://ucar-earth-ro-archive-untarred",
                'processing_center': "ucar",
                'atmPhs': "champ/repro2016/level1b/2002/083/atmPhs_repro2016_2002_083/atmPhs_CHAM.2002.083.02.59.G09_2016.2430_nc",
                'atmPrf': "champ/repro2016/level2/2002/083/atmPrf_repro2016_2002_083/atmPrf_CHAM.2002.083.02.59.G09_2016.2430_nc",
                'wetPrf': "champ/repro2016/level2/2002/083/wetPf2_repro2016_2002_083/wetPf2_CHAM.2002.083.02.59.G09_2016.2430_nc" }
        ]

    test_couplets( couplets )

def test_jpl():

    couplets = [

            {   'input_prefix': "s3://jpl-earth-ro-archive-untarred", 
                'processing_center': "jpl", 
                'calibratedPhase': "cosmic1/calibratedPhase/2009/06/03/calibratedPhase_cosmic1_jpl_v2.6_cosmic1c1-G02-200906030937.nc", 
                'refractivityRetrieval': "cosmic1/refractivityRetrieval/2009/06/03/refractivityRetrieval_cosmic1_jpl_v2.6_cosmic1c1-G02-200906030937.nc", 
                'atmosphericRetrieval': "cosmic1/atmosphericRetrieval/2009/06/03/atmosphericRetrieval_cosmic1_jpl_v2.6_cosmic1c1-G02-200906030937.nc" }, 

            {   'input_prefix': "s3://ucar-earth-ro-archive-untarred",
                'processing_center': "ucar",
                'atmPhs': "cosmic1/repro2013/level1b/2009/154/atmPhs_repro2013_2009_154/atmPhs_C001.2009.154.09.37.G02_2013.3520_nc", 
                'atmPrf': "cosmic1/repro2013/level2/2009/154/atmPrf_repro2013_2009_154/atmPrf_C001.2009.154.09.37.G02_2013.3520_nc", 
                'wetPrf': "cosmic1/repro2013/level2/2009/154/wetPrf_repro2013_2009_154/wetPrf_C001.2009.154.09.37.G02_2013.3520_nc" }, 

            {   'input_prefix': "s3://romsaf-earth-ro-archive-untarred",
                'processing_center': "romsaf",
                'atm': "romsaf/download/cosmic/2009/atm_20090603_cosmic_R_2304_0010/2009-06-03/atm_20090603_093752_C001_G002_R_2304_0010.nc", 
                'wet': "romsaf/download/cosmic/2009/wet_20090603_cosmic_R_2304_0010/2009-06-03/wet_20090603_093752_C001_G002_R_2304_0010.nc" }, 

            {   'input_prefix': "s3://eumetsat-earth-ro-archive-liveupdate",
                'processing_center': "eumetsat",
                'level1b': "untarred/cosmic1/postProc/level1b/2009/154/IGOR_postProc_2009_154/IGOR_1B_C01_20090603093736Z_20090603093911Z_R_O_20201206114911Z_G02_NN_0100.nc" }, 

            {   'input_prefix': "s3://jpl-earth-ro-archive-untarred", 
                'processing_center': "jpl", 
                'calibratedPhase': "champ/calibratedPhase/2003/06/03/calibratedPhase_champ_jpl_v2.6_champ-G10-200306032355.nc", 
                'refractivityRetrieval': "champ/refractivityRetrieval/2003/06/03/refractivityRetrieval_champ_jpl_v2.6_champ-G10-200306032355.nc", 
                'atmosphericRetrieval': "champ/atmosphericRetrieval/2003/06/03/atmosphericRetrieval_champ_jpl_v2.6_champ-G10-200306032355.nc" }

            ]

    test_couplets( couplets )

def debug_jpl():

    couplets = [

            {   'input_prefix': "s3://jpl-earth-ro-archive-untarred", 
                'processing_center': "jpl", 
                'atmosphericRetrieval': "cosmic1/atmosphericRetrieval/2009/09/21/atmosphericRetrieval_cosmic1_jpl_v2.6_cosmic1c1-G18-200909210935.nc" }, 

            {   'input_prefix': "s3://jpl-earth-ro-archive-untarred", 
                'processing_center': "jpl", 
                'atmosphericRetrieval': "cosmic1/atmosphericRetrieval/2009/09/21/atmosphericRetrieval_cosmic1_jpl_v2.6_cosmic1c1-G21-200909210922.nc" }, 

            {   'input_prefix': "s3://jpl-earth-ro-archive-untarred", 
                'processing_center': "jpl", 
                'atmosphericRetrieval': "cosmic1/atmosphericRetrieval/2009/09/21/atmosphericRetrieval_cosmic1_jpl_v2.6_cosmic1c1-G30-200909210941.nc" } 

            ]

    test_couplets( couplets )

def test_spire():

    couplets = [
            {   'input_prefix': "s3://ucar-earth-ro-archive-liveupdate/untarred",
                'processing_center': "ucar",
                'atmPhs': "spire/noaa/nrt/level1b/2022/150/conPhs_nrt_2022_150/conPhs_S125.2022.150.07.18.R18_0001.0001_nc",
                'atmPrf': "spire/noaa/nrt/level2/2022/150/atmPrf_nrt_2022_150/atmPrf_S125.2022.150.07.18.R18_0001.0001_nc",
                'wetPrf': "spire/noaa/nrt/level2/2022/150/wetPf2_nrt_2022_150/wetPf2_S125.2022.150.07.18.R18_0001.0001_nc" },
            {   'input_prefix': "s3://ucar-earth-ro-archive-liveupdate/untarred",
                'processing_center': "ucar",
                'atmPhs': "spire/noaa/nrt/level1b/2022/150/conPhs_nrt_2022_150/conPhs_S125.2022.150.07.16.E33_0001.0001_nc",
                'atmPrf': "spire/noaa/nrt/level2/2022/150/atmPrf_nrt_2022_150/atmPrf_S125.2022.150.07.16.E33_0001.0001_nc",
                'wetPrf': "spire/noaa/nrt/level2/2022/150/wetPf2_nrt_2022_150/wetPf2_S125.2022.150.07.16.E33_0001.0001_nc" }
        ]

    test_couplets( couplets )

def test_cosmic2():

    couplets = [
            {   'input_prefix': "s3://ucar-earth-ro-archive-liveupdate", 
                'processing_center': "ucar",
                'atmPhs': "untarred/cosmic2/nrt/level1b/2022/010/conPhs_nrt_2022_010/conPhs_C2E1.2022.010.00.00.G10_0001.0001_nc" }, 
            {   'input_prefix': "s3://ucar-earth-ro-archive-liveupdate", 
                'processing_center': "ucar",
                'atmPhs': "untarred/cosmic2/nrt/level1b/2022/195/conPhs_nrt_2022_195/conPhs_C2E1.2022.195.00.28.G23_0001.0001_nc" }, 
            {   'input_prefix': "s3://ucar-earth-ro-archive-liveupdate", 
                'processing_center': "ucar",
                'atmPhs': "untarred/cosmic2/nrt/level1b/2022/221/conPhs_nrt_2022_221/conPhs_C2E5.2022.221.00.17.R11_0001.0001_nc" } 
        ]

    test_couplets( couplets )


def test_tsx():

    couplets = [
            {   'input_prefix': "s3://ucar-earth-ro-archive-untarred",
                'processing_center': "ucar", 
                'atmPhs': "tsx/postProc/level1b/2018/152/atmPhs_postProc_2018_152/atmPhs_TSRX.2018.152.07.06.G01_2018.3000_nc", 
                'atmPrf': "tsx/postProc/level2/2018/152/atmPrf_postProc_2018_152/atmPrf_TSRX.2018.152.07.06.G01_2018.3000_nc", 
                'wetPrf': "tsx/postProc/level2/2018/152/wetPf2_postProc_2018_152/wetPf2_TSRX.2018.152.07.06.G01_2018.3000_nc" }, 
            {   'input_prefix': "jpl", 
                'processing_center': "jpl", 
                'calibratedPhase': "tsx/calibratedPhase/2018/06/01/calibratedPhase_tsx_jpl_v2.6_tsx-G01-201806010706.nc", 
                'refractivityRetrieval': "tsx/refractivityRetrieval/2018/06/01/refractivityRetrieval_tsx_jpl_v2.6_tsx-G01-201806010706.nc", 
                'atmosphericRetrieval': "tsx/atmosphericRetrieval/2018/06/01/atmosphericRetrieval_tsx_jpl_v2.6_tsx-G01-201806010706.nc" }
        ]

    test_couplets( couplets )


def test_eci2ecf():

    couplets = [
        {   'input_prefix': "s3://ucar-earth-ro-archive-untarred",
            'processing_center': "ucar",
            'atmPhs': "tsx/postProc/level1b/2019/002/atmPhs_postProc_2019_002/atmPhs_TSRX.2019.002.08.06.G20_2018.3000_nc", 
            'atmPrf': "tsx/postProc/level2/2019/002/atmPrf_postProc_2019_002/atmPrf_TSRX.2019.002.08.06.G20_2018.3000_nc", 
            'wetPrf': "tsx/postProc/level2/2019/002/wetPrf_postProc_2019_002/wetPrf_TSRX.2019.002.08.06.G20_2018.3000_nc" }, 
        {   'input_prefix': "workingdir", 
            'processing_center': "ucar", 
            'atmPhs': "cosmic2/nrt/level1b/2022/306/conPhs_nrt_2022_306/conPhs_C2E4.2022.306.10.44.G11_0001.0001_nc"
        }
        ]

    test_couplets( couplets, output_prefix="workingdir" )



def test_cloudstreaming( output_prefix = "s3://gnss-ro-data-test/zarr/v1.0" ):

    input_roots = [
            "s3://gnss-ro-data-staging/contributed/v1.0/ucar/champ/calibratedPhase/2002/01/01",
            "s3://gnss-ro-data-staging/contributed/v1.0/ucar/champ/refractivityRetrieval/2002/01/01",
            "s3://gnss-ro-data-staging/contributed/v1.0/ucar/champ/atmosphericRetrieval/2002/01/01" ]

    for input_root in input_roots:
        create_zarr_files( input_root, output_prefix, clobber=True )

    return

def test_readcloudstreaming( output_prefix = "s3://gnss-ro-data-test/zarr/v1.0" ):

    if output_prefix[:5] == "s3://":
        fs = s3fs.S3FileSystem( profile="aernasaprod", client_kwargs={'region_name':"us-east-1"} )
        store = s3fs.S3Map( root=output_prefix[5:], s3=fs, check=False )
        root = zarr.group( store=store )
    else:
        root = zarr.open( output_prefix, mode='r' ).group()

    root.tree()
    return

def test_romsaficdr( output_prefix = "s3://gnss-ro-data/test/contributed/v1.1" ): 

    session = boto3.Session( profile_name="aernasaprod", region_name="us-east-1" )
    dbtable = "gnss-ro-test"
    ret = batchprocess( "atm_atm_20170223_000048_META_G008_I_2320_0010.json", dbtable, output_prefix=output_prefix, session=session )
    return

def test_cosmic1_ucar_level2a(): 

    job_definitions_file = "s3://gnss-ro-processing-definitions/batchprocess-jobs/ucar-cosmic1-refractivityRetrieval.000001.json"
    session = boto3.Session( profile_name="aernasaprod", region_name="us-east-1" )
    ret = batchprocess( job_definitions_file, version, session=session, clobber=True )
    return

def test_cosmic1_ucar_level2b(): 

    job_definitions_file = "s3://gnss-ro-processing-definitions/batchprocess-jobs/ucar-cosmic1-atmosphericRetrieval.000001.json"
    session = boto3.Session( profile_name="aernasaprod", region_name="us-east-1" )
    ret = batchprocess( job_definitions_file, version, session=session, clobber=True )
    return

def missing_setting( jsonfile="missing_setting.json" ): 
    """Find the DynamoDB database entries of RO data for which the "setting" 
    information is not present."""

#  Define transmitters and receivers. For now, only COSMIC1 and Metop. 

    transmitters = [ f"G{i:02d}" for i in range(1,33) ]
    receivers = [ f"cosmic1c{i:1d}" for i in range(1,7) ] + \
            [ "metopa" + "metopb" + "metopc" ]

#  AWS session, DynamoDB object. 

    if default_profile is None: 
        session = boto3.Session( region_name=default_region )
    else: 
        session = boto3.Session( profile_name=default_profile, region_name=default_region )

    db = session.resource( "dynamodb" ).Table( dynamodb_table )

    #  Search only for those soundings without the setting flag provided. 

    filters = Attr("setting").eq("") & ~Attr("receiver").eq("") & ~Attr("transmitter").eq("")

    #  Initialize. 

    items = []

    for year in range(2006,2023): 
        sortkey = Key('date-time').between( f"{year:4d}-01-01", f"{year+1:4d}-01-01" )
        yearitems = []
        for receiver in receivers: 
            for transmitter in transmitters: 
                partitionkey = Key('leo-ttt').eq( f"{receiver}-{transmitter}" )
                ret = db.query( KeyConditionExpression=partitionkey&sortkey, FilterExpression=filters )
                yearitems += ret['Items']
        print( "For year {:} found {:} items in total".format( year, len(yearitems) ) )
        items += yearitems

    print( "Found {:} items in total".format( len(items) ) )

    #  Replace Decimal with float. 

    for item in items: 
        conversion = {}
        for key, value in item.items(): 
            if isinstance( value, Decimal ): 
                conversion.update( { key: float( value ) } )
        item.update( conversion )

    with open( jsonfile, 'w' ) as fp: 
        json.dump( items, fp, indent="  " )

    return items


def analyze_update(): 
    """Check to see if reprocessing to get setting flag in database based on ROMSAF 
    translations worked."""

    date1 = datetime( year=2006, month=4, day=26 )
    date2 = date1 + timedelta(days=1)
    ucar_filetypes = set( [ "ucar_calibratedPhase", "ucar_refractivityRetrieval", "ucar_atmosphericRetrieval" ] )

    if aws_profile is None: 
        s3 = s3fs.S3FileSystem( client_kwargs={'region_name':aws_region} )
        db = boto3.Session( region_name=aws_region ).resource( "dynamodb" ).Table( dynamodb_table )
    else: 
        s3 = s3fs.S3FileSystem( profile=aws_profile, client_kwargs={'region_name':aws_region} )
        db = boto3.Session( profile_name=aws_profile, region_name=aws_region ).resource( "dynamodb" ).Table( dynamodb_table )

    date1str = "{:4d}-{:02d}-{:02d}".format( date1.year, date1.month, date1.day )
    date2str = "{:4d}-{:02d}-{:02d}".format( date2.year, date2.month, date2.day )

    #  Get listing of all ROMSAF atmosphericRetrieval files. 

    remotedir = os.path.join( staging_bucket, "contributed/v1.1/romsaf/cosmic1/atmosphericRetrieval", 
            "{:4d}/{:02d}/{:02d}".format( date.year, date.month, date.day ) )
    listing = [ f for f in s3.ls(remotedir) if re.search( "\.nc$", f ) ]

    #  Loop over files. 

    for file in listing: 
        head, tail = os.path.split( file )
        m = re.search( "_([a-zA-Z0-9]+)-([GRECJS]\d{2})-(\d{12})\.nc$", tail )
        receiver, transmitter, timestamp = m.group(1), m.group(2), m.group(3)
        year, month, day = int(timestamp[0:4]), int(timestamp[4:6]), int(timestamp[6:8])
        hour, minute = int(timestamp[8:10]), int(timestamp[10:12])

        message = f"File {tail}:\n"

        #  Query database for this occultation. 

        partitionkey = Key('leo-ttt').eq( f"{receiver}-{transmitter}" )
        sortkey = Key('date-time').eq( f"{year:04d}-{month:02d}-{day:02d}-{hour:02d}-{minute:02d}" )

        ret = db.query( KeyConditionExpression = partitionkey & sortkey )

        if ret['Count'] == 0: 
            message += "*** not found in database"
            print( message )
            continue

        item = ret['Items'][0]

        if item['setting'] == "": 
            message += "*** setting not set\n"

        if len( ucar_filetypes.intersection( item.keys() ) ) == 0: 
            message += "*** romsaf only"

        conversion = { key: float(value) for key, value in item.items() if isinstance(value,Decimal) }
        item.update( conversion )
        message += json.dumps( item, indent="  " ) + "\n"

        print( message )


def test_romsaf(): 

    jsonfile = "test_romsaf.json"
    dbtable = "gnss-ro-test"
    session = boto3.session.Session( profile_name=default_profile, region_name=default_region )

    #  Create job definitions for CDR. 

    jobdefinitions = { 
        'InputPrefix': "s3://romsaf-earth-ro-archive-untarred/romsaf/download", 
        'ProcessingCenter': "romsaf", 
        'InputFiles': [
            "cosmic/2012/atm_20120102_cosmic_R_2304_0010/2012-01-02/atm_20120102_034257_C001_G001_R_2304_0010.nc", 
            "cosmic/2012/wet_20120103_cosmic_R_2304_0010/2012-01-03/wet_20120103_031222_C001_G001_R_2304_0010.nc", 
            "metop/2008/atm_20080602_metop_R_2305_0010/2008-06-02/atm_20080602_005934_META_G003_R_2305_0010.nc", 
            "metop/2008/wet_20080602_metop_R_2305_0010/2008-06-02/wet_20080602_005934_META_G003_R_2305_0010.nc"
        ]
    }

    #  Write definitions to JSON driver. 

    with open( jsonfile, 'w' ) as fp: 
        json.dump( jobdefinitions, fp )

    #  Batchprocess. 

    version = Versions.get_version( "1.1" )
    ret = batchprocess( jsonfile, version, session=session, workingdir="workingdir", clobber=True )

    version = Versions.get_version( "2.0" )
    ret = batchprocess( jsonfile, version, session=session, workingdir="workingdir", clobber=True )

    #  Create job definitions for ICDR. 

    jobdefinitions = { 
        'InputPrefix': "s3://romsaf-earth-ro-archive-liveupdate/untarred", 
        'ProcessingCenter': "romsaf", 
        'InputFiles': [
            "metop/2022/atm_20220124_metop_I_3102_0012/2022-01-24/atm_20220124_003334_METC_G030_I_3102_0012.nc", 
            "metop/2022/wet_20220124_metop_I_3102_0012/2022-01-24/wet_20220124_003334_METC_G030_I_3102_0012.nc"
        ]
    }

    #  Write definitions to JSON driver. 

    with open( jsonfile, 'w' ) as fp: 
        json.dump( jobdefinitions, fp )

    #  Batchprocess. 

    version = Versions.get_version( "1.1" )
    ret = batchprocess( jsonfile, version, session=session, workingdir="workingdir", clobber=True )

    version = Versions.get_version( "2.0" )
    ret = batchprocess( jsonfile, version, session=session, workingdir="workingdir", clobber=True )

    return

def test_eumetsat(): 

    jsonfile = "test_eumetsat.json"
    dbtable = "gnss-ro-test"
    session = boto3.session.Session( profile_name=default_profile, region_name=default_region )

    #  Create job definitions. 

    jobdefinitions = { 
        'InputPrefix': "s3://eumetsat-earth-ro-archive-liveupdate/untarred", 
        'ProcessingCenter': "eumetsat", 
        'InputFiles': [
#           "champ/repro2016/level1b/2003/033/BJxx_repro2016_2003_033/BJxx_1B_CHA_20030202010305Z_20030202010526Z_R_O_20201129184219Z_G09_ND_0100.nc", 
#           "grace/postProc/level1b/2010/195/BJxx_postProc_2010_195/BJxx_1B_GR1_20100714043139Z_20100714043325Z_R_O_20201130045226Z_G16_NN_0100.nc", 
            "metopa/postProc/level1b/2015/194/GRAS_postProc_2015_194/GRAS_1B_M02_20150713221255Z_20150713221255Z_R_O_20201204232042Z_G09_ND_0300.nc", 
            "metopb/postProc/level1b/2017/255/GRAS_postProc_2017_255/GRAS_1B_M01_20170912111221Z_20170912111444Z_R_O_20201202133942Z_G11_NN_0300.nc", 
            "metopc/postProc/level1b/2020/122/GRAS_postProc_2020_122/GRAS_1B_M03_20200501232630Z_20200501232840Z_R_O_20201220030633Z_G21_NN_0300.nc"
        ]
    }

    with open( jsonfile, 'w' ) as fp: 
        json.dump( jobdefinitions, fp )

    version = Versions.get_version( "1.1" )
    ret = batchprocess( jsonfile, version, session=session, workingdir="workingdir", clobber=True )

    version = Versions.get_version( "2.0" )
    ret = batchprocess( jsonfile, version, session=session, workingdir="workingdir", clobber=True )

    return


if __name__ == "__main__":
#   pdb.set_trace()
#   test_eci2ecf()
#   test_champ()
#   test_spire()
#   test_metop()
#   test_cosmic2()
#   test_tsx()
#   test_bad()
#   test_cloudstreaming()
#   test_cloudstreaming( "zarr" )
#   test_readcloudstreaming()
#   test_romsaficdr()
#   items = missing_setting()
#   analyze_update()
#   test_romsaf()
#   test_romsaf()
#   test_cosmic1_ucar_level2a()
#   test_cosmic1_ucar_level2b()
#   test_eumetsat()
    test_jpl()
#   debug_jpl()

    pass
