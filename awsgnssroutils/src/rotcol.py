import argparse
import json
from datetime import datetime
from time import time

from awsgnssroutils.database import valid_table 
from awsgnssroutils.collocation.instruments import instruments

def execute_rotation_collocation( missions, timerange, ro_processing_center, 
        nadir_instrument, nadir_satellite, outputfile ): 

    #  Initialize return structure. 

    ret = { 'status': None, 'messages': [], 'comments': [] }

    #  Relevant imports. 

    from awsgnssroutils.database import RODatabaseClient
    from awsgnssroutils.collocation.core.spacetrack import Spacetrack
    from awsgnssroutils.collocation.core.rotation_collocation import rotation_collocation

    #  Initialize RO database client and access to Space-Track data. 

    rodb = RODatabaseClient()
    strack = Spacetrack()

    #  Instantiate the nadir-scanner instrument. 

    if nadir_satellite in [ "Aqua", "Suomi-NPP", "JPSS-1", "JPSS-2" ]: 

        from awsgnssroutils.collocation.core.nasa_earthdata import NASAEarthdata
        nasa_earthdata_access = NASAEarthdata()
        inst = instruments[nadir_instrument]['class']( nadir_satellite, nasa_earthdata_access, spacetrack=strack )

    elif nadir_satellite in [ "Metop-A", "Metop-B", "Metop-C" ]: 

        from awsgnssroutils.collocation.core.eumetsat import EUMETSATDataStore
        eumetsat_data_store = EUMETSATDataStore()
        inst = instruments[nadir_instrument]['class']( nadir_satellite, eumetsat_data_store, spacetrack=strack )

    #  Collocation tolerances. 

    time_tolerance = 600                           # 10 min/600 sec
    spatial_tolerance = 150.0e3                    # m

    #  Get occultation geolocations. 

    print( "Querying occultation database" )

    tbegin = time()
    occs = db.query( missions=ro_mission, datetimerange=[ dt.isoformat() for dt in datetimerange ], 
                availablefiletypes=f'{ro_processing_center}_refractivityRetrieval', silent=True )
    tend = time()

    print( "  - number found = {:}".format( occs.size ) )
    print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

    #  Exercise rotation-collocation. 

    print( "Executing rotation-collocation" )

    tbegin = time()
    ret_rotation = rotation_collocation( inst, occs, time_tolerance, spatial_tolerance, 2 )
    tend = time()

    ret['messages'] += ret_rotation['messages']
    ret['comments'] += ret_rotation['comments']

    if ret_rotation['status'] == "fail": 
        ret['status'] = "fail"
        print()
        return ret

    collocations_rotation = ret_rotation['data']

    print( "  - number found = {:}".format( len( collocations_rotation ) ) )
    print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

    #  Populate instrument data. 

    tbegin = time()
    ret = inst.populate( datetimerange )
    tend = time()

    ret['messages'] += ret_rotation['messages']
    ret['comments'] += ret_rotation['comments']

    if ret_rotation['status'] == "fail": 
        ret['status'] = "fail"
        print()
        return ret

    print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

    #  Extract data. 

    print( "Extracting collocation data" )

    tbegin = time()
    for collocation in collocations_rotation: 
        occid = collocation.get_data( ro_processing_center )
    tend = time()

    print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

    #  Save to output file. 

    tbegin = time()
    print( f"Writing to output file {outputfile}" )
    collocations_rotation.write_to_netcdf( file )
    tend = time()

    print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

    #  Done. 

    ret['status'] = "success" 

    return ret


def main(): 

    #  Choices and defaults. 

    version = "v1.1"
    output_default = "output.nc"
    ro_processing_center_default = "ucar"

    all_valid_missions = sorted( list( set( [ item['mission'] for item in valid_table 
            if item['version']==version and item['filetype'] == "refractivityRetrieval" ] ) ) )

    all_valid_centers = sorted( list( set( [ item['center'] for item in valid_table 
            if item['version']==version and item['filetype'] == "refractivityRetrieval" ] ) ) )

    all_valid_instruments = sorted( list( instruments.keys() ) )

    all_valid_satellites = []
    for key, val in instruments.items(): 
        all_valid_satellites += val['valid_satellites']
    all_valid_satellites = sorted( list( set( all_valid_satellites ) ) )

    #  Define the parent (root) parser. 

    parser = argparse.ArgumentParser( prog='rotcol', 
            description="""This program performs a collocation-finding calculation by between 
            GNSS radio occultation data and passive nadir-scanner sounding data by implementing 
            the rotation-collocation algorithm, generating NetCDF output containing the 
            data associated with the collocations. The user must set various defaults 
            (setdefaults) before executing (execute) a rotation-collocation computation.""" )

    subparsers = parser.add_subparsers( dest="command" )

    #  Define the setdefaults parser. 

    setdefaults_parser = subparsers.add_parser( "setdefaults", 
            help="""
            The command "setdefaults" will sent default paths for data, metadata, and 
            login fields (username, password, etc.) for access to various services. Those 
            services and their associated fields are...
            (1) "awsro" refers to the AWS Registry of Open Data repository for GNSS radio 
            occultation data. If requested, the user should provide the dataroot and the 
            metadataroot paths where the data and RO metadata should be stored on the local 
            file system.
            (2) "eumetsat" refers to the EUMETSAT Data Store. If requested, the user 
            should consider entering the dataroot where EUMETSAT Data Store should be stored 
            on the local file system; also, the ConsumerKey and SecretKey that provides online 
            access to the EUMETSAT Data Store, which can be obtained from the EUMETSAT Data 
            Store website. 
            (3) "earthdata" refers to the NASA Earthdata portal to the NASA DAACs. If 
            requested, the user should consider entering the dataroot where Earthdata data 
            should be stored on the local file system; also the username and password for the 
            user's account in NASA Earthdata.
            (4) "spacetrack" refers to the Space-Track portal for orbital TLE data. If 
            requested, the user should consider entering the dataroot where Space-Track TLE 
            data should be stored on the local file system; also, the username and password 
            for the user's Space-Track account.""" )

    setdefaults_parser.add_argument( "service", type=str, choices=("awsro", "eumetsat", "earthdata", "spacetrack"), 
            help="""The service for which to set defaults. awsro = RO data in the AWS 
            Registry of Open data; eumetsat = the EUMETSAT Data Store, which hosts data 
            from the Metop satellites; earthdata = the NASA Earthdata DAACS, which hosts 
            data from NASA satellites, including the JPSS satellites; spacetrack = satellite 
            orbital data hosted by Space-Track.""" )

    setdefaults_parser.add_argument( "--dataroot", dest="dataroot", default=None, type=str, required=False, 
            help="""Path on the local file system where data should be downloaded and stored 
            for future reference (and efficiency)""" )

    setdefaults_parser.add_argument( "--metadataroot", dest="metadataroot", default=None, type=str, required=False, 
            help="""Path on the local file system where metadata should be downloaded and stored 
            for future reference (and efficiency)""" )

    setdefaults_parser.add_argument( "--username", dest="username", default=None, type=str, required=False, 
            help="""Username for the Earthdata or Space-Track account""" )

    setdefaults_parser.add_argument( "--password", dest="password", default=None, type=str, required=False, 
            help="""Password for the Earthdata or Space-Track account""" )

    setdefaults_parser.add_argument( "--consumer-key", dest="consumer_key", default=None, type=str, required=False, 
            help="""ConsumerKey as provided by the EUMETSAT Data Store""" )

    setdefaults_parser.add_argument( "--consumer-secret", dest="consumer_secret", default=None, type=str, required=False, 
            help="""ConsumerSecret as provided by the EUMETSAT Data Store""" )

    #  Define the execution parser. 

    collocation_parser = subparsers.add_parser( "execute", 
            help="""Execute a rotation-collocation calculation to find 
            collocations between GNSS radio occultation soundings and a selected 
            nadir-scanning instrument. In this the user must list the RO missions 
            from which to draw data, what nadir-scanning instrument on what satellite 
            to search for collocated sounder data, and over what time period 
            collocations are being sought.""" )

    collocation_parser.add_argument( "missions", type=str, choices=all_valid_missions, 
            default="cosmic1 cosmic2 metop", 
            help="""A list of GNSS radio occultation missions to draw from, separated by white space""" )

    collocation_parser.add_argument( "timerange", type=str, 
            help="""Two ISO-format UTC datetimes defining the time interval over which to search for 
            collocations, separated by spaces""" )

    collocation_parser.add_argument( "nadir_instrument", type=str, choices=all_valid_instruments, 
            help="""Name of the nadir-scanning instrument""" )

    collocation_parser.add_argument( "nadir_satellite", type=str, choices=all_valid_satellites, 
            help="""Name of the satellite hosting the nadir-scanning instrument""" )

    collocation_parser.add_argument( "--output", "-o", type=str, default=output_default, 
            help=f'Output file name for collocation data; by default "{output_default}".' )

    collocation_parser.add_argument( "--ro-center", dest="ro_processing_center", type=str, 
            choices=all_valid_centers, default=ro_processing_center_default, 
            help='Name of the RO processing center from which to take data; by default ' + \
                    f'"{ro_processing_center_default}".' )

    #  Parse arguments. 

    args = parser.parse_args()

    if args.command == "setdefaults": 

        #  Set defaults. 

        if args.service == "awsro": 

            from awsgnssroutils.database import setdefaults

            kwargs = { 'version': "v1.1" }
            if args.dataroot is not None: 
                kwargs.update( { 'data_root': args.dataroot } )
            if args.metadataroot is not None: 
                kwargs.update( { 'metadata_root': args.dataroot } )

            print( "Updating AWS RO defaults: " + json.dumps( kwargs ) )
            setdefaults( **kwargs )

        elif args.service == "eumetsat": 

            from awsgnssroutils.collocation.core.eumetsat import setdefaults

            kwargs = {}
            if args.dataroot is not None: 
                kwargs.update( { 'root_path': args.dataroot } )
            if args.consumer_key is not None and args.consumer_secret is not None: 
                kwargs.update( { 'eumetsattokens': ( args.consumer_key, args.consumer_secret ) } )

            print( "Updating EUMETSAT defaults: " + json.dumps( kwargs ) )
            setdefaults( **kwargs )

        elif args.service == "earthdata": 

            from awsgnssroutils.collocation.core.nasa_earthdata import setdefaults

            kwargs = {}
            if args.dataroot is not None: 
                kwargs.update( { 'root_path': args.dataroot } )
            if args.consumer_key is not None and args.consumer_secret is not None: 
                kwargs.update( { 'eumetsattokens': ( args.consumer_key, args.consumer_secret ) } )

            print( "Updating NASA Earthdata defaults: " + json.dumps( kwargs ) )
            setdefaults( **kwargs )

        elif args.service == "spacetrack": 

            from awsgnssroutils.collocation.core.spacetrack import setdefaults

            kwargs = {}
            if args.dataroot is not None: 
                kwargs.update( { 'root_path': args.dataroot } )
            if args.username is not None and args.password is not None: 
                kwargs.update( { 'spacetracklogin': ( args.username, args.password ) } )

            print( "Updating Space-Track defaults: " + json.dumps( kwargs ) )
            setdefaults( **kwargs )

        else: 

            print( f'Invalid service: "{args.service}"' )

    elif args.command == "execute": 

        #  Execute rotation-collocation algorithm for collocation finding. 

        #  Get arguments for rotation-collocation: missions, timerange, nadir_satellite, 
        #  nadir_instrument, ro_processing_center. 

        missions = re.split( "\s+", args.missions )

        ss = re.split( "\s+", args.timerange )
        timerange = ( datetime.fromisoformat( ss[0] ), datetime.fromisoformat( ss[1] ) )

        nadir_satellite = str( args.nadir_satellite )

        nadir_instrument = str( args.nadir_instrument )

        ro_processing_center = str( args.ro_processing_center )

        #  Check for valid satellite-instrument combination. 

        if nadir_satellite in instruments[nadir_instrument]['valid_satellites']: 

            ret = execute_rotation_collocation( missions, timerange, ro_processing_center, 
                    nadir_instrument, nadir_satellite )

            if ret['status'] == "fail": 
                print( "messages = " + ", ".join( ret['messages'] ) + "\n" )
                for comment in ret['comments']: 
                    print( comment )

        else: 

            print( f"""Satellite {nadir_satellite} has no instrument {nadir_instrument}, 
                or at least it is not registered in this package. The instrument 
                {nadir_instrument} has been incorporated for the following satellites: """ + \
                ", ".join( instruments[nadir_instrument]['valid_satellites'] ) )

    else: 
        print( f'Invalid command: "{root_args.command}"' )
        return


if __name__ == "__main__": 
    main()
    pass

