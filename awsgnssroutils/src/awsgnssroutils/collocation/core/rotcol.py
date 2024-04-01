import argparse
from awsgnssroutils.database import valid_table 
from awsgnssroutils.collocation.instruments import instruments

def main(): 

    #  Choices and defaults. 

    version = "v1.1"

    all_valid_missions = sorted( list( set( [ item['mission'] for item in valid_table 
            if item['version']==version and item['filetype'] == "refractivityRetrieval" ] ) ) )

    all_valid_centers = sorted( list( set( [ item['center'] for item in valid_table 
            if item['version']==version and item['filetype'] == "refractivityRetrieval" ] ) ) )

    all_valid_instruments = sorted( list( instruments.keys() ) )

    all_valid_satellites = []
    for key, val in instruments.items(): 
        all_valid_satellites += val['valid_satellites']
    all_valid_satellites = sorted( list( set( all_valid_satellites ) ) )


    root_parser = argparse.ArgumentParser( prog='rotcol',
            description="""This program performs a collocation-finding calculation by between 
            GNSS radio occultation data and passive nadir-scanner sounding data by implementing 
            the rotation-collocation algorithm, generating NetCDF output containing the 
            data associated with the collocations.""" )

    root_parser.add_argument( "command", choices=("setdefaults","execute"), 
            help="""Function to undertake. If "setdefaults", the user can define the defaults 
            for subscriptions to the EUMETSAT Data Store, the NASA Earthdata services, 
            Space-Track (for orbital two-line element -- TLE) data, and for AWS radio 
            occultation storage locations on the local file system. If "execute", the user 
            intends to execute a rotation-collocation calculation.""" )

    setdefaults_parser = argparse.ArgumentParser( parents=[root_parser] )

    setdefaults_parser.add_argument( "service", type=str, choices=("eumetsat", "earthdata", "space-track"), 
            help="""The service for which to set defaults. "eumetsat" refers to the EUMETSAT Data 
            Store, in which case the user should consider entering the ConsumerKey and SecretKey that 
            can be obtained from the EUMETSAT Data Store website. "earthdata" refers to the NASA 
            Earthdata portal to the NASA DAACs, in which case the user should consider providing 
            the username and password for the user's account in NASA Earthdata. "space-track" 
            refers to the Space-Track portal for orbital TLE data, in which case the user should 
            consider providing the username and password for the user's Space-Track account.""" )

    #  Define the setdefaults parser. 

    setdefaults_parser.add_argument( "--dataroot", dest="dataroot", default=None, type=str, required=False, 
            help="""Path on the local file system where data should be downloaded and stored 
            for future reference (and efficiency)""" )

    setdefaults_parser.add_argument( "--username", dest="username", default=None, type=str, required=False, 
            help="""Username for the Earthdata or Space-Track account""" )

    setdefaults_parser.add_argument( "--password", dest="password", default=None, type=str, required=False, 
            help="""Password for the Earthdata or Space-Track account""" )

    setdefaults_parser.add_argument( "--consumer-key", dest="consumer_key", default=None, type=str, required=False, 
            help="""ConsumerKey as provided by the EUMETSAT Data Store""" )

    setdefaults_parser.add_argument( "--consumer-secret", dest="consumer_secret", default=None, type=str, required=False, 
            help="""ConsumerSecret as provided by the EUMETSAT Data Store""" )

    #  Define the rotation-collocation parser. 

    collocation_parser = argparse.ArgumentParser( parents=[root_parser] )

    collocation_parser.add_argument( "gnssro_missions", dest="missions", type=str, choices=all_valid_missions, 
            default="cosmic1 cosmic2 metop", 
            help="""A list of GNSS radio occultation missions to draw from, separated by white space""" )

    collocation_parser.add_argument( "timerange", type=str, 
            help="""Two ISO-format UTC datetimes defining the time interval over which to search for 
            collocations, separated by spaces""" )

    collocation_parser.add_argument( "nadir_instrument", type=str, choices=all_valid_instruments, 
            help="""Name of the nadir-scanning instrument""" )

    collocation_parser.add_argument( "nadir_satellite", type=str, choices=all_valid_satellites, 
            help="""Name of the satellite hosting the nadir-scanning instrument""" )

    collocation_parser.add_argument( "--ro-center", dest="ro_processing_center", type=str, 
            choices=all_valid_centers, default="ucar", 
            help="""Name of the RO processing center from which to take data""" )

    #  Parse arguments. 

    root_args = root_parser.parse_args()

    if root_args.command == "setdefaults": 
        setdefaults_args = setdefaults_parser.parse_args()

        if setdefaults_args.service == "eumetsat": 
            args = {}
            if setdefaults_args.dataroot is not None: 
                args.update( { 'data_root': setdefaults_args.dataroot } )
            if setdefaults_args.consumer_key is not None and setdefaults_args.consumer_secret is not None: 
                args.update( { 'eumetsatkeys': ( setdefaults_args.consumer_key, setdefaults_args.consumer_secret ) } )
            print( "Updating EUMETSAT defaults: " + json.dumps( args ) )
            eumetsat_setdefaults( **args )
            return

        elif setdefaults_args.service == "earthdata": 

        elif setdefaults_args.service == "spacetrack": 

        else: 
            print( f'Invalid service: "{setdefaults_args.service}"' )
            return

    elif root_args.command == "execute": 
        collocation_args = collocation_parser.parse_args()

    else: 
        print( f'Invalid command: "{root_args.command}"' )
        return




if __name__ == "__main__": 
    main()
    pass

