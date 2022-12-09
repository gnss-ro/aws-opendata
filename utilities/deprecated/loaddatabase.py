import json
from pandas import DataFrame

def loadjsonfiles( jsonfiles ): 
    """Import a collection of JSON files containing the contents of the database
    of GNSS radio occultation data hosted in the AWS Open Data Registry. The 
    files should be downloaded from  the AWS S3 path 
    s3://gnss-ro-data/dynamo/v1.1/export_subsets/ and hosted locally. The 
    jsonfiles is a list of paths to those files in the local file system that 
    should be imported. A pandas.DataFrame is returned."""

    records = []

    for jsonfile in jsonfiles: 

        try: 
            fp = open( jsonfile, 'r' )
            lines = fp.readlines()
            fp.close()
            print( f"Importing {jsonfile}" )
        except: 
            print( f"File {jsonfile} is found or unreadable" )
            continue

        #  Read each line as an independent JSON record. 

        for line in lines: 
            entry = json.loads( line )
            record = {}

            #  Categorize "setting" as a Boolean, import all strings as 
            #  they are, and convert all numbers to floats. 

            for key1, val1 in entry.items(): 
                if key1 == "setting": 
                    record.update( { key1: ( val1['s'] == "True" ) } )
                else: 
                    for key2, val2 in val1.items(): 
                        if key2 == "s": 
                            record.update( { key1: str( val2 ) } )
                        elif key2 == "n": 
                            record.update( { key1: float( val2 ) } )
                        else: 
                            print( f'Unrecognized type "{key2}" for key "{key1}"' )

            #  Append this new entry. 

            records.append( record )

    #  Return result. 

    print( f"{len(records):d} records imported" )

    df = DataFrame( records )

    return df

