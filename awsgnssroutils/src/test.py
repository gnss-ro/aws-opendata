#  Access to RO data, Celestrak TLEs, and NASA Earthdata DAAC

from awsgnssroutils.database import RODatabaseClient
from awsgnssroutils.collocation.core.spacetrack import Spacetrack
from awsgnssroutils.collocation.core.nasa_earthdata import NASAEarthdata

db = RODatabaseClient()
st = Spacetrack()
nasa_earthdata_access = NASAEarthdata()

#  Define JPSS-1 ATMS instrument. 

from awsgnssroutils.collocation.instruments import instruments

JPSS1_ATMS = instruments['JPSS_ATMS']['class']( "JPSS-1", nasa_earthdata_access, spacetrack=st )

#  Time interval for collocation finding. 

from datetime import datetime, timedelta

day = datetime( year=2023, month=6, day=5 )
nextday = day + timedelta(days=1)
datetimerange = ( day, nextday )

#  Collocation tolerances. 

time_tolerance = 600                           # 10 min/600 sec
spatial_tolerance = 150.0e3                    # m

#  Get occultation geolocations. 

ro_processing_center = "ucar"
ro_mission = "cosmic2"

from time import time

print( "Querying occultation database" )

tbegin = time()
occs = db.query( missions=ro_mission, datetimerange=[ dt.isoformat() for dt in datetimerange ], 
                availablefiletypes=f'{ro_processing_center}_refractivityRetrieval', silent=True )
tend = time()

print( "  - number found = {:}".format( occs.size ) )
print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

#  Exercise rotation-collocation. 

from awsgnssroutils.collocation.core.rotation_collocation import rotation_collocation

print( "Executing rotation-collocation" )

tbegin = time()
collocations_rotation = rotation_collocation( JPSS1_ATMS, occs, 
        time_tolerance, spatial_tolerance, 2 )
tend = time()

print( "  - number found = {:}".format( len( collocations_rotation ) ) )
print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

#  Populate ATMS data. 

tbegin = time()
JPSS1_ATMS.populate( datetimerange )
tend = time()

print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

#  Extract data. 

print( "Extracting collocation data" )
tbegin = time()
for collocation in collocations_rotation: 
    occid = collocation.get_data( ro_processing_center )
tend = time()
print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )
cdata = collocations_rotation[0].data

#  Save to output file. 

file = "cosmic2_collocations.nc"
tbegin = time()
print( f"Writing to output file {file}" )
collocations_rotation.write_to_netcdf( file )
tend = time()
print( "  - elapsed time = {:10.3f} s".format( tend-tbegin ) )

