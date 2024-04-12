# AWS GNSS RO Utilities

This package &mdash; **awsgnssroutils** &mdash; contains two utilities 
intended to serve and take advantage of the GNSS radio occultation (RO) data 
in the AWS Registry of Open Data. The first is a database utility, 
which queries the RO database for occultations that satisfy a variety 
of search criteria. The second is a highly efficient collocation-finding 
utility, which finds nadir-scanning radiance sounder data that is coincident 
with GNSS RO data. 

This package can be installed from PyPI. On the Linux command line...

```
pip install awsgnssroutils
```

**Contents**
1. [First steps](#1-first-steps). These are necessary first steps for basic functionality. 
2. [Database utility](#2-database-utility). Query, subset, and download RO data. 
3. [Collocation utility](#3-collocation-utility). Find nadir radiance scanner data collocated with RO data. 


## 1. First Steps 
Before proceeding to use the query utility, be sure to set defaults for 
the database query system. In Python, 

```
from awsgnssroutils.database import setdefaults
setdefaults( metadata_root="/my/path/to/RO/metadata", data_root="/my/path/to/RO/data", version="v1.1" )
```

All queries will attempt to access metadata in a directory hierarchy 
whose root is the first argument in this call, and all attempts to 
download RO data files will attempt to find the requested files in a 
directory hierarchy whose root is the second argument of the call. 
(Of course, rename these root paths to the desired root paths on your 
own file system.) At this point, the only version of RO data published in the 
AWS Registry of Open Data is "v1.1". 

A user can choose to prepopulate all RO metadata, thereby guaranteeing 
great efficiency in all queries. Prepopulating can take up to ten minutes, 
but it only needs to be executed once; perhaps more often if the user 
needs to refresh the metadata. Prepopulating the metadata is done in Python as...

```
from awsgnssroutils.database import populate
populate()
```

If a user erases metadata files in the metadata root path, future queries 
will still function correctly, but they will have to repopulate the metadata, 
thereby greatly increasing the wall clock time of queries. 

## 2. Database Utility

The module *awsgnssroutils.database* obtains RO metadata from the AWS 
Registry of Open Data if needed, queries the metadata according to 
a variety of conditions, permits filtering/subsetting of the results, and 
downloads RO data as desired. The module defines two classes that serve 
as its core engine: *RODatabaseClient* and *OccList*. The first creates a 
portal to a database of RO metadata, and the second is an instance of a 
list of ROs. Each are described below.  

### *RODatabaseClient*

Create an instance of a portal to a metadata on all RO data in the AWS
Registry of Open Data. This tool is made efficient by storing 
metadata used in previous queries on the local file system. Subsequent 
queries will find this metadata on the local file system and will not 
spend valuable wall clock time repeating the download of the metadata 
of interest. The same holds true for RO data files themselves: previously 
downloaded data are stored on the local file system. The user is free to 
clear out these local "mirrors" of the metadata and data, but this will 
penalize future efficiency for the gain of local disk space. 

**Executing queries.** All queries to the AWS repository of RO data are 
executed through an instance of the *RODatabaseClient* class. The results 
of queries are instances of class OccList. Create an instance of the 
portal class in Python as...

```
from awsgnssroutils.database import RODatabaseClient
rodb = RODatabaseClient()
```

in which *rodb* is an interface directly to the AWS S3 bucket to access
the metadata. If metadata has not been prepopulated, queries can be slow. 
If authentication is needed for access to an AWS S3 bucket, this class will 
automatically re-authenticate without user interference. 

There are two methods to create a list of occultations through the
database client. One is to perform an inquiry in which missions and/or
a date-time range is specified, and a second is to restore a previously
saved list of RO data. In Python, 

```
occlist = rodb.query( missions="champ" )
```

generates an OccList containing metadata on all CHAMP RO data. The inquiry
can be performed instead over a range in time. The date-time fields are
always ISO format times: 

```
occlist = rodb.query( datetimerange=("2019-06-01","2019-06-30") )
```

creates an OccList of metadata for all RO soundings in the month of June,
2019, regardless of mission.

The other option to creating an OccList is to restore a previously
saved OccList:

```
occlist = rodb.restore( "old_occlist.json" )
```

in which the old OccList was saved in a JSON format file.

A jupyter notebook is provided for introduction to the *database* module as 
[database_demonstration](http://github.com/gnss-ro/aws-opendata/blob/master/tutorials/database_demonstration.ipynb). 

### *OccList*

An instance of the class OccList is contains the metadata on a list of RO
soundings along with pointers to the RO data files in the AWS Registry of
Open Data S3 bucket. AWS functionality is completely embedded in the
methods of the OccList class. Those methods include the ability to
subset/filter the list according to geolocation and time,
GNSS transmitter/constellation, GNSS receiver, whether it is a rising or a
setting occultation, etc. It also includes the ability to combine
instances of OccList, save the OccList to a JSON format file for future
restoration by RODatabaseClient.restore, and even download RO data files.   

In order to filter an OccList previously generated by
RODatabaseClient.query or RODatabaseClient.restore, use the OccList.filter
method:

```
champoccs = rodb.query( missions="champ" )
champoccs_2003 = champoccs.filter( datetimerange=("2003-01-01","2004-01-01") )
```

illustrates how to apply a filter in date-time, retaining all CHAMP RO
metadata for the year 2003. Filtering can be done in longitude and latitude
as well:

```
champoccs_US = champoccs.filter( longituderange=(-110,-70), latituderange=(25,55) )
```

and even those can be subset by local time (a.k.a. solar time):

```
champoccs_US_midnight = champoccs_US.filter( localtimerange=(22,2) )
```

in which the local time range is given in hours and can wrap around
midnight. Other filter options are for the GNSS constellation used as
transmitters ("G" for GPS, "R" for GLONASS, "E" for Galileo, "C" for
BeiDou), for individual transmitters ("G01", etc.), for individual
receivers ("cosmic1c1", "metopb", etc.), and for occultation 'geometry'
("rising" vs. "setting").

One can get information on the metadata in an OccList using the
OccList.info method. For instance, if you want to get a listing of all of
the Spire receiver satellites, do

```
spire = rodb.query( "spire" )
spire_receivers = spire.info( "receiver" )
```

The first step in this process could be time consuming if the Spire
metadata do not already reside on the local file system and the rodb object
does not interface with a local repository. One can also get a list of the
GNSS transmitters tracked by Spire on a particular day by

```
spire_day = spire.filter( datetimerange=("2021-12-01","2021-12-02") )
spire_day_transmitters = spire_day.info("transmitter")
```

which will give a list of all GNSS transmitters tracked by all Spire
satellites on December 1, 2021. The spire\_day list can be split up between
rising and setting RO soundings as well:

```
spire_day_rising = spire_day.filter( geometry="rising" )
spire_day_setting = spire_day.filter( geometry="setting" )
```

Then it is possible to save the spire metadata OccList to a JSON file
for future restoration by

```
spire.save( "spire_metadata.json" )
```

The metadata also contain pointers to the RO sounding data files in the
AWS Open Data bucket. To get information on the data files available,
use the OccList.info( "filetype" ) method. For example, to find out the
types of RO data files avialable for the month of June, 2009:

```
June2009 = rodb.query( datetimerange=("2009-06-01","2009-07-01") )
filetype_dict = June2009.info( "filetype" )
```

which will return a dictionary with the AWS-native RO file types as keys
with corresponding values being the counts of each. The file types have the
format "{processing\_center}\_{file\_type}" in which "processing\_center" is an
RO processing center that contributed to the AWS repository ("ucar",
"romsaf", "jpl") and the "file\_type" is one of "calibratedPhase",
"refractivityRetrieval", or "atmosphericRetrieval".

The values of the longitude, latitude, datetime, and localtimes of the RO
soundings in an OccList can be obtained using the OccList.values() method:  

```
longitudes = June2009.values( "longitude" )  
latitudes = June2009.values( "latitude" )  
localtimes = June2009.values( "localtime" )  
```

each of these variables being a masked numpy ndarray.  

Finally, RO data files themselves can be downloaded for subsequent
scientific analysis using the OccList.download() method. If one wishes to
download the all RO bending angle data contributed by JPL to the archive
for the week of June 5-11, 2012, one only need execute the commands

```
week_list = rodb.query( datetimerange=("2012-06-05","2012-06-12") )
week_list.download( "jpl_refractivityRetrieval", data_root="datadir", keep_aws_structure=False )
```

which will download all file type "refractivityRetrieval" contributed by
JPL into the directory "datadir". All of the files will be entered into
just one directory. If instead one wants to download the files maintaining
the AWS directory structure, which is the default, set the keyword 
*keep\_aws\_structure* to True: 

```
week_list.download( "jpl_refractivityRetrieval", keep_aws_structure=True )
```

In this case, because the *data_root* was not specified, it used the 
data_root previously set by setdefaults as the default. 

## 3. Collocation Utility

This package includes a utility that finds nadir-scanner radiance soundings 
that are collocated with RO soundings. It implements the rotation-collocation 
algorithm, which greatly accelerates finding collocations by rotating the 
geolocations of RO soundings into the reference frame of a nadir-scanning 
instrument's scan pattern. Because the scan pattern is constantly moving 
(along with its host satellite), the rotation is time-dependent. The 
algorithm is fully documented in 
[a peer reviewed paper](http://doi.org/10.5194/amt-16-3345-2023). 

The rotation-collocation algorithm is composed of a large suite of low-level 
routines that perform various necessary tasks. If you wish to learn more 
about these low-level routines, consult the jupyter notebook 
[collocation_demonstration](http://github.com/gnss-ro/aws-opendata/blob/master/tutorials/collocation_demonstration.ipynb). 

Among those tasks are 
interfaces to multiple data sources, definitions of various instrument types, 
the SGP4 orbit propagator, implementation of the rotation-collocation method, 
data download capability, and collocation save capability. Several defaults 
must be set in advance if these algorithms are to work. 

### Set defaults

Several defaults must be set for access to various online 
data sources. The [Space-Track](http://www.space-track.org) site contains 
two-line element satellite orbit data that are used in the rotation-collocation 
algorithm. The user must establish an account---with username and password---on 
Space-Track.com. The same is true for access to Metop instrument data, which 
hosted on the EUMETSAT Data Store. In this case, the user will have to obtain 
a "consumer key" and a "consumer secret" after first obtaining an account. 
Again, the same is true for NASA Earthdata, which serves as an interface to 
data on the NASA Earth Science Data and Information Systems' DAACs. Finally, 
of course, the user must set the defaults for access to the AWS RO repository
as described above. **All but the AWS RO repository 
defaults will be stored in the file "~/.collocation", which is created with 
user read-write permissions only.** The AWS RO repository defaults 
are stored in "~/.awsgnssroutilsrc". Since the latter does not contain 
a password or secret key, it is assigned no exclusive access permissions. 

For Space-Track, obtain an account. After having done so, set the defaults 
associated with Space-Track data by 

```
from awsgnssroutils.collocation.core.spacetrack import setdefaults
setdefaults( root_path="/my/path/to/space-track/data", spacetracklogin=("my_username","my_password") )
```

This establishes the directory indicated by *root_path* as the root path where 
all TLE (two-line element) data obtained from Space-Track will be stored on the 
local file system and the username and password of your account with Space-Track. 
Once executed, all future access to Space-Track for orbit TLE data will function 
cleanly without interference from the user. 

For NASA Earthdata, obtain an account. Follow steps like those for Space-Track...

```
from awsgnssroutils.collocation.core.nasa_earthdata import setdefaults
setdefaults( root_path="/my/path/to/earthdata/data", earthdatalogin=("my_username","my_password") )
```

In this case, the username and password correspond to your NASA Earthdata account's 
username and password. 

For the EUMETSAT Data Store, obtain an account. Your account will grant you access 
tokens in the form of a consumer key and a consumer secret. You can find both on 
[this site](https://api.eumetsat.int/api-key/) after logging in to your EUMETSAT 
data store account. You can then set defaults for access to the EUMETSAT Data Store by...

```
from awsgnssroutils.collocation.core.eumetsat import setdefaults
setdefaults( root_path="/my/path/to/eumetsat/data", eumetsattokens=("consumer_key","consumer_secret") )
```

Set the consumer key and consumer secret to those granted you in your EUMETSAT account. 

### Executing rotation-collocation 

For ease of use, both a single function and a command line utility are provided. The function is 
*awsgnssroutils.collocation.rotcol.execute_rotation_collocation*, and the 
command line utility is *rotcol*. The latter is nothing more than the 
command line implementation of the former but with documentation that can be 
obtained with the "-h" or "--help" switches. 

**The function** is imported by 

```
from awsgnssroutils.collocation.rotcol import execute_rotation_collocation
```

The execution of the collocation is done by rotation-collocation. RO data is 
extracted from the AWS RO repository, sounder data is extracted from a EUMETSAT or 
a NASA data source according to the instrument, and RO and sounder data for those 
collocations are written to a NetCDF file. 

```
execute_rotation_collocation( missions, datetimerange, ro_processing_center, nadir_instrument, nadir_satellite, output_file )
```

The *missions* is a tuple/list of RO missions as defined for the AWS RO repository; 
*datetimerange* is a 2-element tuple/list containing ISO-format time strings that 
bound the period over which RO data are to be considered for collocation; 
*ro_processing_center* defines the RO processing centers whose RO data is to be 
used as the baseline (as contributed to the AWS RO repository); *nadir_instrument* is 
the nadir-scanning instrument that obtained the soundings to be collocated with the 
RO soundings; *nadir_satellite* is the name of the satellite that hosts the nadir-scanning 
instrument, and *output_file* is the NetCDF file where collocation data is to be written. 
The options for all of these arguments is fluid. At present, the RO processing center 
can be "ucar", "romsaf", or "jpl"; the nadir instrument can be one of the microwave 
instruments "AMSU-A" or "ATMS"; the nadir satellite can be "Metop-A", "Metop-B", "Metop-C" 
(for AMSU-A) or "Suomi-NPP", "JPSS-1", "JPSS-2" (for ATMS). Try

```
execute_rotation_collocation( "cosmic2", ( "2021-03-04", "2021-03-05" ), "ucar", "ATMS", "JPSS-1", "output.nc" )
```

to find all COSMIC-2 RO data that are collocated with JPSS-1 ATMS data for the date of March 4, 2021. 
Results will be written to "output.nc". 

**The command-line utility** is *rotcol*. Upon installation of the package, the script 
will be placed in the session PATH. The Linux command is 

```
rotcol -h
```

in order to obtain help. Note that it provides two major options: the is to allow the user 
to set defaults without having to do so in Linux; the second actually executes the 
rotation-collocation algorithm and extracts the collocated sounding data.

In order to set defaults, execute the following command to get help: 

```
rotcol setdefaults -h
```

You will see documentation that allows you to set your defaults (usernames, passwords, data root paths, etc.) 
for the AWS repository ("awsro"), the NASA Earthdata portal ("earthdata"), the EUMETSAT Data 
Store ("eumetsat"), and the Space-Track archive of satellites' orbital TLE data ("spacetrack"). 
In order to execute a rotation-collocation, you can get help documentation on doing so 
by 

```
rotcol execute -h
```

It is a front-end to the function *execute_rotation_collocation* that also provides 
current information on satellite instruments, satellite names, and available RO data. 
