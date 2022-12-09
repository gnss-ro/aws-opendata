# GNSS RO in AWS Utilities

**AWS Location**: s3://gnss-ro-data

**AWS Region**: us-east-1  

**Managing Organization**: Atmospheric and Environmental Research, Inc.

*Correspondence:* Stephen Leroy (sleroy@aer.com) or Amy McVey (amcvey@aer.com)

============================================


## Introduction

A utility is provided in the Python module **awsgnssroutils.py** that enables a Python 
programmer to query the AWS GNSS radio occultation (RO) database, mirror the 
contents of the database 
to a local file system as needed, and download RO data files. Beneath its hood 
it interfaces directly with the GNSS RO metadata stored in the 
AWS Registry of Open Data repository of GNSS RO data. Use of the utility does not 
require access to an AWS account, and the user need not issue any commands 
directly by an AWS API. The metadata for GNSS RO have become voluminous, 
so first queries using the Python utility can be time consuming. If the user 
creates a local mirror of the RO metadata &mdash; which is extremely highly recommended &mdash;
subsequent queries of the same metadata will proceed a factor of 100 to 1000 
times faster than the first. 

If instead a user chooses to take advantage of the AWS DynamoDB service in order to 
perform queries, see 
[deprecated](https://github.com/gnss-ro/aws-opendata/blob/master/utilities/deprecated/). 
The DynamoDB service is a more efficient database service than the Python 
utility described herein, but considerable effort will be necessary to create a 
private copy of the DynamoDB table in the user's own AWS account, and the coding 
for DynamoDB database queries is more complicated than coding using the provided 
Python module. 

## Python database module

*Prerequisites:* numpy, boto3, s3fs

*Notebook:* 
The jupyter notebook 
[demonstration_notebook.ipynb](https://github.com/gnss-ro/aws-opendata/blob/master/utilities/demonstration_notebook.ipynb) 
demonstrates how to use 
the *RODatabaseClient* and *OccList* classes to query the RO database. 

The Python module is [awsgnssroutils.py](https://github.com/gnss-ro/aws-opendata/blob/master/utilities/awsgnssroutils.py). 
It contains the definitions of two object classes: *RODatabaseClient* 
and *OccList*. *RODatabaseClient* functions as a portal to a database of the 
metadata associated with all of the Earth GNSS RO data in the AWS Registry of 
Open Data. It contains two methods that generate metadata objects. (An RO 
metadata object contains all metadata of a set of GNSS RO soundings, including 
the satellites involved, their geolocations and time, whether they are rising 
or setting occultations, and pointers to the 
corresponding RO data in the AWS Registry of Open Data.)  The first method 
restores a previously saved metadata object, and the 
second method generated a new metadata object according to RO mission(s) and/or a 
range of date-times. The metadata objects come in the form of instances of 
*OccList*, which offers a method to further filter/subset the RO soundings they 
contain to create a new metadata object, a method to 
download RO data files, and a method to generate the values of RO metadata such as 
geolocation and local time.  Inline documentation in the classes 
*RODatabaseClass* and *OccList* is complete: executing `help(RODatabaseClient)`
or `help(OccList)` will provide thorough descriptions of usage. 

### RODatabaseClient

The *RODatabaseClient* class creates a portal to the database (of metadata) on 
RO in the AWS Registry of Open Data. Instantiation offers three keywords: 
"repository", "version", and "update". By default, the client establishes a 
connection to the metadata on RO contained in AWS Open Data and reads directly 
from the AWS S3 bucket s3://gnss-ro-data. This is an inefficient way to go 
about querying the database, which is rather large, so an option is provided 
to create a local repository of the RO database on the local file system. The 
local repository is not a complete mirror, however; it only stores the contents 
of the RO database locally that have been requested in prior queries. If the 
"repository" key is set to a local path, the contents of previous queries are 
stored in that path. Second, generally there are several versions of the 
AWS RO data, and this key allows the user to specify which version to access. 
(As of 23 Nov 2022, the most up-to-date version is "v1.1", the default.) 
Third, the user has the option to update the repository on the local file 
system if the latter are older than what is hosted in the AWS S3 bucket. Only 
those metadata files that exist in the local repository are updates; no new 
metadata files are downloaded. 

**We highly recommend that users create a repository of the metadata on their 
local file systems.**

```
from awsgnssroutils import RODatabaseClient
db = RODatabaseClient( repository="rodatabase" )
```
will create a gateway "db" to the RO database in the Open Data Registry and 
copy all that is retrieved by subsequent queries of the AWS database into 
the local directory "rodatabase". 

The instance of *RODatabaseClient* can then be used to create instances of 
*OccList* by either querying the database or by restoring a previously 
saved *OccList*. Any query must constrain a search by either a time range 
or by RO mission(s) or both. For example, one get query the database for all 
RO data over the year 2009 by 
```
occs2009 = db.query( datetimerange=("2009-01-01","2010-01-01") )
```
in which both elements of the 2-tuple for "datetimerange" are [ISO-format 
datetime strings](https://www.w3.org/TR/NOTE-datetime). A user can also 
query the database by mission(s) by 
```
occs = db.query( missions=("sacc","tsx","tdx") )
```
which will get metadata for all RO soundings by the RO missions SAC-C, 
TerraSAR-X, and TanDEM-X. Both "occs2009" and "occs" are instances of 
*OccList*. 

Use of the *RODatabaseClient.restore* method is illustrated below, 
in the description of the *OccList.save* method. 

### OccList

Once an instance of *OccList* is generated by *RODatabaseClient.query* or by 
*RODatabaseClient.restore*, many possible manipulation of that instance are 
made possible by the methods it contains. The methods allow for further 
filtering/subsetting of the instance, getting information on the instance, 
retrieving geolocation metadata on the RO soundings in the instance, downloading 
a desired file type (*calibratedPhase*, *refractivityRetrieval*, 
*atmosphericRetrieval*) of RO data from the AWS Registry of Open Data, and 
saving the instance to a JSON-format file for future restoration by the 
*RODatabaseClient.restore* method. 

Using "occs" as obtained above, that instance of RO metadata can be filtered in 
time, longitude, latitude, and local time by 
```
occs_2011_US_midnight = occs.filter( datetimerange=("2011-01-01","2012-01-01"), 
	longituderange=(-110,-70), latituderange=(25,55), localtimerange=(22,2) )
```
which filters "occs" to just the year 2011, over the contiguous U.S. by 
restricting longitude and latitude, and to just those soundings within two hours 
of local midnight. (The units of localtimerange values are hours, and the 
range can wrap around midnight, like longituderange can wrap around the date 
line.) It is also possible to filter by *transmitters* (GNSS transmitting 
satellites), by *GNSSconstellation* (e.g., "G" for GPS, "R" for GLONASS, etc.), 
and by sounding *geometry* ("rising" or "setting"). 

It is also possible to get metadata values from an *OccList* instance using the 
*OccList.info* method. For example one can retrieve a list of RO missions in the 
instance, a list of tracked GNSS transmitters, ranges of longitudes, latitudes, 
local times, date-times, etc. 
```
missions_in_occs2009 = occs2009.info( "mission" )
geometry_in_occs2009 = occs2009.info( "geometry" )
longituderange_in_occs2009 = occs2009.info( "longitude" )
```
The "missions_in_occs2009" is a list of missions in "occs2009"; "geometry_in_occs2009" 
is a dictionary with keywords "nrising" and "nsetting" that count the numbers of 
rising and setting RO soundings in "occs2009"; and "longituderange_in_occs2009" is a 
dictionary with keywords "min" and "max" that indicate the minimum and maximum 
longitude of the RO soundings in "occs2009". 

It is possible to get metadata values on geolocation from an *OccList* instance 
as well. For example, 
```
longitudes = occs2009.values( "longitude" )
latitudes = occs2009.values( "latitude" )
```
yields ndarrays of longitude and latitude for all RO soundings ever obtained in 
the year 2009.

Existing instances of *OccList* can be saved for future restoration using the 
*OccList.save* and the *RODatabaseClient.restore* methods. For example, 
```
occs_2011_US_midnight.save( "savefile.json" )
occs_restored = db.restore( "savefile.json" )
```
saves "occs_2011_US_midnight" to the JSON file "savefile.json" and restores it 
to the new *OccList* instance "occs_restored". 

Finally, it is possible to download RO data using the *OccList.download* method. 
In order to do so for 13 February 2009, for example, try 
```
occ_day = occs2009.filter( datetimerange=("2009-02-13","2009-02-14") )
occfiles = odd_day.download( "ucar_atmosphericRetrieval", "rodata", keep_aws_structure=True )
```
which downloads all *atmosphericRetrieval* files contributed by the UCAR 
RO processing center for one day, and does so in a way that maintains the same directory 
structure as in the AWS Registry of Open Data S3 bucket with the local directory 
"rodata" as the root of the download. 

