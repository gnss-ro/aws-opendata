# AWS GNSS-RO Utilities

This package &mdash; **awsgnssroutils** &mdash; contains two utilities 
intended to serve and take advantage of the GNSS radio occultation data 
in the AWS Registry of Open Data. The first utility is a database utility, 
which queries the RO database for occultations that satisfy a variety 
of search criteria. The second utility is a highly efficient collocation 
finder, which finds nadir-scanning radiance sounder data that is coincident 
with GNSS RO data. 

A jupyter notebook is provided for introduction to this package as 
[awsgnssroutils_demonstration](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/awsgnssroutils_demonstration.ipynb). 

# Installation

This package can be installed from PyPI

```
pip install awsgnssroutils
```

## Database Functionality

This module defines two classes: RODatabaseClient and OccList. The first
creates a portal to a database of RO metadata, and the second is an instance
of a list of radio occultations (ROs). Each are described below.  

## RODatabaseClient:

Create an instance of a portal to a metadata on all RO data in the AWS
Registry of Open Data. This tool is made efficient by storing 
metadata used in previous queries on the local file system. Subsequent 
queries will find this metadata on the local file system and will not 
spend valuable wall clock time repeating the download of the metadata 
of interest. The same holds true for RO data files themselves: previously 
downloaded data are stored on the local file system. The user is free to 
clear out these local "mirrors" of the metadata and data, but this will 
penalize future efficiency for the gain of local disk space. 

### Setting defaults

Before proceeding to use the query utility, be sure to set defaults for 
the database query system...

```
> from awsgnssroutils.database import setdefaults
> setdefaults( "/my/path/to/RO/metadata", "/my/path/to/RO/data", version="v1.1" )
```

All queries will attempt to access metadata in the root path of the first 
argument in this call, and all attempts to download RO data files will 
attempt to find the requested files in the root path of the second 
argument of the call. (Of course, rename these paths to paths defined 
as you wish.) At this point, the only version of RO data published in the 
AWS Registry of Open Data is "v1.1", so be certain this is the version of 
RO data you specify when executing setdefaults. 

A user can choose to prepopulate all RO metadata, thereby guaranteeing 
great efficiency in all queries. Prepopulating can take up to ten minutes, 
but it only needs to be executed once; perhaps more often if the user 
needs to refresh the metadata. Prepopulating the metadata is done as...

```
from awsgnssroutils.database import populate
populate()
```

If a user erases metadata files in the metadata root path, future queries 
will still function correctly, but they will have to repopulate the metadata, 
thereby greatly increasing the wall clock time of queries. 

### Executing queries

All queries to the AWS repository of RO data is done through an instance 
of the RODatabaseClient class, which serves as a portal to the AWS 
repository. The results of queries are instances of class OccList. 

Create an instance of the portal class by...

```
> from awsgnssroutils.database import RODatabaseClient
> rodb = RODatabaseClient()
```

It is designed to auto re-authenticate. If authentication is needed 
for access to an AWS S3 bucket, this class will automatically 
re-authenticate without user interference. 

creates a database interface directly to the AWS S3 bucket to access
the metadata. This interface is slow but requires no local disk space.

By specifying "update" as True, the local repository is updated at the
instantiation of rodb. The update compares metadata in the repository
of metadata on the local file system to the same metadata files in the
AWS Registry of Open Data and updates the local metadata as needed.
The update does not add any "new" metadata files to the local repository.   

There are two methods to create a list of occultations through the
database client. One is to perform an inquiry in which missions and/or
a date-time range is specified, and a second is to restore a previously
saved list of RO data.

```
> occlist = rodb.query( missions="champ" )
```

generates an OccList containing metadata on all CHAMP RO data. The inquiry
can be performed instead over a range in time. The date-time fields are
always ISO format times...

```
> occlist = rodb.query( datetimerange=("2019-06-01","2019-06-30") )
```

creates an OccList of metadata for all RO soundings in the month of June,
2019, regardless of mission.

The other option to creating an OccList is be restoring a previously
saved OccList:

```
> occlist = rodb.restore( "old_occlist.json" )
```

in which the old OccList was saved in a JSON format file.

## OccList

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
> champoccs = rodb.query( missions="champ" )
> champoccs_2003 = champoccs.filter( datetimerange=("2003-01-01","2004-01-01") )
```

illustrates how to apply a filter in date-time, retaining all CHAMP RO
metadata for the year 2003. Filtering can be done in longitude and latitude
as well:

```
> champoccs_US = champoccs.filter( longituderange=(-110,-70), latituderange=(25,55) )
```

and even those can be subset by local time (a.k.a. solar time):

```
> champoccs_US_midnight = champoccs_US.filter( localtimerange=(22,2) )
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
> spire = rodb.query( "spire" )
> spire_receivers = spire.info( "receiver" )
```

The first step in this process could be time consuming if the Spire
metadata do not already reside on the local file system and the rodb object
does not interface with a local repository. One can also get a list of the
GNSS transmitters tracked by Spire on a particular day by

```
> spire_day = spire.filter( datetimerange=("2021-12-01","2021-12-02") )
> spire_day_transmitters = spire_day.info("transmitter")
```

which will give a list of all GNSS transmitters tracked by all Spire
satellites on December 1, 2021. The spire\_day list can be split up between
rising and setting RO soundings as well:

```
> spire_day_rising = spire_day.filter( geometry="rising" )
> spire_day_setting = spire_day.filter( geometry="setting" )
```

Then it is possible to save the spire metadata OccList to a JSON file
for future restoration by

```
> spire.save( "spire_metadata.json" )
```

The metadata also contain pointers to the RO sounding data files in the
AWS Open Data bucket. To get information on the data files available,
use the OccList.info( "filetype" ) method. For example, to find out the
types of RO data files avialable for the month of June, 2009:

```
> June2009 = rodb.query( datetimerange=("2009-06-01","2009-07-01") )
> filetype_dict = June2009.info( "filetype" )
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
> longitudes = June2009.values( "longitude" )  
> latitudes = June2009.values( "latitude" )  
> localtimes = June2009.values( "localtime" )  
```

each of these variables being a masked numpy ndarray.  

Finally, RO data files themselves can be downloaded for subsequent
scientific analysis using the OccList.download() method. If one wishes to
download the all RO bending angle data contributed by JPL to the archive
for the week of June 5-11, 2012, one only need execute the commands

```
> week_list = rodb.query( datetimerange=("2012-06-05","2012-06-12") )
> week_list.download( "jpl_refractivityRetrieval", "datadir" )
```

which will download all file type "refractivityRetrieval" contributed by
JPL into the directory "datadir". All of the files will be entered into
just one directory. If instead one wants to download the files maintaining
the AWS directory structure, use the keyword "keep\_aws\_structure" in the
method call:

```
> week_list.download( "jpl_refractivityRetrieval", "datadir", \
        keep_aws_structure=True )
```
