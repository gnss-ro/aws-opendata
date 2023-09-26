GNSS RO in AWS Tutorials
============================================

**AWS Location**: s3://gnss-ro-data

**AWS Region**: us-east-1  

**Managing Organization**: Atmospheric and Environmental Research, Inc.

*Correspondence:* Stephen Leroy (sleroy@aer.com) or Amy McVey (amcvey@aer.com)


## Introduction

In this directory you can find tutorial software in Python that illustrates
the use of GNSS RO data in the AWS Open Data Registry. There are three jupyter 
notebooks and a Python program that illustrate how to manipulate GNSS RO 
data and metadata in the Amazon Web Services (AWS) Registry of Open Data. 


## Python environment

Be certain to install awsgnssroutils, which now resides in the PyPI 
open source repository: 

```
% pip install awsgnssroutils
```

This pip installation requires Python 3.8+ and installs s3fs and numpy as 
dependencies if they are not already installed. The tutorial demonstrations 
described below also require installations of matplotlib and cartopy. 

### Simple introduction to awsgnssroutils.database

The awsgnssroutils provides a simple mechanism for querying RO sounding 
data according to RO mission, GNSS transmitter, low-Earth orbiting receiver, 
GNSS constellation tracked, longitude range, latitude range, range of 
date-times, range of local (solar) times, and occultation geometry 
(rising v. setting). It introduces two class definitions, one which creates 
a portal to RO metadata, and a second which allows occultation list 
filtering/subsetting, metadata retrieval, and download of RO data. The 
notebook ends with a simple use case, a scatter plot of RO sounding 
locations on a particular day, color-coded by RO mission. 

The package is designed to be simple to use, to be of general use, and to 
maximize efficiency of database management and querying. All AWS interactions 
are hidden "beneath the hood" so that users do not need to establish an 
AWS account, role, or services to use this facility. The querying language 
is complete enough and extensive enough to enable many research use cases. 
Lastly, a local demand-driven mirror of RO metadata is generated to 
maximize the efficiency of querying a large RO metadata database. 

The jupyter notebook is [awsgnssroutils_demonstration](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/awsgnssroutils_demonstration.ipynb). 

In order to greatly simplify use of the awsgnssroutils, be sure to 
initialize your local repository of previous RO database queries using 
the awsgnssroutils.database.initialize function. For example, 

```
from awsgnssroutils.database import initialize
initialize( "/home/myhome/local/rodatabase", rodata="/home/myhome/Data/rodata" )
```

will define the first argument as the directory in which a history of 
previous database queries will be stored and will define the second 
argument as the location where all RO data files will be downloaded 
by default. 

### RO processing center inter-comparison 

The AWS Registry of Open Data repository of GNSS RO data contains 
contributions from several different and (mostly) independent RO 
processing centers. The AWS repository has matched up all of the 
contributions such that the same RO sounding processed by different 
centers has the same occultation identifier ("occid"). Moreover, 
the metadata database has one record associated with each sounding 
with pointers to atmospheric profile data for the sounding contributed 
by multiple centers. This makes center inter-comparison research 
simple. A jupyter notebook demonstrates how center inter-comparison 
research can be done using the awsgnssroutils package and simple 
data analysis and plotting commands. 

The jupyter notebook is [intercomparison_demonstration](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/intercomparison_demonstration.ipynb). 

### Tropopause analysis

This jupyter notebook analyzes cold-point tropopause temperature, 
pressure, and geopotential height, separating a determination of 
a tropical tropopause from a determination of an extra-tropical 
tropopause according to pressure. It uses the awsgnssroutils package 
to download RO atmospheric retrieval from the AWS Registry of Open Data 
and subsequently reads the contents of those data files, scans for 
temperature minima, and plots a scatterplot of the cold-point tropopause 
on a latitude-height plot. 

The jupyter notebook is [tropopause_demonstration](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/tropopause_demonstration.ipynb). 

### Stackplot of RO counts by mission

Before a stackplot of RO mean daily counts by mission can be generated, the
occultations must be counted! This can be a time-intensive operation, so it is
best done in parallel, breaking up the counting by year interval. For that reason,
we provide the [count_occultations](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/count_occultations)
program. It utilizes the awsgnssroutils package and generates
a local repository of GNSS RO metadata in the directory ~/local/rodatabase.
Consider the following parallel processing, submitted as background jobs:
```
./count_occultations 1995 2004 count_occultations.1995-2004.json &
./count_occultations 2005 2014 count_occultations.2005-2014.json &
./count_occultations 2015 2022 count_occultations.2015-2023.json &
```
Incidentally, these commands will create a local repository of GNSS RO
metadata in ~/local/rodatabase that is complete from 1995 through 2023. These
commands might take a long time to run the first time, because they copy the RO
metadata from the AWS Registry of Open Data S3 bucket if they don't already exist
locally. Subsequent consultations of RO metadata, however, will run at least 10
times faster, precisely because the RO metadata will be local, in ~/local/rodatabase.

Once these jobs are completed, then you can use the command line
python executable
[plot_count_occultations](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/plot_count_occultations)
to generate an encapsulated postscript plot of the occultation counts by mission.
```
./plot_count_occultations count_occultations.*.json
```
By default, output is written to plot_count_occultations.eps.


### OPAC7 IROWG9 workshop jupyter notebook

The [opac7irowg9_workshop.ipynb](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/opac7irowg9_workshop.ipynb) 
code was used during the 2022 workshop in Austria. It walked users through s3fs and a
program to perform queries of DynamoDB.
As it is based on querying an AWS DynamoDB database table, you will not be able to execute
this notebook without having first created your own private AWS DynamoDB table. 

### Questions? Comments?

For comments and questions, please send correspondence to those listed
in the preamble.
