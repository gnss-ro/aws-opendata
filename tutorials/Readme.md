GNSS RO in AWS Tutorials
============================================

**AWS Location**: s3://gnss-ro-data

**AWS Region**: us-east-1  

**Managing Organization**: Atmospheric and Environmental Research, Inc.

*Correspondence:* Stephen Leroy (sleroy@aer.com) or Amy McVey (amcvey@aer.com)


## Introduction

In this directory you can find tutorial software in Python that illustrates
the use of GNSS RO data in the AWS Open Data Registry. Note that before any
of this code can be run, you must install
[awsgnssroutils.py](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/utilities/awsgnssroutils.py)
in your PYTHONPATH path.

There are two Python programs that illustrate how to manipulate GNSS RO
metadata in the Registry of Open Data for
GNSS RO data for basic diagnostics on profile counts and distribution.

## Python environment

Be certain that your Python installation is version 3.8 or later
and that the following modules are installed: numpy,
matplotlib, and cartopy.

### Stackplot of RO counts by mission

Before a stackplot of RO mean daily counts by mission can be generated, the
occultations must be counted! This can be a time-intensive operation, so it is
best done in parallel, breaking up the counting by year interval. For that reason,
we provide the [count_occultations](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/count_occultations)
program. It utilizes awsgnssroutils.RODatabaseClient under the hood and generates
a local repository of GNSS RO metadata in the directory ~/local/rodatabase.
Consider the following parallel processing, submitted as background jobs:
```
./count_occultations 1995 2004 count_occultations.1995-2004.json &
./count_occultations 2005 2014 count_occultations.2005-2014.json &
./count_occultations 2015 2022 count_occultations.2015-2022.json &
```
Incidentally, these commands will create a local repository of GNSS RO
metadata in ~/local/rodatabase that is complete from 1995 through 2022. These
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

### Daily RO distribution by mission

The second python program is
[plot_distribution_solartime](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/plot_distribution_solartime).
It generates an encapsulated postscript file with the longitude-latitude and
solar time-latitude distribution of occultations by mission for one day. For example,
```
./plot_distribution_solartime 2018-04-05
```
generates an encapsulated postscript file with longitude-latitude and local time-latitude
occultation locations for April 5, 2018. By default, output is sent to plot_distribution_solartime.eps.

### OPAC7 IROWG9 workshop jupyter notebook

The [opac7irowg9_workshop.ipynb](https://raw.githubusercontent.com/gnss-ro/aws-opendata/master/tutorials/opac7irowg9_workshop.ipynb) 
code was used during the 2022 workshop in Austria. It walked users through s3fs and a
program to perform queries of DynamoDB.
As it is based on querying an AWS DynamoDB database table, you will not be able to execute
this notebook without having first created your own private AWS DynamoDB table. Instructions for
doing so are in http://github.com/gnss-ro/aws-opendata/tree/master/utilities.

### Questions? Comments?

For comments and questions, please send correspondence to those listed
in the preamble.
