GNSS RO in AWS Utilities
============================================

**AWS Location**: s3://gnss-ro-data

**AWS Region**: us-east-1  

**Managing Organization**: Atmospheric and Environmental Research, Inc.

*Correspondence:* Stephen Leroy (sleroy@aer.com) or Amy McVey (amcvey@aer.com)


# Introduction

In this directory you can find tutorial software in Python that illustrates
the use of GNSS RO data in the AWS Open Data Registry as well as a utility
for converting the JSON data in the Open Data Registry bucket (listed above)
into an AWS DynamoDB database table.

Note that before any of this code can be run, you must first establish
authentication to your own AWS account in order to create and access your
own DynamoDB table. In order to gain free access to the AWS Open Data
Registry archive of RO data, you must establish your AWS region associated
with your database to be the same as that of the "AWS Region" defined above
and you must execute the computations in an AWS computing environment
configured in the same region. Once the authentication is established, you
can then define a name for the DynamoDB table you wish to create, associate
your AWS authentication as an "aws_profile", and define each in the
preamble of the code contained herein.

## Implement DynamoDB table

A utility is provided to assist you in creating your own DynamoDB database
table given the JSON files hosted in *s3://gnss-ro-data/dynamo_export_subsets*.
That utility is **import_gnss-ro_dynamoDB.py**. It creates a DynamoDB database
given an RO mission name and optionally a date range for RO soundings. See
the in-line documentation for usage examples.

## Tutorial software

There are two Python modules code that illustrate how data in the Open Data
Registry of GNSS RO data can be accessed and manipulated. In each case,
be sure to edit the configuration in the preamble of the module in order to
assure the authentication required for you to access your implementation of
the DynamoDB database.

The first python module is **dynamodb_demonstration.py**. It contains three
functions that illustrate how the DynamoDB database of GNSS RO data can be
manipulated. The major functionality illustrated in the code is use of the
*query* DynamoDB method. It always requires a precisely defined partition
key and a sort key, which can be only partially defined. The first method,
*occultation_count_by_mission*, computes a monthly tally of occultation
count by RO mission. It shows how to query the table by mission, composing
all possible partition keys for each mission. The output is written to a
JSON file. The second method, *occultation_count_figure*, plots the results
of *occultation_count_by_mission* as a matplotlib stack plot. The third
method, *distribution_solartime_figure*, plots the distribution of RO
soundings for a given year, month, and day in longitude-latitude space and
in solar-time-latitude space. Note that *occultation_count_figure* must
be executed prior to calling *distribution_solartime_figure* in order to
compose the mission color table. The plots are all encapsulated postscript.

The second python module is **data_analysis_demonstration.py**. It contains
two functions: *compute_center_intercomparison*, which computes the
differences in bending angle, refractivity, dry temperature, temperature,
and specific humidity for a set of RO soundings that were processed by
both UCAR and ROM SAF for a particular day of COSMIC-1 data; and
*center_intercomparison_figure*, which plots the results as matplotlib
box-and-whisker plots.

## Python Environment

The specific packages required for each utility is listed in-line for each respective file.  
To easily create an encironment to run the included utilities please see the
**Install_python_miniconda_linux.sh** bash script.  This will install the latest
python version via Miniconda and the necessary python packages.
