GNSS RO in AWS Tutorials
============================================

**AWS Location**: s3://gnss-ro-data

**AWS Region**: us-east-1  

**Managing Organization**: Atmospheric and Environmental Research, Inc.

*Correspondence:* Stephen Leroy (sleroy@aer.com) or Amy McVey (amcvey@aer.com)


### Introduction

In this directory you can find tutorial software in Python that illustrates
the use of GNSS RO data in the AWS Open Data Registry. Note that before any 
of this code can be run, you must first create your own version of the 
DynamoDB database of the RO data in this archive. For instructions on how 
to do so, go to the [utilities](http://github.com/gnss-ro/aws-opendata/utilities) 
and follow the instructions in the Readme document therein. 

In order to access the RO data in the s3://gnss-ro-data Open Data 
archive, you must execute the tutorial demonstrations in an AWS computing 
environment---such as a WorkSpace, EC2 instance, or Batch---and be sure 
you have appropriate authentication to access (and create) your own version 
of the DynamoDB database. Moreover, the computing environment and database 
must be manifested in the same AWS region as the Open Data bucket, which is 
defined in the preamble above. Ordinarily, your authentication will be 
assigned a profile name, which is called "aws_profile" in the tutorial code. 
Be sure that this is defined correctly. 

There are two Python modules that illustrate how data in the Open Data
Registry of GNSS RO data can be accessed and manipulated. In each case,
be sure to edit the configuration in the preamble of the module in order to
assure the authentication required for you to access your implementation of
the DynamoDB database.

### Python environment

Be certain that your Python installation is version 3.8 or later 
and that the following modules are installed: numpy, scipy, netCDF4, 
matplotlib, cartopy, and boto3. For each of the tutorials listed below, 
be certain that the "aws_region" is correct and that "dynamodb_table" 
points to the DynamoDB table you created from the s3://gnss-ro-data 
archive. Most importantly, be sure that the "aws_profile" points toward 
the correct profile of AWS credentials and that the credentials tokens 
are current. 

### DynamoDB database tutorial

The first python module is 
[dynamodb_demonstration.py](http://github.com/gnss-ro/aws-opendata/tutorials/dynamodb_demonstration.py). 
It contains three
functions that illustrate how the DynamoDB database of GNSS RO data can be
manipulated. The major functionality illustrated in the code is use of the
*query* DynamoDB method. It always requires a precisely defined partition
key and a sort key, the latter of which can be only partially defined. The 
first method, *occultation_count_by_mission*, computes a monthly tally of 
occultation count by RO mission. It shows how to query the table by mission, 
composing all possible partition keys for each mission. The output is 
written to a JSON file. The second method, *occultation_count_figure*, plots 
the results of *occultation_count_by_mission* as a matplotlib stack plot. 
The third method, *distribution_solartime_figure*, plots the distribution of 
RO soundings for a given year, month, and day in longitude-latitude space 
and in solar-time-latitude space. Note that *occultation_count_figure* must
be executed prior to calling *distribution_solartime_figure* in order to
compose the mission color table. The plots are all encapsulated postscript.

In order to run the code, be sure the configuration in the header is set to 
the correct values of "aws_profile", "aws_region", and "dynamodb_table". 

### Data analysis tutorial

The second python module is 
[data_analysis_demonstration.py](http://github.com/gnss-ro/aws-opendata/tutorials/data_analysis_demonstration.py). 
It contains
two functions: *compute_center_intercomparison*, which computes the
differences in bending angle, refractivity, dry temperature, temperature,
and specific humidity for a set of RO soundings that were processed by
both UCAR and ROM SAF for a particular day of COSMIC-1 data; and
*center_intercomparison_figure*, which plots the results as matplotlib
box-and-whisker plots.

As above, be sure the configuration in the header is set to the correct 
values of "aws_profile", "aws_region", and "dynamodb_table". 

### Questions? Comments? 

For comments and questions, please send correspondence to those listed 
in the preamble. 

