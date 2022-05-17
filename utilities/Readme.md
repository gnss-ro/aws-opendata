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

## Tutorial software

There are two Python modules code that illustrate how data in the Open Data 
Registry of GNSS RO data can be accessed and manipulated. In each case, 
be sure to edit the configuration in the preamble of the module in order to 
assure the authentication required for you to access your implementation of 
the DynamoDB database. 

The first python module is **dynamodb_demonstration.py**. It contains three 
functions that illustrate how the DynamoDB database of GNSS RO data can be 
manipulated. The major functionality illustrated in the code is use of the 
__query__ DynamoDB method. It always requires a precisely defined partition 
key and a sort key, which can be only partially defined. The first method, 
__occultation_count_by_mission__, computes a monthly tally of occultation 
count by RO mission. It shows how to query the table by mission, composing 
all possible partition keys for each mission. The output is written to a 
JSON file. The second method, __occultation_count_figure__, plots the results 
of __occultation_count_by_mission__ as a matplotlib stack plot. The third 
method, __distribution_solartime_figure__, plots the distribution of RO 
soundings for a given year, month, and day in longitude-latitude space and 
in solar-time-latitude space. Note that __occultation_count_figure__ must 
be executed prior to calling __distribution_solartime_figure__ in order to 
compose the mission color table. The plots are all encapsulated postscript. 

The second python module is **data_analysis_demonstration.py**. It contains 
two functions: __compute_center_intercomparison__, which computes the 
differences in bending angle, refractivity, dry temperature, temperature, 
and specific humidity for a set of RO soundings that were processed by 
both UCAR and ROM SAF for a particular day of COSMIC-1 data; and 
__center_intercomparison_figure__, which plots the results as matplotlib 
box-and-whisker plots. 

