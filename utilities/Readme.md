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

There are two files of Python code that illustrate how data in the Open Data 
Registry of GNSS RO data can be accessed and manipulated. The first is 
**dynamodbdemonstration.py**. It contains three functions that illustrate 
how the DynamoDB database of GNSS RO data can be manipulated. The major 
functionality illustrated in the code is use of the __query__ DynamoDB 
method. It always requires a precisely defined partition key as the first 
argument and a loosely defined sort key as the second 
