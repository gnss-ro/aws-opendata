GNSS RO in AWS Utilities
============================================

**AWS Location**: s3://gnss-ro-data

**AWS Region**: us-east-1  

**Managing Organization**: Atmospheric and Environmental Research, Inc.

*Correspondence:* Stephen Leroy (sleroy@aer.com) or Amy McVey (amcvey@aer.com)


### Introduction

Utilities are provided here for an independent user to create his/her own
DynamoDB database of all RO data contained in the AWS Open Data Registry
archive of Earth RO data.
### Pre-requisits

In order for any of this to work, you must first get an account
with AWS and obtain up-to-date authentication tokens. Typically, these authentication
tokens will be referenced by a profile name, and that profile name should be set
in the configuration section of the header of the import_gnss-ro_dynamoDB.py script.
The variable that should be set is "aws_profile".

### Python environment

The specific packages required for each utility is listed in-line for each respective file. To
easily create an environment to run the included utilities please see the
[Install_python_miniconda_linux.sh](http://github.com/gnss-ro/aws-opendata/blob/master/utilities/Install_python_miniconda_linux.sh)
bash script. This will install the latest python version via Miniconda and the necessary
python packages.

### Implement DynamoDB table

A utility is provided to assist you in creating your own DynamoDB database
table given the JSON files hosted in *s3://gnss-ro-data/dynamo_export_subsets*.
There is a faster more efficient way however complex, using AWS DataPipeline.  We have
also created a python script that uses boto3 to loop through and create the database one item at a time.
However this second way is very time consuming to recreate the entire table, but great for partial chunks.

#### AWS DataPipeline:
This option will import the full table to your AWS DynamoDB within a few hours.
The instructions to set the up are included here in "DynamoDB_full_import_instructions.txt". These instructions
assume you have full access to your AWS account.

To update it you can use the Python script below and simply type in the most recent month or day.
For example:

```
python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name my_ro_database --date_str "202208"
```

#### Python and DynamoDB
The script is **import_gnss-ro_dynamoDB.py**. It creates a DynamoDB database
given an RO mission name and optionally a date range for RO soundings. For example,
in order to import all CHAMP RO data into your own DynamoDB table, execute the command

```
python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name my_ro_database --mission champ
```

In order to import just one year of CHAMP entries into your database, try

```
python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name my_ro_database --mission champ --date_str "2003"
```

for all of year 2003. Note that you can define the table name of your own database in place of
"my_ro_database". To import just one month of CHAMP entries in the database, try

```
python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name my_ro_database --mission champ --date_str "2003-02"
python3 import_gnss-ro_dynamoDB.py --dynamodb_table_name my_ro_database --mission champ --date_str "2003-02-14"
```

If you wish to import all entries into your database, ...

```
python3 import_gnss-ro_dynamoDB.py --full
```

but this operation can take a very long time, up to several days.


### Database design and usage

A DynamoDB database is premised on the usage of "partition" and "sort" keys. Together, they uniquely
define an RO sounding. In this case, the partition key is "leo-ttt" where "leo" is the low-Earth-orbiting
receiver name and "ttt" is the GNSS transmitter identifier. The sort key is "yyyy-mm-dd-hh-mm" (year,
month, day, hour, minute) of the RO sounding. See the main
[Readme document](http://github.com/gnss-ro/aws-opendata/blob/master/Readme.md). Any query of the database requires
a unique specification of the partition key and at least a partial definition of the sort key. Each
entry (for a unique radio occultation sounding) contains information on the time, longitude, latitude,
solar time (local time) of the occultation, whether it is a rising or setting occultation, and pointers to the
various data files in the S3 bucket.

For an explicit demonstration of how to use the DynamoDB database, see the
[tutorial demonstrations](http://github.com/gnss-ro/aws-opendata/tree/master/tutorials).
