# Radio Occultation Reformatting and Cataloging (RORefCat) System

Author: Stephen Leroy (sleroy@aer.com)

Date: October 3, 2024


This software package possesses tools that ingest GNSS (Global Navigation Satellite 
Systems) radio occultation (RO) data produced by a wide range of RO missions and 
processed by several independent algorithms/centers, reformats the data into 
standard formats, matches up RO soundings across centers, and catalogs metadata for 
each RO sounding. The software is configured to allow easy extension to new 
contributing processing centers and new missions. Templates are provided for each. 

The package consists of three sub-packages: **Database**, **GNSSSatellites**, 
**Missions**, **Reformatters**, **Utilities**, **Versions**, and **Webscrape**. 
Each is documented below. **Reformatters** provides the reformatting code necessary 
for each contributing processing center, **Versions** provides version-specific 
templates for the standard formats to be generated, **Missions** documents 
RO tracking algorithms and naming conventions for each ingested RO mission, 
**GNSSSatellites** provides utilities for bookkeeping the history of GNSS 
transmitter satellites, **Database** provides wrappers for cataloging RO 
metadata and documenting the paths to retrievals for individual occultations, 
**Utilities** provides fundamental utilities for RO reformatting as well as 
the command line executables, and **Webscrape** provides webscrapting 
utiltiies. 

**Caveat:**  By default, this code will run in operational mode. If you 
wish to run in a test environment, set the environment variable _TEST_ to 
a non-empty quantity. 

### Reformatters

Each module in Reformatters contains reformatting code relevant to a 
single processing center. The name of the module is the processing center
with ".py". It uses templates provided by Versions.get\_version for the output 
file. The module "template.py" defines a template for a new contributing 
retrieval center. See "ucar.py" for reformatting code relevant to retrievals 
generated by the UCAR COSMIC Program Office. 

The reformatting of excess phase data is a function _level1b2aws_ in 
the module, bending angle and refractivity data is _level2a2aws_, and 
1DVar thermodynamic retrieval data is _level2b2aws_. The module must also 
provide a function _varnames_ that extracts important meta information 
relevant to the occultation from the input file name. 

## Versions

Each module in Versions provides templates for reformatted level 1b, 
level 2a, and level 2b (reformatted) output files. In order to invent 
a new output format, simply provide a new module. 

Follow the code in version1.py or version2.py as a template for a new 
version. The mandatory attributes are _AWSversion_, _defpath_, 
_file_indexing_, _datbase\_variables_ (which specifies the RO 
metadata fields to keep track of), _format_level1b_, _format_level2a_, 
_format_level2b_. 

## Missions

Each module in Missions defines an RO mission. It defines the tracking 
algorithms for RO measurement and the file naming conventions each 
RO processing center uses for the mission, including the AWS standard 
naming convention. A template.py is provided for augmenting with a 
new mission. 

## Database

This provides wrapper code for the Reformatters, communicating RO 
metadata in and out of the reformatters, and writing and reading that 
information do a DynamoDB database as needed. It is highly preferred 
that this sub-package not be touched. 

## Utilities

Useful utililites for RO interpretation. It also includes high-level 
routines used as command line executables. The most important of these 
are _createjobs_, _batchprocess_, and _liveupdate\_wrapper_. The first 
creates a list of reformatting jobs for incoming RO data that can be 
submitted as a single AWS Batch job. The second actually executes a 
single AWS Batch job for reformatting and cataloging data. The last 
executes live-update processing for all new incoming data. 


### createjobs

The program _createjobs_ generates a list of JSON files, each of which 
defines a list of incoming RO retrieval data files that can be submitted 
to _batchprocess_ for reformatting and cataloging. The JSON files are then 
written to an S3 bucket for future access by _batchprocess_. 

The usage is 
```
createjobs [-h] [--version VERSION] [--liveupdate] [--daterange DATERANGE] 
    [--jobsperfile JOBSPERFILE] [--nonnominal] [--verbose] 
    processing_center mission file_type
```

The processing\_center designates from which RO processing center incoming 
RO data are to be retrieved, and it must be a valid processing center as 
defined for the AWS RO repository. The mission states which RO mission's data 
are to be ingested, and it, too, must be a valid AWS RO repository mission. 
The file\_type is the RO retrieval level; i.e., "level1b", "level2a", 
"level2b". The daterange prescribes the range of days over which to ingest 
RO data, --nonnominal states that even non-nominal ROM SAF RO retrievals 
should be included, and --jobsperfile prescribes how many RO data files 
should find their way into each batchprocess job definition that is output. 

The in-line help message is complete ("createjobs -h"). 

### batchprocess

The program _batchprocess_ reformats and catalogs a collection of incoming 
RO retrieval files as specified by a JSON files created by _createjobs_. 

The usage is 
```
batchprocess [-h] [--version AWSVERSION] [--workingdir WORKINGDIR] 
    [--clobber] [--verbose] jsonfile
```

The AWSVERSION specifies the AWS format version for the reformatted files; 
WORKINGDIR specifies a working area for the processing; --clobber declares 
that previously created output file should always be clobbered. The default 
is that previously created files and metadata entries should not be 
be clobbered. 

The in-line help message is complete ("batchprocess -h"). 

