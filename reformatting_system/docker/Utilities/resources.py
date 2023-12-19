"""These are resource parameters for the Radio Occultation Reformatting-Cataloging 
System. These parameters are the key for portability.

Authors: Stephen Leroy (sleroy@aer.com) and Amy McVey (amcvey@aer.com)
Date: December 7, 2023
"""

#  The AWSprofile is the name associated with the AWS account in which the 
#  reformatting and cataloging is done. It can also be considered the 
#  "staging" account. The account exists in the AWS region (AWSregion). 
#
#  The stagingBucket is the name of the S3 bucket in which reformatted 
#  RO data are written and JSON dumps of the DynamoDB table are written. 
#  The data are then subsequently synchronized from this bucket into the 
#  AWS Registry of Open Data bucket (OpenDataBucket) which is exposed to 
#  the public. 

AWSprofile = "aernasaprod"
AWSregion = "us-east-1"
stagingBucket = "gnss-ro-data-staging"
OpenDataBucket = "gnss-ro-data"

#  The DynamoDB table is defined by the appropriate module in Versions, as 
#  each versions of the output files is associated with a unique DynamoDB 
#  table for cataloging occultations and storing occultation metadata. 

