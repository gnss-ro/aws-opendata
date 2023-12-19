# Purpose
UCAR has a few missions that are still ongoing and therefore have update/current datasets.  To keep our database and dataset uptodate, we must routinely check their FTP site and see if there are any new dates for existing missions, new missions, or new processing streams.

# Process
1. Lambda is triggered on a cron by the 15th of the month.  
2. Lambda will scrape UCAR site and local processed S3 buckets to find new dates to process
3. Lambda will send each tarball as a parameter to the ro-framework container with the --liveupdate parameter
4. the Batch job will then download, unpack then copy all files back to s3 while making a batchprocessing.json file 
5. the same batch job will then submit a batchprocessing job providing this newly create json file.
6. Lambda is triggered on a cron by the 1th of the month.
7. Lambda submits a batch job to export the DynamoDB table containing the metadata for the RO dataset.
8. This batch job exports dynamo, then processes the output into json files read by the gnssroutils API
9. the batch job then submits other batch jobs to sync all missions and dynamo to the Open Data Bucket.

Note:  if this code is run routinely, then it will not time out, but if this is the first time in awhile, you may need to change how far back the code checks or run locally instead of in Lambda.

## Job Tracking

TBD

## Lambda Detailed Scrape info
1. get the most recent master_policy.json file from ucar liveupdate s3
2. check for new missions on UCAR FTP
3. check for new processing streams on UCAR FTP
4. UCAR Scrape
5. S3 Scrape
6. Compare the sets and submit to batch any remaining tarballs to process
7. Save file listing processed downloads back to s3  

Note: the daily tarfile on UCAR site matches the daily folder name on S3.  This is how we see what's been done.

### UCAR Scrape
1. Loop through missions that don't have "keep_none" as their policy
2. for each mission, processing version where the end_date is blank, search it
3. Search for today through the past 60 days for available days of data for each mission, kept processing stream, and level  

Note: Step 3 above will need to be adjusted if running this for the first time.

### S3 Scrape
1. loop through all UCAR buckets.  In this case there is an original one and a liveupdate one
2. Using s3fs list the contents of the same prefix we are searching for on the UCAR site.  

### Needed Files
* s3://ucar-earth-ro-archive-liveupdate/master_policy.json
    contains the policies (see below) for which processing for each mission to search for.

#### master_policy.json
* "keep_all": Keep everything regardless of date  
    start_date: NOT REQUIRED  
    end_date: NOT REQUIRED  
    * Start search from <processing type>/
* "keep_after": Keep everything after a start_date  
    start_date: REQUIRED  
    end_date: NOT REQUIRED  
    * Start search from <processing type>/<level1b,level2>/<YYYY>/<DDD>/ and search for DOYs and years  
    greater than the start_date
* "keep_before": Keep everything before an end_date  
    start_date: NOT REQUIRED  
    end_date: REQUIRED  
    * Start search from <processing type>/<level1b,level2>/<YYYY>/<DDD>/ and search for DOYs and years less
    than the end_date  
* "keep_after_and_before": Keep everything between start_date and end_date  
    start_date: REQUIRED  
    end_date: REQUIRED  
    * Start search from <processing type>/<level1b,level2>/<YYYY>/<DDD>/ and search dates between the start_date
    and end_date
* "keep_none": Get nothing at all for this processing version  
    start_date: NOT REQUIRED  
    end_date: NOT REQUIRED  
    * No search done for this policy  

# Initial Scrape Run
Either adjust the ucar scrape lambda code or look in tools/ for additional scripts.
