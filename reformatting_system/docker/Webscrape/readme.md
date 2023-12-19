# original webscrape process flow
  1. ucar/romsaf web is scraped resulting in a list of tarballs to be processed.
  2. for each tarball the webscrape container batch job is called.
  3. once download, untarred and then uploaded the parts to s3 a .json is created in respective liveupdate bucket prefix = batchprocess-jobs
  4. webscrape container "gets active version"
  5. webscrape container submits batch job to def: f"ro-processing{aws_version.replace('.', '_')}"
  6. few days later the export/sync job is created. needs list of missions and active version

# TO DO
1. find way to skip download tar if already on s3 but still create .json for batchprocess.
ie. provided metopb/postProc/level1b/2021/001/atmPhs_postProc_2021_001.tar.gz so if tar is there, then look in s3 /untarred/metopb/postProc/level1b/2021/001/atmPhs_postProc_2021_001/*

2. update lambda to call new job definition

3. test from each mission

4. update submit_batch_ro-processing.py for new createjobs code

# Batch Commands
-e TEST=1
["liveupdate_wrapper", "webscrape", "1.1", "--tarfile", "metopb/postProc/level1b/2021/001/atmPhs_postProc_2021_001.tar.gz"]
["batchprocess","s3://gnss-ro-data-test/batchprocess-jobs/metopb_atmPhs_MTPB.2021.001.00.04.G26_2016.json","--version","1.1"]
["batchprocess","s3://gnss-ro-processing-definitions/batchprocess-jobs/romsaf-cosmic1-atmosphericRetrieval.000001.json","--version","1.1"]

# Workspace Commands

## New:
docker run -i --rm -e TEST=1 -v /home/i22916/.aws/credentials:/root/.aws/credentials ro-processing-framework liveupdate_wrapper webscrape 1.1 --tarfile file.tar.gz --auth

docker run -i --rm -v /home/i22916/.aws/credentials:/root/.aws/credentials ro-processing-framework liveupdate_wrapper webscrape 1.1 --tarfile file.tar.gz --auth

# framework

-e TEST=1 ro-processing-framework liveupdate_wrapper webscrape 1.1 --tarfile file.tar.gz
-e TEST=1 ro-processing-framework liveupdate_wrapper export 1.1
-e TEST=1 ro-processing-framework liveupdate_wrapper export 1.1 --manifest_file_s3_path s3://....json --mission tsx
ro-processing-framework liveupdate_wrapper sync 1.1 --prefix contributed/v1.1/romaf/metop/atmosphericRetrieval/2020

# createjobs

createjobs ucar champ [level1b, level2a, level2b] --liveupdate

# batchprocess

batchprocess s3://ucar-earth-ro-archive-liveupdate/batchprocess-jobs/cosmic2_atmPrf_C2E1.2021.295.00.06.R07_0001.json  --version v1.1

--env TEST=1 batchprocess s3://ucar-earth-ro-archive-liveupdate/batchprocess-jobs/cosmic2_atmPrf_C2E1.2021.295.00.06.R07_0001.json  --version v1.1

# clean
liveupdate_wrapper clean 1.1 --mission cosmic2
