import sys, os
sys.path.append('./lib/')
import json
from datetime import datetime

import requests
import boto3

from bs4 import BeautifulSoup

#create s3 boto3 object and session
#session = boto3.session.Session(profile_name = "aernasaprod", region_name = 'us-east-1' )
session = boto3.Session( region_name= "us-east-1")

s3_resource = session.resource('s3')
s3_client = session.client('s3')
batch = session.client( service_name="batch")
sns_client = session.client('sns')
dynamodb = boto3.resource('dynamodb')

romsaf_site = "https://rom-saf.eumetsat.int/pub/icdr/v1-series/profs/"
#romsaf_site = "https://www.romsaf.org/pub/icdr/v1-series/profs/"
#https://www.romsaf.org/pub/icdr/v1-series/profs/metop/atm/2017/atm_20170101_metop_I_2320_0010.tgz
liveupdate_bucket_name = 'romsaf-earth-ro-archive-liveupdate'
staging_bucket_name = "gnss-ro-data-staging"
keep_prefixes = ['atm', 'wet']
mission_list = ["metop"]
todayDate = datetime.today().strftime("%Y%m")

def send_sns(message):
    topic_arn = f'arn:aws:sns:us-east-1:996144042418:webscrape'

    response = sns_client.publish(
        TopicArn= topic_arn,
        Message= message,
        Subject= f'ROMSAF webscrape',
        MessageStructure= 'String',
        MessageAttributes= {
            'njobs': {
                'DataType': 'String',
                'StringValue': "filler",
            }
        }
    )

def submit_batch(tarfile):
    #tarfile = metop/atm/2017/atm_20170101_metop_I_2320_0010.tgz
    job_tracking = {} #initialize

    jobName = f"romsaf-{tarfile.split('metop/')[1].replace('/','_')[:-4]}"
    command = ["liveupdate_wrapper", "webscrape", "1.1", "--tarfile", tarfile, "--romsaf"]
    response_webscrape = batch.submit_job(
        jobName = jobName,
        jobQueue = "ro-processing-EC2",
        jobDefinition = "ro-processing-framework",
        containerOverrides =
        {
            'command': command ,
            'vcpus': 1,
            'memory': 3500
        },
        timeout={
            'attemptDurationSeconds': 3600
        }
    )

    job_tracking = {
        'job-date': f"webscrape-{todayDate}",
        'jobID': f"romsaf-{response_webscrape['jobId']}",
        'jobname': jobName,
        'queue': "ro-processing-EC2",
        'status': "RUNNING",
        'exit_code': "",
        'rerun_status': "",
        'version': "1.1",
        'center': "romsaf",
        "mission": "metop",
        "ram": 3500,
        "test": "false",
        "jobdef": "ro-processing-framework",
        'level': "level2",
        'process_date': jobName.split('_')[3],
        'command': command
    }

    create_tracking_item(job_tracking)

def create_tracking_item(job_tracking):
    # create a reference to the existing "aqcast-requests" table
    tracking_table = dynamodb.Table("job-tracking")
    #Put Item:
    tracking_table.put_item(
        Item = job_tracking
    )

def scrape(url):
    #returns desired contents of any url
    #plan B downloads index.html wget https://username:password@rom-saf.eumetsat.int/pub/nrt/atm/2023/2023-06-29/ --no-check-certificate
    try:
        with requests.session() as soup_session:
            response = soup_session.get(url, auth=('Amy McVey', '0Mjr2u'), verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')

        contents = []

        #find indexes of sitemap and javascript:history...
        for i,a_tag in enumerate(soup.findAll('a')):
            #can't be number as diff missions have diff start year
            if a_tag.attrs.get('href') == "/login.php":
                sitemap_index = i + 1
            if "javascript:history.go(-1)" in a_tag.attrs.get('href'):
                end_index = i

        #get hrefs for links between sitemap and javascript:history
        for i in range(sitemap_index, end_index):
            tarfile = soup.findAll('a')[i].attrs.get('href').replace("/","")
            if len(tarfile) > 10:
                file_type = tarfile.split('_')[0]
                year = tarfile.split('_')[1][0:4]
                if "metop" in tarfile: mission = "metop"
                tarpath = os.path.join(mission, file_type, year, tarfile)
                #metop/atm/2017/atm_20170101_metop_I_2320_0010.tgz

            else:
                tarpath = tarfile
            contents.append(tarpath)
        return contents
    except Exception as e:
        print(e)
        send_sns(f"Romsaf failed scrape, {e}")
        sys.exit(1)

def scrape_s3_liveupdate():
    #scrape nasa s3 liveupdate bucket for tarballs
    obj_list = []
    livestream_scrape = s3_resource.Bucket(liveupdate_bucket_name)

    #check to make sure objects are in the right folder with the right suffix.
    for bucket_object in livestream_scrape.objects.filter( Prefix=f"tarballs/" ):
        if ".tgz" in bucket_object.key:
            obj_list.append(bucket_object.key.split('tarballs/')[1])

    return obj_list

#upload updated file to s3
def upload_s3_file(bucket, file):

    file_local = f'/tmp/{file}'
    if "download_list" in file:
        file =f'download_list/{file}'

    s3_client.upload_file( file_local, bucket, file )
    return file_local

################
#### MAIN ######

def lambda_handler(event, context):

    print(f"event: {event}\ncontext: {context}")

    #initialize arrays
    scrape_list = [] #list of tar files on romsaf site or metop atm and wet
    download_list = [] #initial array of tarballs not in our s3 bucket
    objKey_list = [] #list of all tarballs in s3://romsaf-earth-ro-archive-liveupdate/tarballs

    #get list of tar files in romsaf site
    for mission in mission_list:
        for file_prefix in keep_prefixes:
            #get list of years of data to loop through
            print(os.path.join(romsaf_site, mission, file_prefix)+'/')
            years = scrape(os.path.join(romsaf_site, mission, file_prefix)+'/')
            print(years)
            for y in years:
                scrape_list.extend(scrape(os.path.join(romsaf_site, mission, file_prefix, y)))

    #scrape liveupdate bucket only
    objKey_list = scrape_s3_liveupdate()

    #find diff in s3 obj list and romsaf site obj list
    a= set(objKey_list)
    b= set(scrape_list)
    download_list = b - a

    #for debugging
    if event['detail-type'] != "test":
        #with final download list complete, submit batch jobs
        njobs = 0
        if len(download_list) > 0:
            for tarfile in download_list:
                njobs += 1
                #call batch job webscrape to download and untar etc.
                submit_batch(tarfile)
        else:
            print("all set, no files needed")

        #send sns for njobs submitted to batch
        send_sns(f"Jobs submitted: {njobs}")

        #keep refined list on s3
        today_obj = datetime.today()
        download_file = f'download_list_{today_obj.strftime("%Y%m%d")}.txt'
        with open(f"/tmp/{download_file}", 'w') as file:
            for f in download_list:
                file.write(f + '\n')

        upload_s3_file(liveupdate_bucket_name, f"{download_file}")

    else:
        print("test submit:", len(download_list), "jobs")


if __name__ == "__main__":

    lambda_handler("test", "local")
