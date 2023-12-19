import sys, os
sys.path.append('./lib/')
import json
from datetime import datetime, timedelta

#triggered from ucar webscrape lambda

import requests
import boto3
import s3fs
from bs4 import BeautifulSoup

#create s3 boto3 object and session
session = boto3.Session( region_name= "us-east-1")

s3_client = session.client('s3')
s3_resource = session.resource('s3')
batch = session.client( service_name="batch")
sns_client = session.client('sns')
dynamodb = session.resource('dynamodb')

#create s3fs client
s3fs_nasa = s3fs.S3FileSystem( client_kwargs={ 'region_name': "us-east-1" })


keep_levels = ['level1b', 'level2']
keep_tar_prefixes = ['conPhs', 'atmPhs', 'atmPrf', 'wetPrf', 'wetPf2']

ucar_site = "https://data.cosmic.ucar.edu/gnss-ro/"
liveupdate_bucket_name = 'ucar-earth-ro-archive-liveupdate'
staging_bucket_name = "gnss-ro-data-staging"
todayDate = datetime.today().strftime("%Y%m")

def send_sns(message):
    topic_arn = f'arn:aws:sns:us-east-1:996144042418:webscrape'

    response = sns_client.publish(
        TopicArn= topic_arn,
        Message= message,
        Subject= f'Ucar webscrape',
        MessageStructure= 'String',
        MessageAttributes= {
            'njobs': {
                'DataType': 'String',
                'StringValue': "filler",
            }
        }
    )

def submit_batch(tarfile,test):
    #cosmic2/nrt/level2/2022/058/wetPf2_nrt_2022_058.tar.gz
    job_tracking = {} #initialize

    mission = tarfile.split('/')[0]
    file_base = os.path.basename(tarfile)
    command = ["liveupdate_wrapper", "webscrape", "1.1", "--tarfile", tarfile]

    if "cosmic2" in mission and "level1" in tarfile:
        ram = 15000
    else:
        ram = 6500

    if "atmPhs" in tarfile or "conPhs" in tarfile:
        lvl = "level1b"
    else:
        lvl = "level2"

    if test:
        jobName = f"{mission}_{file_base[:-7].replace('.','_')}_test"
        print("test",command)
    else:
        jobName = f"{mission}_{file_base[:-7].replace('.','_')}"

        job_tracking = {
            'job-date': f"webscrape-{todayDate}",
            'jobname': jobName,
            'queue': "ro-processing-EC2",
            'status': "RUNNING",
            'exit_code': "",
            'rerun_status': "",
            'version': "1.1",
            'center': "ucar",
            'test': "false",
            "mission": mission,
            'level': lvl,
            "ram": ram,
            "jobdef": "ro-processing-framework",
            'process_date': jobName[-8:],
            'command': command
        }

        response_webscrape = batch.submit_job(
            jobName = jobName,
            jobQueue = job_tracking['queue'],
            jobDefinition = "ro-processing-framework",
            containerOverrides =
            {
                'command': command ,
                'vcpus': 1,
                'memory': ram
            },
            timeout={
                'attemptDurationSeconds': 7200
            }
        )

        job_tracking["jobID"] = f"{job_tracking['center']}-{response_webscrape['jobId']}"
        create_tracking_item(job_tracking)


#download s3 file local
def download_s3_file(bucket, file):

    file_local = f'/tmp/{file}'

    s3_client.download_file(bucket, file, file_local)
    return file_local

#upload updated file to s3
def upload_s3_file(bucket, file):

    file_local = f'/tmp/{file}'
    if "download" in file:
        file =f'refined_list/{file}'

    s3_client.upload_file( file_local, bucket, file )
    return file_local

def scrape(url):
    #returns contents of any url
    contents = []
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    for a_tag in soup.findAll('a'):
        #avoids the link to the parent folder
        if a_tag.attrs.get('href') != "../":
            contents.append(a_tag.attrs.get('href').split('/')[0])

    return contents


def get_policies():
    #load local policy.json file into dict
    local_file = download_s3_file(liveupdate_bucket_name, "master_policy.json")

    with open(local_file, 'r') as the_json_file:
        policy_dict = json.load(the_json_file)

    #make list of missions to keep and to know when we have new ones.
    keep_missions = policy_dict.keys()

    #get list of processing folders to keep
    keep_proc_folders = []
    for m in keep_missions:
        for key in policy_dict[m].keys():
            if key not in keep_proc_folders:
                keep_proc_folders.append(key)
    return policy_dict

def new_mission_check(policy_dict):
    
    mission_list = []
    #scrape ucar site for missions only
    site_content = scrape(ucar_site)
    for m in site_content:
        if m not in policy_dict.keys():
            mission_list.append(m)

    return mission_list
    
def new_proc_check(policy_dict):
    
    proc_list = []
    #scrape ucar site for new processing types
    for m in policy_dict.keys():
        proc_content = scrape(os.path.join(ucar_site,m))
        
        for proc in proc_content:
            if proc == "noaa" or proc == "provisional":
                proc_content2 = scrape(os.path.join(ucar_site,m,proc))
                proc = f"{proc}/{proc_content2[0]}"
            if proc not in policy_dict[m].keys():
                proc_list.append(f"{m}/{proc}")

    return proc_list
    
def create_tracking_item(job_tracking):
    # create a reference to the existing "aqcast-requests" table
    tracking_table = dynamodb.Table("job-tracking")
    #Put Item:
    tracking_table.put_item(
        Item = job_tracking
    )

def get_ucar_prefix_list(prefix):
    #check tar folder name on ucar site
    ucar = scrape(f'{ucar_site}/{prefix}')
    keep_files = []
    for i in ucar:
        k = [i for j in keep_tar_prefixes if j in i]
        if len(k) > 0:
            keep_files.append(f'{prefix}{k[0].split(".")[0]}')

    #print(keep_files)
    return keep_files

def check_aer_s3(prefix):
    #check tar folder name on s3 untarred buckets as they have the same folder structure as tar files
    local_clean = []
    untarred_buckets = ["ucar-earth-ro-archive-untarred","ucar-earth-ro-archive-liveupdate/untarred"]
    for bucket in untarred_buckets:
        local_list = s3fs_nasa.ls( os.path.join( bucket, prefix ) +'/')
        local_clean.extend([ i.split(bucket)[1][1:] for i in local_list])

    return local_clean
    
def deeper_archive_scrape(level_prefix):
    now_Date = datetime.today()
    #now_Date = datetime(2023,1,12) #for testing
    start_Date =  now_Date - timedelta(days = 60)
    final_list = []
    get_list = []
    year_list = list(set([str(now_Date.year), str(start_Date.year)]))
    
    for y in year_list:     
        day_list = scrape(os.path.join(ucar_site,level_prefix,y))
        for d in day_list:
            #so we don't check the whole year but only 60 days back
            if d < start_Date.strftime("%j") and len(year_list) == 1: continue
            #so we don't check the whole previous year if 60 span covers two year values
            if d < "300" and len(year_list) == 2 and year_list.index(y) == 1: continue
            
            prefix = os.path.join(level_prefix,y,d)
            #print("checking",level_prefix,y,d, year_list.index(y)) 
            #first we check ftp site
            ucar_list = get_ucar_prefix_list(f"{prefix}/")
            a = set(ucar_list)

            #second we check to aer nasa buckets for the untarred file prefix
            aer_list = check_aer_s3(prefix)
            b = set(aer_list)

            #remove aer from the ucar list to see what we need to process
            c = a-b
            
            #save file in list
            
            if len(list(c)) != 0:
                get_list.extend(list(c))
            
    # make sure we get the tarball
    final_list = [f'{f}.tar.gz' for f in get_list]
    
    print(level_prefix, final_list)
    
    return final_list

                
################
#### MAIN ######

def lambda_handler(event, context):

    #initialize arrays and dicts
    policy_dict = {} #from master_policy.json
    scrape_dict = {} #loaded from ucar_scrape.json and updated by scraping from ucar site
    job_tracking = {} #for dynamo tracking table
    download_list = [] #initial array of tarballs not in our s3 bucket
    refined_download_list = [] #refined download list based on policies
    objKey_list = [] #list of all tarballs in s3://ucar-earth-ro-archive
    keep_missions = [] #list of missions in the master_policy
    new_missions = [] #list of new missions for sns
    new_proc = [] #list of new processing types for sns
    day_of_month = datetime.today().day

    if day_of_month == 16:
        job_tracking = {
            'job-date': f"sync-{todayDate}",
            'jobname': "dynamo_export_v1_1_auto",
            'queue': "ro-processing-EC2",
            'status': "RUNNING",
            'exit_code': "",
            'rerun_status': "",
            'version': "1.1",
            'center': "",
            'test': "false",
            "mission": "",
            "ram": 7500,
            'level': "",
            'process_date': "",
            "jobdef": "ro-processing-framework",
            'command': ["liveupdate_wrapper", "export", "1.1"]
        }

        #submit export
        response_webscrape = batch.submit_job(
            jobName = job_tracking['jobname'],
            jobQueue = job_tracking['queue'],
            jobDefinition = "ro-processing-framework",
            containerOverrides =
            {
                'command': job_tracking['command'] ,
                'vcpus': 1,
                'memory': 7500
            }
        )

        job_tracking['jobID']= f"ro-{response_webscrape['jobId']}"

        create_tracking_item(job_tracking)
        send_sns("Submitted Dynamo export and sync all")
    else:
        policy_dict = get_policies()
        
        new_missions = new_mission_check(policy_dict)
        new_proc = new_proc_check(policy_dict)
        print("new",new_missions,new_proc)
        
        download_list = []
        for mission in policy_dict.keys():
            for p in policy_dict[mission].keys():
                if policy_dict[mission][p]["policy"]=="keep_none": continue
                #print(policy_dict[mission][p]["end_date"])
                if policy_dict[mission][p]["end_date"]!="": continue
                #print("check",mission,p)
                for lev in keep_levels:
                    download_list.extend(deeper_archive_scrape(f"{mission}/{p}/{lev}/") )
            
        #final save data
        print("Saving final data.....\n")
        download_file = f'refined_list_{datetime.today().strftime("%Y%m%d%H")}.txt'
        with open(f"/tmp/{download_file}",'w') as file:
            file.write(json.dumps(download_list))
        
        upload_s3_file(liveupdate_bucket_name, download_file)
        test = False
        #submit jobs to batch   
        for file in download_list:
            #print("test",file)
            submit_batch(file,test)
            
        #send sns for new missions, proc and njobs submitted to batch
        message = f"New missions: {new_missions}.\nnew processing types: {new_proc}\n" + \
            f"files processed: {len(download_list)}"
        send_sns(message)
        
if __name__ == "__main__":
     
    boto3.setup_default_session(profile_name='aernasaprod')
    event = {}
    context = {}
    lambda_handler(event, context)            
