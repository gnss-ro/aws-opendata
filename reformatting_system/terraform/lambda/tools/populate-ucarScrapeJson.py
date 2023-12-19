#purpose:
#When there are too many new paths to scrape on the ucar site lambda times out.
#this script is to run locally to update s3://ucar-earth-ro-archive-liveupdate/ucar_scrape.json
# once this is updated, lambda will run quicker and submit batch for any files not processed.
#code taken from ucarWebScrapeToS3 lambda

import sys, os
import json
import re
from datetime import datetime

import requests
import boto3
import s3fs
from bs4 import BeautifulSoup

#create s3 boto3 object and session
session = boto3.Session( profile_name = "aernasaprod", region_name= "us-east-1")

batch = session.client( service_name="batch")
s3_client = session.client('s3')
s3_resource = session.resource('s3')
nasa_bucket = s3_resource.Bucket("ucar-earth-ro-archive-untarred")

#create s3fs client
s3fs_nasa = s3fs.S3FileSystem( client_kwargs={ 'region_name': "us-east-1" }, profile = "aernasaprod")

#set params
keep_levels = ['level1b', 'level2']
keep_tar_prefixes = ['conPhs', 'atmPhs', 'atmPrf', 'wetPrf', 'wetPf2', 'opnGps']
missions_with_noaa_folder = ["spire", "geoopt","planetiq"]

ucar_site = "https://data.cosmic.ucar.edu/gnss-ro/"
liveupdate_bucket_name = 'ucar-earth-ro-archive-liveupdate'
staging_bucket_name = "gnss-ro-data-staging"

"""Scan the metadata database, searching for incorrect format date-time."""

import os
import json
import re

def scan_database( path ): 
    """Scan the metadata contained in the directory "path"."""

    jsonfiles = sorted( [ f for f in os.listdir(path) if f[-5:] == ".json" ] )

    #  Loop over files. 

    for jsonfile in jsonfiles: 
        d = None
        nvalidsetting = 0
        with open( os.path.join( path, jsonfile ), 'r' ) as f: 
            d = json.load( f )
        if d is None: 
            print( f"Bad format: {jsonfile}" )
            continue

        #  Loop over entries in the database. 

        for key, val in d.items(): 
            m = re.search( '^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}$', val['date-time'] )
            if not m: 
                print( f'Bad date-time: {jsonfile} record {key}' )
            if val['setting'] != "": 
                nvalidsetting += 1

        if nvalidsetting == 0: 
            print( f'No valid setting flags: {jsonfile}' )

def submit_batch_tarfile(file):
    if "cosmic2" in file and "level1" in file:
        ram = 15000
    else:
        ram = 6500
        
    response = batch.submit_job(
        jobName = file[:-7].replace('/','_'),
        jobQueue = 'ro-processing-EC2',
        jobDefinition = 'ro-processing-framework',
        containerOverrides =
        {
        'command': ["liveupdate_wrapper", "webscrape","1.1","--tarfile",file] ,
        'vcpus': 1,
        'memory': ram
        }
    )
    
def get_policies():
    #load local policy.json file into dict
    local_file = "master_policy.json"
    s3_client.download_file(liveupdate_bucket_name, local_file, local_file)

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

def main():
    
    #initialize arrays and dicts
    policy_dict = {} #from master_policy.json
    scrape_dict = {} #loaded from ucar_scrape.json and updated by scraping from ucar site
    keep_missions = [] #list of missions in the master_policy

    policy_dict = get_policies()
    keep_missions = policy_dict.keys()

    #load scrape data from s3
    local_scrape_json = "ucar_scrape.json"
    s3_client.download_file(liveupdate_bucket_name, local_scrape_json, local_scrape_json)

    print("loading data.....\n")
    with open(local_scrape_json, 'r') as file:
        lines = file.read()
        scrape_dict  = json.loads(lines)

    #scrape ucar site for missions only
    site_content = scrape(ucar_site)
    for m in site_content:
        if m in keep_missions:
            if m not in scrape_dict.keys():
                scrape_dict[m] = {}
                scrape_dict[m]["files"] = []
                scrape_dict[m]["prefix"] = []

    #loop through missions on ucar site to get processing type, lvl, yr, doy
    for m in scrape_dict.keys():
        print("start checking: ", m, len(scrape_dict[m]['files']))
        if m in missions_with_noaa_folder:
            proc_content = scrape(os.path.join(ucar_site,m,"noaa"))
        else:
            proc_content = scrape(os.path.join(ucar_site,m))

        for proc in proc_content:
            if m in missions_with_noaa_folder:
                proc = f"noaa/{proc}"
            if m == "cosmic2" and proc == "provisional":
                #there are 2 sub procs here both keep_none but just use one to continue
                proc = f"{proc}/release1"

            if proc not in policy_dict[m].keys():
                #only get the processing type if it's listed in the policy for the respective mission
                continue
            if policy_dict[m][proc]["policy"] == "keep_none":
                continue
            if proc not in scrape_dict[m].keys():
                scrape_dict[m][proc] = {}

            #get levels
            level_content = scrape(os.path.join(ucar_site,m,proc))
            for lvl in level_content:
                #make sure to get only interested levels, i.e. not level1a
                if lvl in keep_levels:
                    if lvl not in scrape_dict[m][proc].keys():
                        scrape_dict[m][proc][lvl] = {}
                else:
                    continue

                #get years
                year_content = scrape(os.path.join(ucar_site,m,proc,lvl))
                for yr in year_content:
                    #get day
                    #set year in dict to the array of days for looping
                    scrape_dict[m][proc][lvl][yr] = scrape(os.path.join(ucar_site,m,proc,lvl,yr))
                    for day in scrape_dict[m][proc][lvl][yr]:
                        #keep file prefix to see if we've checked that day
                        obj_day_prefix = f'{m}/{proc}/{lvl}/{yr}/{day}/'
                        if obj_day_prefix not in scrape_dict[m]["prefix"]:
                            #screenout so we know we are processing new files
                            #print("processing new file:", m, proc, lvl, yr, len(scrape_dict[m][proc][lvl][yr]), "files",len(scrape_dict[m]['files']))
                            #get tarball list for each day
                            tar_files = scrape(os.path.join(ucar_site,m,proc,lvl,yr,day))
                            for tar in tar_files:
                                #check to make sure it has a prefix we want to keep
                                keep_files = [i for i in keep_tar_prefixes if i in tar]
                                if len(keep_files) > 0 :
                                    scrape_dict[m]['files'].append(os.path.join(obj_day_prefix,tar))
                            scrape_dict[m]["prefix"].append(obj_day_prefix)
                        else:
                            continue


        print("done checking: ", m, len(scrape_dict[m]['files']))

    #final save data
    print("Saving final data.....\n")
    with open(local_scrape_json,'w') as file:
        file.write(json.dumps(scrape_dict))

    s3_client.upload_file( local_scrape_json, liveupdate_bucket_name, local_scrape_json ) 

def get_ucar_prefix_list(prefix):
    #check tar folder name on ucar site
    #
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
        #print(os.path.join( bucket, prefix )+'/',local_list)

    return local_clean
 
def clean_bfr(m= None):
    
    policy_dict = get_policies()
    for mission in policy_dict.keys():
        if m and mission!=m: continue  #so we can run missions ad hoc
        for p in policy_dict[mission].keys():
            if policy_dict[mission][p]["policy"]=="keep_none": continue
            print("check",mission,p)
            level_prefix = f"{mission}/{p}/level2/"
                
    
    #first we check ftp site
    year_list = scrape(os.path.join(ucar_site,level_prefix))
    for y in year_list: 
        print("checking",level_prefix,y)  
        day_list = scrape(os.path.join(ucar_site,level_prefix,y))
        for d in day_list:
            prefix = os.path.join(level_prefix,y,d)
            
            local_list = s3fs_nasa.ls( os.path.join( "ucar-earth-ro-archive-untarred", prefix ) +'/')
            aer_list = [ i.split("ucar-earth-ro-archive-untarred")[1][1:] for i in local_list]
            for f in aer_list:
                if "bfrPrf" in f:
                    print(f)
                    nasa_bucket.objects.filter(Prefix = f"{f}/").delete()              
                    
                        
def deeper_archive_scrape(level_prefix):
    
    #first we check ftp site
    year_list = scrape(os.path.join(ucar_site,level_prefix))
    for y in year_list: 
        print("checking",level_prefix,y)  
        day_list = scrape(os.path.join(ucar_site,level_prefix,y))
        for d in day_list:
            prefix = os.path.join(level_prefix,y,d)
            
            ucar_list = get_ucar_prefix_list(f"{prefix}/")
            a = set(ucar_list)

            aer_list = check_aer_s3(prefix)
            b = set(aer_list)              
                    
            #remove aer from the ucar list to see what we need to process
            c = a-b

            final_list = [f'{f}.tar.gz' for f in c]
        
            for file in final_list:
                print("submitting",file)
                submit_batch_tarfile(file)
                
def thourough_check(m=None):
    '''for each valid mission and processing type we are keeping, 
    check each year and day for the respective level files in the ucar ftp
    site and compare to the untarred folder on nasa s3 bucket'''
    policy_dict = get_policies()
    for mission in policy_dict.keys():
        if m and mission !=m: continue  #so we can run missions ad hoc
        for p in policy_dict[mission].keys():
            if policy_dict[mission][p]["policy"]=="keep_none": continue
            print("check",mission,p)
            for lev in keep_levels:
                deeper_archive_scrape(f"{mission}/{p}/{lev}/")    
          
if __name__ == "__main__":
    #main()
    # champ, cnofs, cosmic1, cosmic2, geoopt, gpsmet, gpsmetas, grace, kompsat5, 
    # metopa - c, paz, planetiq, sacc, spire, tdx, tsx
    m="tsx"
    thourough_check(m=m)
    
    clean_bfr(m=m)
    quit()
    
    #check stagin bucket json files for valid setting
    #first sync json files
    os.system("aws s3 sync s3://gnss-ro-data-staging/dynamo/v1.1/export_subsets/ ~/Downloads/rodatabase/v1.1/ --delete --profile aernasaprod")
    scan_database( "~/Downloads/rodatabase/v1.1/" )
    
    #submit_batch_tarfile("cosmic1/repro2013/level1b/2012/001/atmPhs_repro2013_2012_001.tar.gz")
    mission = "cosmic2"
    p = "nrt"
    lev = "level2"
    #for lev in keep_levels:
    deeper_archive_scrape(f"{mission}/{p}/{lev}/2022")  

