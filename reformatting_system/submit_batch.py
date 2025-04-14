import os
import re
import boto3
import sys
import datetime
import subprocess

from rorefcat.Webscrape import job_tracking as track 

#  Create session.

try: 
    session = boto3.session.Session( profile_name="aernasaprod", region_name="us-east-1" )
except: 
    session = boto3.session.Session( region_name="us-east-1" )

batch = session.client( service_name="batch")
s3 = session.resource( "s3" )
todayMMDD = datetime.datetime.today().strftime('%m%d')
todayDate = datetime.datetime.today().strftime("%Y%m%d")

def rerun_log_file(lst,AWSversion):
    with open(lst,'r') as file:
        lines = file.readlines()

        for log in lines:

            basename = os.path.basename(log.strip())
            json_filename = basename.split('.')[0]+'.json'

            if "ucar" in basename:
                command = ['batchprocess', f"s3://gnss-ro-processing-definitions/batchprocess-jobs/{json_filename}","--version",AWSversion, "--clobber"]
            else:
                #for liveupdate ucar
                command = ['batchprocess', f"s3://ucar-earth-ro-archive-liveupdate/batchprocess-jobs/{json_filename}","--version",AWSversion, "--clobber"]

            jobName = f"rerun_{json_filename.split('.')[0]}_{todayMMDD}"

            job_tracking = {}
            job_tracking = {
                'job-date': f"batchprocess-{todayDate}",
                'jobname': jobName,
                'test': "false",
                'ram': 1900,
                'version': AWSversion,
                'center': "ucar",
                "mission": os.path.basename(json_filename).split('-')[0],
                'process_date': os.path.basename(json_filename).split('.')[1],
                'command': command
            }

            dependsID = track.main(job_tracking, session)

def submit_export( AWSversion, mission="all" ):
    job_tracking = {}
    if mission == "all": 
        command = [ "liveupdate_wrapper", "export", AWSversion ]
    else: 
        command = [ "liveupdate_wrapper", "export", AWSversion, "--mission", mission ]

    job_tracking = {
        'job-date': f"export-{todayDate}",
        'jobname': f"dynamo_export_test_{todayMMDD}_{AWSversion.replace('.','_')}",
        'test': "false",
        'ram': 7500,
        'version': AWSversion,
        'center': "",
        "mission": mission,
        'process_date': "",
        'command': command 
    }
    dependsID = track.main(job_tracking)

def submit_createjobs(jobName, command,center,mission,lvl):
    job_tracking = {}
    job_tracking = {
        'job-date': f"createjobs-{todayDate}",
        'jobname': f'{jobName}',
        'test': "false",
        'ram': 7500,
        'version': "1.1",
        "level": lvl,
        'center': center,
        "mission": mission,
        'process_date': "",
        'command': command
    }
    print("submitting",jobName)
    dependsID = track.main(job_tracking)

def submit_batchprocess(processing_center, liveupdate, calibratedphase, AWSversion, mission, test):
    """Submit batchprocess jobs. This will preprocess all UCAR and ROMSAF supplied
    RO data."""

    if test == "false":
        bucket_name = "gnss-ro-processing-definitions"
    else:
        bucket_name =  "gnss-ro-data-test" 
            
    bucket = s3.Bucket( bucket_name )       
    #  Intitialize.

    njobs = 0

    #  Iterate over job definitions.
    for obj in bucket.objects.filter( Prefix=f"batchprocess-jobs/{processing_center}" ):

        if obj.key[-5:] != ".json": continue
        definition = obj.key

        if liveupdate and "liveupdate" not in definition: continue
        if not liveupdate and "liveupdate" in definition: continue

        if calibratedphase and "level1b" not in definition: continue
        if not calibratedphase and "level1b" in definition: continue

        #if "refractivityRetrieval" not in definition: continue

        #valid_file_types = [ "calibratedPhase", "refractivityRetrieval", "atmosphericRetrieval" ]
        #valid_file_types = [ "level1b", "level2a", "level2b" ]

        #  Submit job definitions.

        command = ['batchprocess', f"s3://{bucket_name}/{definition}","--version",AWSversion, "--clobber"]

        if mission == "all" or mission in definition :
            njobs += 1
            jobName = f"{AWSversion}_batchprocess-{njobs:07d}.{definition.split('/')[1][:-5]}"
            jobName = jobName.replace('.','_')

            job_tracking = {}
            job_tracking = {
                'job-date': f"batchprocess-{todayDate}",
                'jobname': jobName,
                'test': test,
                'ram': 1900,
                'version': AWSversion,
                'center': processing_center,
                "mission": os.path.basename(definition).split('-')[0],
                'process_date': os.path.basename(definition).split('.')[1],
                'command': command
            }

            dependsID = track.main(job_tracking)

    return

def createjobs(processing_center, mission=None, daterange=None):
    AWSversion = "1.1"
    mission_list = []

    if mission == None: #get all
        if processing_center == "ucar":
            valid_missions = subprocess.run("ls rorefcat/src/rorefcat/Missions/", shell=True, capture_output=True)
            valid_missions = valid_missions.stdout.split()
            for each in valid_missions:
                if "pycach" in each: continue

                m = each.decode('UTF-8')
                if "init" in m: continue
                if "template" in m: continue
                m=m[:-3]
                mission_list.append(m)
        elif processing_center == "romsaf":
            mission_list = ["cosmic1","metop","grace","champ"]
        elif processing_center == "eumetsat":
            mission_list = ["cosmic1","metop","grace","champ"]
        elif processing_center == "jpl":
            mission_list = ["champ", "cosmic1", "grace","paz", "tsx"]            
    else:
        mission_list.append(mission)

    if processing_center == "ucar":
        for lvl in ["level1b", "level2a", "level2b"]:
            for m in mission_list:
                jobName = f"createjobs-{processing_center}-{m}-{lvl}-liveupdate"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion, "--liveupdate"]
                if daterange != None:
                    command.extend(["--daterange", daterange])
                submit_createjobs(jobName, command,processing_center,m,lvl)

                jobName = f"createjobs-{processing_center}-{m}-{lvl}"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion]
                if daterange != None:
                    command.extend(["--daterange", daterange])
                submit_createjobs(jobName, command,processing_center,m,lvl)

    elif processing_center == "romsaf":
        for lvl in ["level2a", "level2b"]:
            for m in mission_list:
                jobName = f"createjobs-{processing_center}-{m}-{lvl}-liveupdate"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion, "--liveupdate"]
                if daterange != None:
                    command.extend(["--daterange", daterange])
                submit_createjobs(jobName, command,processing_center,m,lvl)

                jobName = f"createjobs-{processing_center}-{m}-{lvl}"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion]
                if daterange != None:
                    command.extend(["--daterange", daterange])
                submit_createjobs(jobName, command,processing_center,m,lvl)

    elif processing_center == "eumetsat":
        for lvl in ["level1b"]:
            for m in mission_list:
                jobName = f"createjobs-{processing_center}-{m}-{lvl}-liveupdate"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion, "--liveupdate"]
                if daterange != None:
                    command.extend(["--daterange", daterange])
                submit_createjobs(jobName, command,processing_center,m,lvl)
    elif processing_center == "jpl":
        for lvl in ["level1b", "level2a", "level2b"]:                
            for m in mission_list:
                jobName = f"createjobs-{processing_center}-{m}-{lvl}"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion]
                if daterange != None:
                    command.extend(["--daterange", daterange])
                submit_createjobs(jobName, command,processing_center,m,lvl)
                                  
if __name__ == "__main__":

    STEP = 4 #for which chunk below to RUN
    AWSversion = "1.1"
    test = "false"
    mission = 'cosmic1' # spire, cosmic2, all
    center = "ucar"   #eumetsat, ucar, romsaf, jpl, all
    daterange = "2006-04-30:2019-12-10"

    #COSMIC-1 data for one month, February, 2008. This is a test of organization, as four processing centers contributed different levels of data for COSMIC-1. 
    #All RO data collected for one month, May, 2022. This is a test of volume, including Metop, COSMIC-2, Spire, and others. 

    ##rerun ucar and eumetsat for cosmic 2008 data

    '''
    1: Createjobs
    2: Run calibratedPhase
    3: Run other retrevial files
    4: Export dynamo
    5: rerun log files from list
    6: run check dynamo links, set dates below
    '''


    '''Processing Order full rerun
    ucar calibratedphase=True liveupdate=False
    ucar calibratedphase=True liveupdate=True
    eumetsat calibratedphase=True liveupdate=True
    wait for finish
    ucar calibratedphase=False liveupdate=False
    ucar calibratedphase=False liveupdate=True
    romsaf calibratedphase=False liveupdate=True
    romsaf calibratedphase=False liveupdate=False
    wait for finish
    submit_export '''

    if STEP == 1:
        #run createjobs
        if mission == "all": mission = None
        if center == "all":
            for c in ["eumetsat", "ucar", "romsaf", "jpl"]:
                #createjobs(center)
                #createjobs(center, mission = m)
                createjobs(c, mission=mission, daterange=daterange)
        else:
            #createjobs(center)
            #createjobs(center, mission=mission)            
            createjobs(center, mission=mission, daterange=daterange)

    elif STEP == 2:
        #run calibratedPhase
        if center == "all":
            for c in ["eumetsat", "ucar", "romsaf", "jpl"]:
                calibratedphase = True #for batchprocess to run only calibratedphase or the others
                liveupdate = True #for batchprocess liveupdate/webscrape files
                submit_batchprocess(c, liveupdate, calibratedphase, AWSversion, mission, test)
                liveupdate = False
                submit_batchprocess(c, liveupdate, calibratedphase, AWSversion, mission, test)
        else:
            calibratedphase = True #for batchprocess to run only calibratedphase or the others
            liveupdate = True #for batchprocess liveupdate/webscrape files
            submit_batchprocess(center, liveupdate, calibratedphase, AWSversion, mission, test)
            liveupdate = False
            submit_batchprocess(center, liveupdate, calibratedphase, AWSversion, mission, test)
    elif STEP == 3:
        #run everything else
        if center == "all":
            for c in ["eumetsat", "ucar", "romsaf", "jpl"]:        
                calibratedphase = False
                liveupdate = True
                submit_batchprocess(c, liveupdate, calibratedphase, AWSversion, mission, test)
                liveupdate = False
                submit_batchprocess(c, liveupdate, calibratedphase, AWSversion, mission, test)
        else:
            calibratedphase = False
            liveupdate = True
            submit_batchprocess(center, liveupdate, calibratedphase, AWSversion, mission, test)
            liveupdate = False
            submit_batchprocess(center, liveupdate, calibratedphase, AWSversion, mission, test)
    elif STEP == 4:
        #export dynamo and aws sync all
        #the initial export job submits separate batch jobs
        #for each missions to speed up the json subsetting of dynamo table data
        #also submits sync for each contributed mission_year
        submit_export(AWSversion, mission=mission)

    elif STEP == 5:
        #rerun all json files based on the list of log files
        rerun_log_file("Utilities/log.lst", AWSversion)

    elif STEP == 6:
        sdate = datetime.datetime(2012,1,1)
        edate = datetime.datetime(2022,12,31)
        d = sdate

        job_tracking = {}
        job_tracking = {
            'job-date': f"chec_dynamo-{todayDate}",
            'jobname': '',
            'test': test,
            'ram': 1900,
            'version': AWSversion,
            'center': '',
            "mission": mission,
            'process_date': '',
            'command': ''
        }

        while d <= edate:
            job_tracking['jobname'] = f"check-dynamo-{m}-{d.strftime('%Y%m%d')}"
            job_tracking['command'] = ["liveupdate_wrapper", "check_Dlinks", AWSversion, "--mission", m, "--datestr", d.strftime("%Y-%m-%d")]
            print(f"submitting: check-dynamo-{m}-{d.strftime('%Y%m%d')}")

            dependsID = track.main(job_tracking)
            d += datetime.timedelta(days=1)

    elif STEP == 7: 
        m = re.search( r'^(\d{4}-\d{2}-\d{2}):(\d{4}-\d{2}-\d{2})$', daterange )
        sdate = datetime.datetime.strptime( m.group(1), "%Y-%m-%d" )
        edate = datetime.datetime.strptime( m.group(2), "%Y-%m-%d" )

        ndays_per_job = 365 
        d0 = sdate + datetime.timedelta(days=0)

        while d0 <= edate: 
            d1 = d0 + datetime.timedelta(days=ndays_per_job-1)
            if d1 > edate: 
                d1 = edate + datetime.timedelta(days=0)
            job_tracking = {
                    'job-date': f"check_dynamo_links-{todayDate}", 
                    'jobname': "clinks_{:}_{:}-{:}".format( mission, d0.strftime("%Y%m%d"), d1.strftime("%Y%m%d") ), 
                    'test': test, 
                    'ram': 1900, 
                    'version': AWSversion, 
                    'center': "", 
                    'mission': mission, 
                    'process_date': '', 
                    'command': [ "check_dynamo_links", mission, "{:}:{:}".format( d0.strftime("%Y-%m-%d"), d1.strftime("%Y-%m-%d") ), "--version", AWSversion ]
                }

            print( "Submitting " + job_tracking['jobname'] + ": " + " ".join( job_tracking['command'] ) ) 
            dependsID = track.main(job_tracking)

            d0 = d1 + datetime.timedelta(days=1)

