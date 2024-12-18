import os
import boto3
import sys
import datetime
import subprocess

from rorefcat.src.rorefcat.Webscrape import job_tracking as track 

#  Create session.
session = boto3.session.Session( profile_name="nasa", region_name="us-east-1" )
batch = session.client( service_name="batch")
s3 = session.resource( "s3" )
todayMMDD = datetime.datetime.today().strftime('%m%d')
todayDate = datetime.datetime.today().strftime("%Y%m")

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

def submit_export(AWSversion):
    job_tracking = {}
    job_tracking = {
        'job-date': f"export-{todayDate}",
        'jobname': f"dynamo_export_test_{todayMMDD}_{AWSversion.split('.','_')}",
        'test': "false",
        'ram': 7500,
        'version': AWSversion,
        'center': "",
        "mission": "",
        'process_date': "",
        'command': ["liveupdate_wrapper", "export", AWSversion]
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

def createjobs(processing_center, mission = None, daterange = None):
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
    STEP = 3 #for which chunk below to RUN
    AWSversion = "2.0"
    test = "false"
    m = 'cosmic1' # spire, cosmic2, all
    center = "ucar"   #eumetsat, ucar, romsaf, jpl, all
    daterange = "2008-02-01:2008-02-29"

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
        if m == "all": m = None
        if center == "all":
            for c in ["eumetsat", "ucar", "romsaf", "jpl"]:
                #createjobs(center)
                #createjobs(center, mission = m)
                createjobs(c, mission = m, daterange = daterange)
        else:
            #createjobs(center)
            #createjobs(center, mission = m)            
            createjobs(center, mission = m, daterange = daterange)

    if STEP ==2:
        #run calibratedPhase
        if center == "all":
            for c in ["eumetsat", "ucar", "romsaf", "jpl"]:
                calibratedphase = True #for batchprocess to run only calibratedphase or the others
                liveupdate = True #for batchprocess liveupdate/webscrape files
                submit_batchprocess(c, liveupdate, calibratedphase, AWSversion, m, test)
                liveupdate = False
                submit_batchprocess(c, liveupdate, calibratedphase, AWSversion, m, test)
        else:
            calibratedphase = True #for batchprocess to run only calibratedphase or the others
            liveupdate = True #for batchprocess liveupdate/webscrape files
            submit_batchprocess(center, liveupdate, calibratedphase, AWSversion, m, test)
            liveupdate = False
            submit_batchprocess(center, liveupdate, calibratedphase, AWSversion, m, test)
    if STEP ==3:
        #run everything else
        if center == "all":
            for c in ["eumetsat", "ucar", "romsaf", "jpl"]:        
                calibratedphase = False
                liveupdate = True
                submit_batchprocess(c, liveupdate, calibratedphase, AWSversion, m, test)
                liveupdate = False
                submit_batchprocess(c, liveupdate, calibratedphase, AWSversion, m, test)
        else:
            calibratedphase = False
            liveupdate = True
            submit_batchprocess(center, liveupdate, calibratedphase, AWSversion, m, test)
            liveupdate = False
            submit_batchprocess(center, liveupdate, calibratedphase, AWSversion, m, test)
    if STEP ==4:
        #export dynamo and aws sync all
        #the initial export job submits separate batch jobs
        #for each missions to speed up the json subsetting of dynamo table data
        #also submits sync for each contributed mission_year
        submit_export(AWSversion)

    if STEP == 5:
        #rerun all json files based on the list of log files
        rerun_log_file("Utilities/log.lst", AWSversion)

    if STEP == 6:
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
            "mission": m,
            'process_date': '',
            'command': ''
        }

        while d <= edate:
            job_tracking['jobname'] = f"check-dynamo-{m}-{d.strftime('%Y%m%d')}"
            job_tracking['command'] = ["liveupdate_wrapper", "check_Dlinks", AWSversion, "--mission", m, "--datestr", d.strftime("%Y-%m-%d")]
            print(f"submitting: check-dynamo-{m}-{d.strftime('%Y%m%d')}")

            dependsID = track.main(job_tracking)
            d += datetime.timedelta(days=1)