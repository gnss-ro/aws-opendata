import os
import boto3
import sys
import datetime
import subprocess

sys.path.append("../../docker")
import Webscrape.job_tracking as track

#  Create session.
session = boto3.session.Session( profile_name="default", region_name="us-east-1" )
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

            dependsID = track.main(job_tracking)

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
    #command.extend(['--daterange', '2023-03-01:2023-05-31'])
    job_tracking = {}
    job_tracking = {
        'job-date': f"createjobs-{todayDate}",
        'jobname': f'{jobName}-2023',
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

def submit_batchprocess(processing_center, liveupdate, calibratedphase, AWSversion, mission):
    """Submit batchprocess jobs. This will preprocess all UCAR and ROMSAF supplied
    RO data."""

    bucket = s3.Bucket( "gnss-ro-processing-definitions" )
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

        command = ['batchprocess', f"s3://gnss-ro-processing-definitions/{definition}","--version",AWSversion, "--clobber"]

        if mission == "all" or mission in definition :
            njobs += 1
            jobName = f"{AWSversion}_batchprocess-{njobs:07d}.{definition.split('/')[1][:-5]}"
            jobName = jobName.replace('.','_')

            job_tracking = {}
            job_tracking = {
                'job-date': f"batchprocess-{todayDate}",
                'jobname': jobName,
                'test': "false",
                'ram': 1900,
                'version': AWSversion,
                'center': processing_center,
                "mission": os.path.basename(definition).split('-')[0],
                'process_date': os.path.basename(definition).split('.')[1],
                'command': command
            }

            dependsID = track.main(job_tracking)

    return

def createjobs(processing_center, mission = None):
    AWSversion = "1.1"
    mission_list = []

    if mission == None: #get all
        if processing_center == "ucar":
            valid_missions = subprocess.run("ls Missions/", shell=True, capture_output=True)
            valid_missions = valid_missions.stdout.split()
            for each in valid_missions:

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
                submit_createjobs(jobName, command,processing_center,m,lvl)

                jobName = f"createjobs-{processing_center}-{m}-{lvl}"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion]
                submit_createjobs(jobName, command,processing_center,m,lvl)

    elif processing_center == "romsaf":
        for lvl in ["level2a", "level2b"]:
            for m in mission_list:
                jobName = f"createjobs-{processing_center}-{m}-{lvl}-liveupdate"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion, "--liveupdate"]
                submit_createjobs(jobName, command,processing_center,m,lvl)

                jobName = f"createjobs-{processing_center}-{m}-{lvl}"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion]
                submit_createjobs(jobName, command,processing_center,m,lvl)

    elif processing_center == "eumetsat":
        for lvl in ["level1b"]:
            for m in mission_list:
                jobName = f"createjobs-{processing_center}-{m}-{lvl}-liveupdate"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion, "--liveupdate"]
                submit_createjobs(jobName, command,processing_center,m,lvl)
    elif processing_center == "jpl":
        for lvl in ["level1b", "level2a", "level2b"]:                
            for m in mission_list:
                jobName = f"createjobs-{processing_center}-{m}-{lvl}"
                command = ["createjobs", processing_center, m, lvl, "--version", AWSversion]
                submit_createjobs(jobName, command,processing_center,m,lvl)
                                  
if __name__ == "__main__":
    STEP = 1 #for which chunk below to RUN
    AWSversion = "1.1"
    
    '''
    1: Createjobs
    2: Run calibratedPhase
    3: Run other retrevial files
    4: Export dynamo
    5: rerun log files from list
    6: run eumetsat download
    7: run romsaf download (cdr)
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

    if STEP ==1:
        #run createjobs
        #createjobs("jpl")
        #createjobs("jpl", mission = "paz")
        createjobs("ucar",mission="cosmic1")
        #createjobs("romsaf",mission="grace")
        #createjobs("eumetsat",mission="champ")

    if STEP ==2:
        #run calibratedPhase
        calibratedphase = True #for batchprocess to run only calibratedphase or the others
        liveupdate = False #for batchprocess liveupdate/webscrape files
        submit_batchprocess("jpl", liveupdate, calibratedphase, AWSversion, "all")
        liveupdate = True
        #submit_batchprocess("ucar", liveupdate, calibratedphase, AWSversion, "all")
        #submit_batchprocess("eumetsat", liveupdate, calibratedphase, AWSversion, "cosmic1")

    if STEP ==3:
        #run everything else
        calibratedphase = False
        liveupdate = False
        #submit_batchprocess("ucar", liveupdate, calibratedphase, AWSversion, "all")
        submit_batchprocess("jpl", liveupdate, calibratedphase, AWSversion, "all")
        liveupdate = True
        #submit_batchprocess("ucar", liveupdate, calibratedphase, AWSversion, "all")
        #submit_batchprocess("romsaf", liveupdate, calibratedphase, AWSversion, "all")

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
        eumetsatD.submit_jobs("cosmic1")
        
    if STEP == 7:
        
        job_tracking = {}
        job_tracking = {
            'job-date': f"romsafD-{todayMMDD}",
            'test': "false",
            'ram': 3500,
            'version': AWSversion,
            'center': "romsaf",
            "mission": "",
            'process_date': "",
        }
        
        m = "champ"
        for y in ["2001","2002","2003","2004","2005","2006","2007","2008"]:
            for fType in ["wet","atm"]:
                job_tracking['jobname'] = f"romsaf_download_{todayMMDD}_{AWSversion.replace('.','_')}_{m}_{fType}_{y}"
                job_tracking['command'] = ["liveupdate_wrapper", "romsafD", "1.1", "--mission",m,"--year",y,"--prefix", fType]
                
                dependsID = track.main(job_tracking) 
        
        m = "grace"
        for y in ["2007","2008","2009","2010","2011","2012","2013","2014","2015","2016"]:
            for fType in ["wet","atm"]:
                job_tracking['jobname'] = f"romsaf_download_{todayMMDD}_{AWSversion.replace('.','_')}_{m}_{fType}_{y}"
                job_tracking['command'] = ["liveupdate_wrapper", "romsafD", "1.1", "--mission",m,"--year",y,"--prefix", fType]
                
                dependsID = track.main(job_tracking)
                               
        #champ, 2001 - 2008, wet atm
        #grace, 2007 - 2016, wet atm
        
