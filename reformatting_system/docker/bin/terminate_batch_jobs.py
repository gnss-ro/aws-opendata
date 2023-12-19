import boto3
import os


session = boto3.session.Session( profile_name="default", region_name="us-east-1" )
client = session.client('batch')
for status in ['RUNNABLE', 'SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'RUNNABLE' ]: #'RUNNABLE'
    response = client.list_jobs(
        jobQueue= 'ro-processing-EC2', #'ro-processing-EC2' 'ro-processing-SPOT'
        jobStatus= status,
        maxResults=1000,
    )

    for each in response['jobSummaryList']:
        response2 = client.terminate_job(
            jobId=each['jobId'],
            reason='kill'
        )
    print('done with loop')
