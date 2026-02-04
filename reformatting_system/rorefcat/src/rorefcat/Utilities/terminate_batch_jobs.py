import boto3
import os

#  Create session.
session = boto3.session.Session( profile_name="nasa", region_name="us-east-1" )

def main(): 

    client = session.client('batch')
    for status in ['RUNNABLE','RUNNING']:#, 'SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'RUNNABLE' ]: #'RUNNABLE'
        response = client.list_jobs(
            jobQueue= 'ro-processing-EC2', #'ro-processing-EC2' 'ro-processing-SPOT'
            jobStatus= status,
            maxResults=1500,
        )

        for each in response['jobSummaryList']:
            response2 = client.terminate_job(
                jobId=each['jobId'],
                reason='kill'
            )
            print("kill",each['jobName'])
            
        print('done with loop')


if __name__ == "__main__": 
    main()
    pass

