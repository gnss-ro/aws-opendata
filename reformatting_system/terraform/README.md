# Purpose
To create a batch environment for ro-processing and liveupdate process to webscrape the UCAR and ROMSAF ftp sites for newer RO events.

# Architecture
This folder contains the code necessary to create the AWS resources to run the liveupdate process for GNSS RO.  It uses Terraform by Hashicorp utilizing infrastructure as code.  Each major resource is contained in it's own *.tf file for organizational purposes.  The important one to check before running anything is the terraform.tfvars which lists specific parameters to your AWS environemnt.

## Workflow
1. Lambda is called once a month to check the respective processing center's FTP site for new RO.  This code is written in python and is found in lambda/ucarWebScrapeToS3 and lamba/romsafWebScrapeToS3 respectively.  See thier readme files linked below.  
2. With a list of new RO daily tarballs to process, lambda submits an AWS Batch job for each tarball.  Here the docker code is utilized with lambda providing the necessary parameters.
3. Docker container code processes files, updates metadata table and puts the newly formatted file on S3.

## Running Terraform locally
cd terraform
terraform init
terraform plan
terraform apply

When making any changes to the terraform code make sure to be logged in
to a valid AWS account to use the above commands.

### to unlock state
if needed:
terraform force-unlock 0

## AWS Batch  - Compute
environments
setups
run requirements

### EC2 types
(https://aws.amazon.com/ec2/spot/instance-advisor/)  
c5.large : on-demand $0.085, vCPU 2, 4 GB RAM  
c5a.large : on-demand $0.077, vCPU 2, 4 GB RAM  
m5a.large : on-demand $0.086, vCPU 2, 4 GB RAM  
r5a.large : on-demand $0.113, vCPU 2, 16 GB RAM (for createjobs full cosmic1)  

## Container image - ECR
Docker container to process ro files from all processing centers into the single format created for this NASA ACCESS project.  For more, see ../docker/ro-processing-framework/readme.md

# Liveupdates
Lambda functions exist to check respective ftp sites and get new RO events (daily tarballs), process these files, and then add to the Open Data Bucket.

## UCAR
see lambda/ucarWebScrapeToS3/readme.md

## ROMSAF
see lambda/romsafWebScrapeToS3/readme.md

