#!/usr/bin/env sh

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <AWSAccount>.dkr.ecr.us-east-1.amazonaws.com

docker tag ro-processing-framework:latest <AWSAccount>.dkr.ecr.us-east-1.amazonaws.com/ro-processing-framework:latest

docker push <AWSAccount>.dkr.ecr.us-east-1.amazonaws.com/ro-processing-framework:latest
