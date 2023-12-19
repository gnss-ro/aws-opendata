#!/usr/bin/env csh

eval saml2aws script -a aernasaprod
env | grep "^AWS" >! envs.lis
docker run -it --rm --env TEST=1 --env-file envs.lis -v "$PWD":/mnt ro-processing-framework bash

