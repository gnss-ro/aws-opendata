#!/usr/bin/env bash

docker pull jcsda/docker-gnu-openmpi-dev
docker build -t "fv3-bundle:latest" .

