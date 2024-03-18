#!/bin/bash
# Shell script to add patch files to their correct location in the ROPP

# Untar ROPP
tar -xzf ropp-11.0.tar.gz ropp-11.0

# Copy ropp_io files from patch 
cp -a patch/ropp_io/build/ ropp-11.0/ropp_io/build
cp -a patch/ropp_io/ncdf/ ropp-11.0/ropp_io/ncdf
cp -a patch/ropp_io/ropp/ ropp-11.0/ropp_io/ropp
cp -a patch/ropp_io/tools/ ropp-11.0/ropp_io/tools

# Copy ropp_pp files from patch 
cp -a patch/ropp_pp/data/  ropp-11.0/ropp_pp/data
cp -a patch/ropp_pp/tools/  ropp-11.0/ropp_pp/tools
cp -a patch/ropp_pp/preprocess/  ropp-11.0/ropp_pp/preprocess
cp -a patch/ropp_pp/tests/  ropp-11.0/ropp_pp/tests