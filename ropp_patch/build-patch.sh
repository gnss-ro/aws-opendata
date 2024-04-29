#!/usr/bin/bash
# Shell script to add patch files to their correct location in the ROPP

# Copy ropp_io files from patch 
cp -a patch/ropp_io ropp-11.0

# Copy ropp_pp files from patch 
cp -a patch/ropp_pp  ropp-11.0

#  Rebuild ropp_io with patch
cd ropp-11.0/ropp_io
# Redo automakes 
aclocal -I m4 --force
automake -a -c
autoconf
echo "Rebuilding ropp_io..."
# Run actual build 
./configure --prefix=${1-$ROPP_ROOT/gfortran} 
make clean
make 
make install
echo "ropp_io rebuild complete. Check above for errors."

# Rebuild ropp_pp
cd ropp-11.0/ropp_pp 
aclocal -I m4 --force
automake -a -c
autoconf
echo "Rebuilding ropp_pp..."
./configure 
make clean
make 
make install
echo "ropp_pp rebuild complete. Check above for errors"
