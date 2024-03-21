#!/bin/bash

######################################################
#    Point ROPP to data file locations
######################################################

# Use included EGM96 geoid, WGS84 ellipsoid, and MSIS files
cp  $ROPP_ROOT/ropp_pp/data/MSIS_coeff.nc ./MSIS_coeff.nc
export GEOPOT_COEF=$ROPP_ROOT/ropp_pp/data/egm96.dat
export GEOPOT_CORR=$ROPP_ROOT/ropp_pp/data/corrcoeff.dat

######################################################
#    Build section of ROPP that is changed by patch
######################################################

tar -xzvf patch.tar.gz

#  Build ropp_io
cp -ra ./patch/ropp_io/. $ROPP_ROOT/ropp_io/
# Redo automakes to include aws2ropp
cd /app/ropp/ropp-11.0/ropp_io && aclocal -I m4 --force
cd /app/ropp/ropp-11.0/ropp_io && automake -a -c
cd /app/ropp/ropp-11.0/ropp_io && autoconf
cd /app/ropp/ropp-11.0/ropp_io && ./configure --prefix=${1-$ROPP_ROOT/gfortran} --without-sofa
cd /app/ropp/ropp-11.0/ropp_io && make && make install

cp -ra ./patch/ropp_pp/. $ROPP_ROOT/ropp_pp/
cd /app/ropp/ropp-11.0/ropp_pp && aclocal -I m4 --force
cd /app/ropp/ropp-11.0/ropp_pp && automake -a -c
cd /app/ropp/ropp-11.0/ropp_pp && autoconf
cd /app/ropp/ropp-11.0/ropp_pp && ./configure --without-sofa
cd /app/ropp/ropp-11.0/ropp_pp && make && make install
