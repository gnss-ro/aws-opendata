#!/bin/bash

export FFLAGS-"-I/-I${ROPP_ROOT}/gfortran/include -I/usr/include -I/opt/include -g"

######################################################
#    Point ROPP to data file locations
######################################################

# Use included EGM96 geoid, WGS84 ellipsoid, and MSIS files
cp  $ROPP_ROOT/ropp_pp/data/MSIS_coeff.nc $ROPP_ROOT/MSIS_coeff.nc
export GEOPOT_COEF=$ROPP_ROOT/ropp_pp/data/egm96.dat
export GEOPOT_CORR=$ROPP_ROOT/ropp_pp/data/corrcoeff.dat


######################################################
#    Build section of ROPP that is changed by patch
######################################################

tar -xzvf patch.tar.gz

#  Build ropp_io
cp -ra ./patch/ropp_io/. $ROPP_ROOT/ropp_io/
# Redo automakes to include aws2ropp
cd $ROPP_ROOT/ropp_io && aclocal -I m4 --force
cd $ROPP_ROOT/ropp_io && automake -a -c
cd $ROPP_ROOT/ropp_io && autoconf
cd $ROPP_ROOT/ropp_io && ./configure --prefix=${1-$ROPP_ROOT/gfortran} --without-sofa
cd $ROPP_ROOT/ropp_io && make && make install

cp -ra ./patch/ropp_pp/. $ROPP_ROOT/ropp_pp/
cd $ROPP_ROOT/ropp_pp && aclocal -I m4 --force
cd $ROPP_ROOT/ropp_pp && automake -a -c
cd $ROPP_ROOT/ropp_pp && autoconf
cd $ROPP_ROOT/ropp_pp && ./configure --without-sofa
cd $ROPP_ROOT/ropp_pp && make && make install

# Return home 
cd /mnt
