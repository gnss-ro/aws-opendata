#!/bin/bash

source /etc/profile.d/z10_spack_environment.sh
cd /tmp/jedi/build 
ecbuild -DPython3_EXECUTABLE=$(which python3) /tmp/jedi/src/fv3-bundle
make update
make
ctest

