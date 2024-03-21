Patch to allow ROPP-11.0 to accept calibratedPhase files from aws-opendata project. 
============================================

**AWS Location**: s3://gnss-ro-data

**AWS Region**: us-east-1  

**Managing Organization**: Atmospheric and Environmental Research, Inc.

*Correspondence:* Stephen Leroy (sleroy@aer.com) or Amy McVey (amcvey@aer.com)



## Introduction

The tarball contains files that must be moved to the correct location to replace the corresponding files in the ROPP-11.0, which can be downloaded and built following instructions from ROMSAF at https://rom-saf.eumetsat.int/ropp/index.php 

The files and their locations are listed below. 

1. In ropp_io/ 
	1. build/ Makefile.am (take care not to confuse with Makefiles in other locations)
	2. ncdf/ ncdf_getvar.f90
	3. ropp/ ropp_io_assign.f90, ropp_io_free.f90, ropp_io_init.f90, ropp_io_read.f90, ropp_io_read_ncdf_get.f90, ropp_io_types.f90, ropp_io_write_ncdf_def.f90, ropp_io_write_ncdf_put.f90
	4. tools/ Makefile.am, ropp2ropp.1, aws2ropp.f90**, aws2ropp.1**	
2. In ropp_pp/
	1. data/ ropp_pp_wopt_tool.nc*
	2. tools/ ropp_pp_occ_tool.f90
	3. preprocess/ ropp_pp_preprocess.f90
	4. tests/ it_pp_01.py*, it_pp_spectra_dt.py*, , it_pp_spectra_ep.py* it_pp_wopt_01.py*, it_pp_wopt_02.py*
*These files are new, rather than replacing previous ROPP files. They are added to allow tests to be run with Python rather than IDL. 
**These files are new rather than replacing a previous ROPP file. They converts calibratedPhase (AWS-format) NCDF files to ROPP-format NCDF files.

## Instructions for default installation

Download the ROPP from ROMSAF at https://rom-saf.eumetsat.int/ropp/index.php with all dependencies and install according to the included instructions. Be sure to select "99.0 Complete package distribution." 

With the ROPP installed, the patch can be installed by unpacking patch.tar.gz then running the build-patch.sh executable, which will move the relevant files in the patch to the correct locations in the ROPP. After rerunning the configure and make commands (as instructed in the ROPP), the patch can then be used as normal. 

For the Python tests to work, the following packages are also required: netcdf4, matplotlib, scipy, matplotlib, math, argparse

## Instructions to build with the included Dockerfile

1. Download the original ROPP package and all dependencies except for netCDF-C, netCDF-Fortran, and HDF5, from https://rom-saf.eumetsat.int/ropp/index.php. Place these into the ropp_patch directory.
2. To be compatible with our base Linux version (Amazon Linux 2023), installing the ROPP required different versions of several of the dependency packages than the versions included in the ROPP. Download tarballs of each of the dependencies below to the ropp_patch directory. 
	1. netCDF-C 4.9.2 (https://downloads.unidata.ucar.edu/netcdf/)
	2. netCDF-Fortran 4.6.1 (https://github.com/Unidata/netcdf-fortran/releases/tag/v4.6.1)
	3. HDF5 1.14.3 (https://portal.hdfgroup.org/downloads/index.html)
3. Download the Linux installers for Miniconda 3.10 from https://docs.anaconda.com/free/miniconda/miniconda-other-installer-links/. The Dockerfile will search for either the 64-bit of the aarch 64-bit versions, depending on your internal archictecture (**not** on the architecture of the Docker base image). If you are unsure of your system architecture, download both.
4. Build the Docker image with "docker build -t ropp11-patch .". This step may take about 20 minutes the first time, as building some of the dependencies (especially HDF5) is time-consuming.
5. Run the Docker image with "docker run -it --rm -v "$PWD":/mnt ropp11-patch bash". 
6. Once once inside the Docker image, complete the build of the patch using the shell script by entering "bash docker-build-patch.sh". You should now have a working version of ROPP-11.0 with the AWS-opendata patch running inside a Docker image. 

## Purpose of each file change in the patch

Modified directories: ropp_io, ropp_pp

1. ropp_io/ 
	1. build/ 
		1. Makefile.am has been updated to build aws2ropp
	2. ncdf/
		1. edited ncdf_getvar.f90 to interpret variables when the input file has 2 or more phase codes present
	3. ropp/ 
		1. ropp_io_assign, add our additional variables (L1_navbits, l2_navbits, L1_freq, L2_freq) for memory assignment
		2. ropp_io_free, add our additional variables  (L1_navbits, l2_navbits, L1_freq, L2_freq) for freeing up memory space
		3. ropp_io_init, allocate our additional variables  (L1_navbits, l2_navbits, L1_freq, L2_freq)
		4. ropp_io_read, accept AWS as a data processing center
		5. ropp_io_read_ncdf_get, read in new Lev1a variables, interpret AWS calibratedPhase files correctly (even with varying numbers of phase codes supplied)
		6. ropp_io_types, added additional variables to L1a types  (L1_navbits, l2_navbits, L1_freq, L2_freq)
		7. ropp_io_write_ncdf_def, add definitions for our new variables
		8. ropp_io_write_ncdf_put,  added additional variables  (L1_navbits, l2_navbits, L1_freq, L2_freq)
	4. tools/
		1. ropp2ropp.1, added aws2ropp to list of conversion scripts
		2. created aws2ropp.f90 (and, correspondingly, aws2ropp.1) to transform AWS-formated calibratedPhase files to ROPP-formatted NCDF files (note this is an intermediate step before actually running the ROPP to compute higher level data)
2. ropp_pp
	1.  data/
		1. added ropp_pp_wopt_tool.nc from a sample occultation to allow ropp_pp_wopt_01.py to run for testing. Has to be run with the correct corresponding file, but can also be recreated with ropp_pp_wopt_tool
	2. tools/
		1. ropp_pp_occ_tool, added parameters to allow for interpolation of AWS data (going from ECF positions to ECI velocities).
	3. preprocess/
		1. ropp_pp_preprocess, added AWS as a processing center and directed to call the correct ropp_pp_preprocess function based on the LEO ID.
	4. tests/
		1. it_pp_01.py, it_pp_spectra_dt.py, it_pp_spectra_ep.py, it_pp_wopt_01.py, it_pp_wopt_02.py; created Python versions of IDL test scripts


*Last update: 21 March 2024*
