
Patch to allow ROPP-11.0 to accept calibratedPhase files from aws-opendata project. 

The tarball contains files that must be moved to the correct location to replace the corresponding files in the ROPP-11.0, which can be downloaded and built following instructions from ROMSAF at https://rom-saf.eumetsat.int/ropp/index.php 
Be sure to download the "99.0 Complete package distribution"
The files and their locations are listed below. 

1. In ropp_io/
	a) build/ Makefile.am (take care not to confuse with Makefiles in other locations)
	b) ncdf/ ncdf_getvar.f90
	c) ropp/ ropp_io_assign.f90, ropp_io_free.f90, ropp_io_init.f90, ropp_io_read.f90, ropp_io_read_ncdf_get.f90, ropp_io_types.f90, ropp_io_write_ncdf_def.f90, ropp_io_write_ncdf_put.f90
	d) tools/ ropp2ropp.1, aws2ropp.f90**, aws2ropp.1**	
2. In ropp_pp/
	a) data/ ropp_pp_wopt_tool.nc*
	b) tools/ ropp_pp_occ_tool.f90
	c) preprocess/ ropp_pp_preprocess.f90
	d) tests/ it_pp_01.py*, it_pp_spectra_dt.py*, , it_pp_spectra_ep.py* it_pp_wopt_01.py*, it_pp_wopt_02.py*
*These files are new, rather than replacing previous ROPP files. They are added to allow tests to be run with Python rather than IDL. 
**This file is new rather than replacing a previous ROPP file. It converts calibratedPhase (AWS-format) NCDF files to ROPP-format NCDF files.


For the Python tests to work, the following packages are required: netcdf4, matplotlib, scipy, matplotlib, math, argparse

###############################################################################
#
#    Additional instructions to build with the included Dockerfiles
#
###############################################################################

1. Download the original ROPP package and all dependencies according to ROMSAF directions (https://rom-saf.eumetsat.int/ropp/index.php). Create a base image directory and move these tarballs into the base directory. Unpack only the ROPP tarball.
2. To be compatible with our base Linux version (Amazon Linux 2023), installing the ROPP required different versions of several of the dependency packages than the versions included in the ROPP. Download tarballs of each of the dependencies below and add them to the base image directory. 
	a) netCDF-C 4.9.2 (https://downloads.unidata.ucar.edu/netcdf/)
	b) netCDF-Fortran 4.6.1 (https://github.com/Unidata/netcdf-fortran/releases/tag/v4.6.1)
	c) HDF5 1.14.3 (https://portal.hdfgroup.org/downloads/index.html)
Note that you may also choose to remove the (older) ROPP versions of these dependencies if you wish. The Dockerfile is hard-coded to search for versions (a)-(c) of these particular dependencies.
3. Download Miniconda (https://docs.anaconda.com/free/miniconda/miniconda-other-installer-links/) for Python 3.10. The Dockerfile will look for the macOS installers for Apple M1 and x86, in shell scripts ("64-bit bash"). Windows and Linux users should download the appropriate Miniconda installer and edit Dockerfile_base to use that shell script.
4. Move Dockerfile_base to the base directory and rename to "Dockerfile". Use this Dockerfile to build the base image (in bash, type "docker build -t ropp11-base .").
4. In the main directory, unpack a copy of the original ROPP. Follow the instructions above to add/replace the relevant ROPP patch files to the correct locations in the original ROPP. 
5. Use the main Dockerfile to build the ROPP with the patch in the main directory, which will build from the base image you built in steps 1-2. 
6. Run the Docker image. Once inside the image, use build-devel.sh to build the remaining ROPP patch files. You should now have a working version of ROPP-11.0 with the AWS-opendata patch running inside a Docker image. 


###############################################################################
#
#   The purpose of each of the file changes in the patch are included below.
#
###############################################################################
Modified directories: ropp_io, ropp_pp

1. ropp_io/ 
a) build/ 
	-Makefile.am has been updated to build aws2ropp
b) ncdf/
	-edited ncdf_getvar.f90 to interpret variables when the input file has 2 or more phase codes present
c) ropp/ 
	-ropp_io_assign, add our additional variables (L1_navbits, l2_navbits, L1_freq, L2_freq) for memory assignment
	-ropp_io_free, add our additional variables  (L1_navbits, l2_navbits, L1_freq, L2_freq) for freeing up memory space
	-ropp_io_init, allocate our additional variables  (L1_navbits, l2_navbits, L1_freq, L2_freq)
	-ropp_io_read, accept AWS as a data processing center
	-ropp_io_read_ncdf_get, read in new Lev1a variables, interpret AWS calibratedPhase files correctly (even with varying numbers of phase codes supplied)
	-ropp_io_types, added additional variables to L1a types  (L1_navbits, l2_navbits, L1_freq, L2_freq)
	-ropp_io_write_ncdf_def, add definitions for our new variables
	-ropp_io_write_ncdf_put,  added additional variables  (L1_navbits, l2_navbits, L1_freq, L2_freq)
d) tools/
	-ropp2ropp.1, added aws2ropp to list of conversion scripts
	-created aws2ropp.f90 (and, correspondingly, aws2ropp.1) to transform AWS-formated calibratedPhase files to ROPP-formatted NCDF files (note this is an intermediate step before actually running the ROPP to compute higher level data)
c)preprocess/	
2. ropp_pp
a) data/
	-added ropp_pp_wopt_tool.nc from a sample occultation to allow ropp_pp_wopt_01.py to run for testing. Has to be run with the correct corresponding file, but can also be recreated with ropp_pp_wopt_tool
b)tools/
	-ropp_pp_occ_tool, added parameters to allow for interpolation of AWS data (going from ECF positions to ECI velocities).
c)preprocess/
	-ropp_pp_preprocess, added AWS as a processing center and directed to call the correct ropp_pp_preprocess function based on the LEO ID.
d)tests/
	-it_pp_01.py, it_pp_spectra_dt.py, it_pp_spectra_ep.py, it_pp_wopt_01.py, it_pp_wopt_02.py; created Python versions of IDL test scripts
	
