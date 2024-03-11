
Patch to allow ROPP-11.0 to accept calibratedPhase files from aws-opendata project. 

The tarball contains files that must be moved to the correct location to replace the normal ROPP files. The modified ROPP file then function as normal. The files and their locations are listed below.

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
*These files new, rather than replacing previous ROPP files. They are added to allow tests to be run with Python rather than IDL. 
**This file is new rather than replacing a previous ROPP file. It converts calibratedPhase (AWS-format) NCDF files to ROPP-format NCDF files.


For the Python tests to work, the following packages are required: netcdf4, matplotlib, scipy, matplotlib, math, argparse



###############################################################################
#
#     The purpose of each of the file changes are included below.
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
	-added ropp_pp_wopt_tool.nc from a sample occultation to allow ropp_pp_wopt_01.py to run for testing. Has to be run with the correct corresponding file, but can also be recreated with ropp_pp_opt_tool
b)tools/
	-ropp_pp_occ_tool, added parameters to allow for interpolation of AWS data (going from ECF positions to ECI velocities).
c)preprocess/
	-ropp_pp_preprocess, added AWS as a processing center and directed to call the correct ropp_pp_preprocess function based on the LEO ID.
d)tests/
	-it_pp_01.py, it_pp_spectra_dt.py, it_pp_spectra_ep.py, it_pp_wopt_01.py, it_pp_wopt_02.py; created Python versions of IDL test scripts
	
