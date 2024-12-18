 FROM public.ecr.aws/amazonlinux/amazonlinux:2023

###################################################
# misc. initial installs
###################################################
RUN yum update
RUN yum update
RUN yum install -y bzip2 tar m4 vim make cmake zlib-devel libxml2 bash tcsh man
RUN yum install -y libxml2-devel libxslt-devel python-devel
RUN yum install -y gdb

RUN yum groupinstall -y "Development Tools"
ENV CC=gcc CXX=g++ FC=gfortran F90=gfortran F77=gfortran

#copied over from old Dockerfile
#TO-DO: check which of these are already handled by groupinstall of Development Tools in base
RUN yum install -y bison bison-devel byacc blas atlas
RUN yum install -y diffutils
RUN yum install -y gcc 
RUN yum install -y gcc-c++
RUN yum install -y gcc-gfortran
RUN yum install -y libxml2-devel #needed for netcdf
RUN yum install -y libcurl-devel #needed for netcdf
RUN yum install -y vi

RUN dnf install -y 'dnf-command(config-manager)'
RUN dnf config-manager --add-repo https://al2023-repos-us-west-2-de612dc2.s3.dualstack.us-west-2.amazonaws.com/core/mirrors/2023.0.20230222/debuginfo/x86_64/mirror.list

ENV ROPP_ROOT "/app/ropp/ropp-11.0"
ENV BUFR_TABLES "${ROPP_ROOT}/data/bufr/"

#########################################################
#  Instructions for compiling and linking. 
#########################################################

ENV CPPFLAGS="-I${ROPP_ROOT}/gfortran/include" \
	FCFLAGS="-g -I${ROPP_ROOT}/gfortran/include" \
	F77="gfortran" FFLAGS="-g -I${ROPP_ROOT}/gfortran/include" \
	LDFLAGS="-L${ROPP_ROOT}/gfortran/lib -L${ROPP_ROOT}/gfortran/lib64" \
	LD_LIBRARY_PATH="${ROPP_ROOT}/gfortran/lib:${LD_LIBRARY_PATH}" 

WORKDIR ${ROPP_ROOT}

########################################################## 
# Download ROPP deps and build software
##########################################################

ADD ropp-11.0.tar.gz /app/ropp
ADD zlib-1.2.11.tar.gz ./
ADD eccodes-2.22.0-Source.tar.gz ./

RUN yum install -y zlib-devel

##########################################################
#  ROPP standard build code. 
##########################################################

ADD bufr-24.0.2a.tar.gz ./
ADD bufr_000387a.tar.gz ./
ADD grib_api-1.14.5-Source.tar.gz ./

RUN ./build_deps ${FC} ecbufr
RUN ./build_deps ${FC} eccodes

##########################################################
#  Replacement builds for Docker image. 
##########################################################

#  Build HDF5, a prerequisite of NetCDF-4. 

ADD hdf5-1.14.3.tar.gz ./
RUN cd ${ROPP_ROOT}/hdf5-1.14.3 && ./configure --prefix=${ROPP_ROOT}/gfortran --with-zlib=/usr --disable-shared 
RUN cd ${ROPP_ROOT}/hdf5-1.14.3 && make && make install
RUN rm -fr hdf5-1.14.3

#  Build NetCDF-4. 

ENV CPPFLAGS="-I${ROPP_ROOT}/gfortran/include -I/usr/include" LIBS="-L${ROPP_ROOT}/gfortran/lib -lhdf5_hl -lhdf5 -L/usr/lib64 -lz -ldl"

#  NetCDF C collection. 

ADD netcdf-c-4.9.2.tar.gz ./
RUN cd ${ROPP_ROOT}/netcdf-c-4.9.2 && ./configure --prefix=${ROPP_ROOT}/gfortran --disable-dap --enable-netcdf4 --disable-shared --disable-byterange
RUN cd ${ROPP_ROOT}/netcdf-c-4.9.2 && make && make install
RUN rm -fr netcdf-c-4.9.2

#  NetCDF Fortran collection. 

ADD netcdf-fortran-4.6.1.tar.gz ./
RUN cd netcdf-fortran-4.6.1 && ./configure --prefix=${ROPP_ROOT}/gfortran --disable-shared 
RUN cd netcdf-fortran-4.6.1 && make && make install
RUN rm -fr netcdf-fortran-4.6.1

ENV PATH="${ROPP_ROOT}/gfortran/bin:$PATH"
ENV MANPATH="${ROPP_ROOT}/gfortran/man"

######################################################
#    Install miniconda
######################################################

ENV MINICONDA="/app/miniconda3.10"

ADD Miniconda3-py310_*-Linux-*.sh ./
RUN if [ `arch` == "aarch64" ] ; then \
	    bash Miniconda3-py310_*-Linux-aarch64.sh -b -p ${MINICONDA} ; \
	else \
	    bash Miniconda3-py310_*-Linux-x86_64.sh -b -p ${MINICONDA} ; \
	fi 
RUN rm Miniconda3-py310_*-Linux-*.sh 
ENV PATH="${MINICONDA}/bin:$PATH"

######################################################
#    Install python packages
######################################################

RUN conda config --set solver classic
RUN conda install -c conda-forge netcdf4
RUN conda install matplotlib -y
RUN conda install scipy -y



######################################################
#     Build ropp_utils (not impacted by patch)
######################################################


WORKDIR /app/ropp

ENV FFLAGS="-I${ROPP_ROOT}/gfortran/include -I/usr/include"
RUN cd /app/ropp/ropp-11.0/ropp_utils && ./configure --prefix=${1-$ROPP_ROOT/gfortran}
RUN cd /app/ropp/ropp-11.0/ropp_utils && make && make install


######################################################
#     Complete setup
######################################################

RUN mkdir -p /opt/bin
COPY docker-build-patch.sh /opt/bin
RUN chmod a+x /opt/bin/docker-build-patch.sh
ENV PATH="/opt/bin:$PATH"

WORKDIR /mnt
CMD [ '/bin/bash' ]



