FROM public.ecr.aws/amazonlinux/amazonlinux:2023

WORKDIR /tmp

#  Build OS.

RUN dnf -y install csh bash less gzip wget

#  Install miniconda 3.9. 

ADD https://repo.anaconda.com/miniconda/Miniconda3-py39_24.7.1-0-Linux-x86_64.sh miniconda.sh 
RUN sh miniconda.sh -b -p /opt/miniconda3.9 && rm -fr miniconda.sh
ENV PATH="/opt/miniconda3.9/bin:$PATH"

#  Updates. 

RUN conda update -n base -c defaults conda pip 
RUN conda install -c anaconda -c conda-forge ftputil

#  Install RO Reformat and Catalog (rorefcat) system. 

COPY rorefcat.tar ./
RUN pip install ./rorefcat.tar 
RUN rm rorefcat.tar 

#  Initialize.

RUN mkdir -p /opt/local/share
ENV SATELLITEHISTORY="/opt/local/share/SATELLIT_I20.SAT"
COPY SATELLIT_I20.SAT $SATELLITEHISTORY
RUN initialize

RUN createjobs -h 
RUN batchprocess -h 
RUN liveupdate_wrapper -h

#  Entrypoint

CMD [ "/bin/bash" ]

