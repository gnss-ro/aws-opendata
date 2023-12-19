FROM public.ecr.aws/amazonlinux/amazonlinux:2023

WORKDIR /tmp

#  Build OS.

RUN yum -y install csh bash less gzip wget

#  Install miniconda 3.8 and needed modules.

ADD https://repo.anaconda.com/miniconda/Miniconda3-py38_4.10.3-Linux-x86_64.sh miniconda.sh
RUN sh miniconda.sh -b -p /opt/miniconda3.8 && rm -fr miniconda.sh
ENV PATH="/opt/miniconda3.8/bin:$PATH"
RUN conda update -n base -c defaults conda
RUN conda install -n base conda-libmamba-solver
RUN conda config --set solver libmamba
RUN conda install -c anaconda numpy=1.21.2 boto3=1.20.24 netcdf4=1.5.3 astropy=4.3.1 pandas
RUN conda install -c conda-forge ftputil
RUN pip3 install awscli==1.20.41
# RUN conda install -c conda-forge awscli
RUN pip install s3fs

#  Install python tools.

ENV PYTHONPATH="/opt/local/python/"
RUN mkdir -p $PYTHONPATH

COPY Utilities/ $PYTHONPATH/Utilities/
COPY GNSSsatellites/ $PYTHONPATH/GNSSsatellites/
COPY Missions/ $PYTHONPATH/Missions/
COPY Versions/ $PYTHONPATH/Versions/
COPY Reformatters/ $PYTHONPATH/Reformatters/
COPY Database/ $PYTHONPATH/Database/
COPY Webscrape/ $PYTHONPATH/Webscrape/

#  Install commands.

ENV PATH="/opt/bin:$PATH"
COPY bin/ /opt/bin/

#  Initialize.

RUN mkdir -p /opt/local/share
ENV SATELLITEHISTORY="/opt/local/share/SATELLIT_I20.SAT"
COPY bin/SATELLIT_I20.SAT $SATELLITEHISTORY
RUN initialize

RUN createjobs -h && batchprocess -h && liveupdate_wrapper -h

#  Entrypoint

CMD [ "/bin/bash" ]
