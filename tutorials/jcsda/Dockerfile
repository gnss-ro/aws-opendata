FROM jcsda/docker-gnu-openmpi-dev

WORKDIR /tmp

ENV jedi_cmake_ROOT="/opt/view"

COPY build_jedi.sh /tmp/build_jedi.sh
RUN mkdir -p jedi/src && mkdir -p jedi/build 
RUN cd jedi/src && git clone https://github.com/JCSDA/fv3-bundle.git
RUN bash ./build_jedi.sh 
RUN rm -fr build_jedi.sh jedi

COPY build_ioda.sh .
RUN mkdir -p ioda-build
RUN git clone -b develop https://github.com/JCSDA/ioda-bundle.git 
RUN rm -f ioda-bundle/CMakeLists.txt
COPY CMakeLists.txt ioda-bundle/
RUN bash ./build_ioda.sh 
# RUN rm -fr build_ioda.sh ioda

# WORKDIR /home/nonroot
# USER nonroot

