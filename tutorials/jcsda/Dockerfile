FROM jcsda/docker-gnu-openmpi-dev

WORKDIR /tmp

ENV jedi_cmake_ROOT="/opt/view"
RUN mkdir -p jedi/src && mkdir -p jedi/build
RUN cd jedi/src && git clone https://github.com/JCSDA/fv3-bundle.git
COPY build_jedi.sh /tmp/build_jedi.sh
RUN bash ./build_jedi.sh 
RUN rm -fr build_jedi.sh jedi

WORKDIR /home/nonroot
USER nonroot

