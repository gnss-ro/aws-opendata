FROM ropp11-base

WORKDIR /app/ropp

ENV FFLAGS="-I${ROPP_ROOT}/gfortran/include -I/usr/include"

RUN cd /app/ropp/ropp-11.0/ropp_utils && ./configure --prefix=${1-$ROPP_ROOT/gfortran}
RUN cd /app/ropp/ropp-11.0/ropp_utils && make && make install

RUN mkdir -p /opt/bin
COPY build-devel /opt/bin/
RUN chmod a+x /opt/bin/build-devel
ENV PATH="/opt/bin:$PATH"

WORKDIR /mnt
CMD [ '/bin/bash' ]
