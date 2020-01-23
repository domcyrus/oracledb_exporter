FROM centos:7

ENV ORACLE_VERSION "12.2"
ENV LD_LIBRARY_PATH "/usr/lib/oracle/${ORACLE_VERSION}/client64/lib"
ENV PATH "$PATH:/usr/local/go/bin"

RUN yum -y install wget
RUN wget https://dl.google.com/go/go1.13.4.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.13.4.linux-amd64.tar.gz

COPY oracle-instantclient* /
RUN rpm -Uh --nodeps /oracle-instantclient*.x86_64.rpm && rm /*.rpm 

COPY oci8.pc.template /usr/share/pkgconfig/oci8.pc
RUN sed -i "s/@ORACLE_VERSION@/$ORACLE_VERSION/g" /usr/share/pkgconfig/oci8.pc
RUN echo $LD_LIBRARY_PATH >> /etc/ld.so.conf.d/oracle.conf && ldconfig
RUN yum -y install make automake gcc libaio

WORKDIR /go/src/oracledb_exporter
COPY . .

ARG VERSION
ENV VERSION ${VERSION:-0.5.0}

ENV PKG_CONFIG_PATH /go/src/oracledb_exporter

RUN go build -v -ldflags "-X main.Version=${VERSION} -s -w"

#RUN chmod 755 /oracledb_exporter
#
#EXPOSE 9161
#
#ENTRYPOINT ["/oracledb_exporter"]
