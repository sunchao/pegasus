FROM ubuntu:16.04

RUN apt-get update && apt-get install -y \
  wget \
  openjdk-8-jdk

ARG MIRROR=http://archive.apache.org
ARG VERSION
ENV ZK_HOME=/opt/zk
ENV ZK_CONF_DIR=$ZK_HOME/conf
ENV ZK_DATA_DIR=/data
ENV ZK_DATA_LOG=/datalog
ENV ZK_LOG_DIR=/var/log/zk

LABEL name="zookeeper" version=$VERSION

RUN mkdir -p $ZK_HOME && \
  if [ ! -d $ZK_DATA_DIR ]; then mkdir -p $ZK_DATA_DIR; fi && \
  if [ ! -d $ZK_DATALOGDIR ]; then mkdir -p $ZK_DATALOGDIR; fi && \
  if [ ! -d $ZK_LOG_DIR ]; then mkdir -p $ZK_LOG_DIR; fi && \
  wget -q -O - $MIRROR/dist/zookeeper/zookeeper-$VERSION/apache-zookeeper-$VERSION-bin.tar.gz \
  | tar -xzC $ZK_HOME --strip-components=1

WORKDIR /opt/zk

COPY etc/zoo.cfg $ZK_CONF_DIR
COPY etc/log4j.properties $ZK_CONF_DIR

RUN echo "dataDir=$ZK_DATA_DIR" >> $ZK_CONF_DIR/zoo.cfg
RUN echo "dataLogDir=$ZK_DATA_LOG" >> $ZK_CONF_DIR/zoo.cfg
RUN echo "server.1=zk-box:2888:3888" >> $ZK_CONF_DIR/zoo.cfg
RUN if [ ! -f $ZK_DATA_DIR/myid ]; then echo "1" > $ZK_DATA_DIR/myid; fi

ENV PATH=$PATH:$ZK_HOME/bin
VOLUME [$ZK_CONF_DIR, $ZK_DATA_DIR, $ZK_LOG_DIR]

EXPOSE 2181 2888 3888

ENTRYPOINT ["/bin/sh", "-c", "zkServer.sh start-foreground"]
