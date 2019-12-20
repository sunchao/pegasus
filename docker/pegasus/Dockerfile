FROM ubuntu:16.04

USER root

RUN apt-get update && apt-get install -y \
  gradle \
  openjdk-8-jdk \
  sudo

ENV ROLE

COPY . /opt/pegasus

WORKDIR /opt/pegasus

ENV CONF_DIR /etc/conf
ENV HADOOP_CONF_DIR $CONF_DIR
ENV HIVE_CONF_DIR $CONF_DIR

RUN mkdir -p $CONF_DIR
COPY docker/hdfs/etc/core-site.xml $CONF_DIR
COPY docker/hdfs/etc/hdfs-site.xml $CONF_DIR
COPY docker/hive/etc/hive-site.xml $CONF_DIR

COPY docker/pegasus/bootstrap_pegasus.sh /etc/bootstrap_pegasus.sh
RUN chmod 700 /etc/bootstrap_pegasus.sh

RUN ./gradlew clean distTar && \
  tar -xf pegasus-master/build/distributions/pegasus-master-assembly-*.tar --strip-components=1

# ENTRYPOINT ["/etc/bootstrap_pegasus.sh"]

ENTRYPOINT /bin/bash
EXPOSE 14000 15000
