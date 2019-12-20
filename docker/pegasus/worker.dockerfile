FROM ubuntu:16.04

USER root

RUN apt-get update && apt-get install -y \
  gradle \
  openjdk-8-jdk \
  sudo

COPY . /opt/pegasus

WORKDIR /opt/pegasus

ENV CONF_DIR /etc/conf
ENV HADOOP_CONF_DIR $CONF_DIR
ENV HIVE_CONF_DIR $CONF_DIR

RUN mkdir -p $CONF_DIR
COPY docker/hdfs/etc/core-site.xml $CONF_DIR
COPY docker/hdfs/etc/hdfs-site.xml $CONF_DIR
COPY docker/hive/etc/hive-site.xml $CONF_DIR

RUN ./gradlew clean distTar && \
  tar -xf pegasus-worker/build/distributions/pegasus-worker-assembly-*.tar --strip-components=1

ENTRYPOINT java -Dlog4j.configurationFile=conf/log4j2.yml \
  -cp "lib/*" \
  com.uber.pegasus.PegasusWorker 15000

EXPOSE 15000
