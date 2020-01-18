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

RUN tar -xf pegasus-worker/build/distributions/pegasus-worker-assembly-*.tar --strip-components=1

COPY docker/pegasus/bootstrap_worker.sh /etc/bootstrap_worker.sh
RUN chmod 700 /etc/bootstrap_worker.sh

COPY docker/hive/wait-for-it.sh /etc/wait-for-it.sh
RUN chmod 700 /etc/wait-for-it.sh

ENTRYPOINT ["/etc/bootstrap_worker.sh"]
