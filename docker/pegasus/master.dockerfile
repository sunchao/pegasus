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
  tar -xf pegasus-master/build/distributions/pegasus-master-assembly-*.tar --strip-components=1

COPY docker/pegasus/bootstrap_master.sh /etc/bootstrap_master.sh
RUN chmod 700 /etc/bootstrap_master.sh

COPY docker/hive/wait-for-it.sh /etc/wait-for-it.sh
RUN chmod 700 /etc/wait-for-it.sh

EXPOSE 14000

ENTRYPOINT ["/etc/bootstrap_master.sh"]
