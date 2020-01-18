FROM ubuntu:16.04

ARG HIVE

# Download jars and set up directory
RUN curl -s https://archive.apache.org/dist/hive/hive-${HIVE}/apache-hive-${HIVE}-bin.tar.gz \
  | tar -xz -C /opt
RUN cd /opt && ln -s ./apache-hive-${HIVE}-bin hive

ENV HIVE_HOME /opt/hive
ENV HIVE_CONF_DIR $HIVE_HOME/conf
ENV PATH $HIVE_HOME/bin:$PATH
