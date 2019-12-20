FROM ubuntu:16.04

USER root

ARG HDFS_VERSION
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y software-properties-common && \
  apt-get update && add-apt-repository ppa:openjdk-r/ppa && \
  apt-get update && apt-get install -y \
  curl \
  tar \
  iputils-ping \
  krb5-user \
  libssl-dev:amd64 \
  openjdk-8-jdk \
  openssh-server \
  openssh-client \
  vim \
  sudo \
  lsof \
  man

# Setting up system users & groups
RUN groupadd -r supergroup && \
  groupadd -r hdfs && \
  useradd -r -m -s /bin/bash -g supergroup hdfs && \
  usermod -a -G hdfs hdfs && \
  sed -i -e 's/%sudo\s\+ALL=(ALL:ALL)\s\+ALL/%sudo ALL=(ALL) NOPASSWD:ALL/g' /etc/sudoers

# SSH
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# Download jars and set up directory
RUN curl -s https://archive.apache.org/dist/hadoop/common/hadoop-${HDFS_VERSION}/hadoop-${HDFS_VERSION}.tar.gz \
  | tar -xz -C /opt
RUN cd /opt && ln -s ./hadoop-${HDFS_VERSION} hadoop

# Environment variables
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_PREFIX /opt/hadoop
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_COMMON_HOME /opt/hadoop
ENV HADOOP_HDFS_HOME /opt/hadoop
ENV HADOOP_CONF_DIR /opt/hadoop/etc/hadoop

ENV PATH $HADOOP_HOME/bin:$PATH

ADD etc/hdfs-site.xml $HADOOP_CONF_DIR
ADD etc/core-site.xml $HADOOP_CONF_DIR

# SSH configuration
ADD ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config
RUN chown root:root /root/.ssh/config

# fix the 254 error code
RUN sed  -i "/^[^#]*UsePAM/ s/.*/#&/"  /etc/ssh/sshd_config
RUN echo "UsePAM no" >> /etc/ssh/sshd_config
RUN echo "Port 2122" >> /etc/ssh/sshd_config

# Fix JAVA_HOME because of ssh
RUN sed -i -e 's/^export JAVA_HOME=.*/JAVA_HOME=\/usr\/lib\/jvm\/java-8-openjdk-amd64/' \
  $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Start SSH server
RUN service ssh start

# Format the namespace
RUN $HADOOP_HOME/bin/hdfs namenode -format

ADD bootstrap_hdfs.sh /etc/bootstrap_hdfs.sh
RUN chmod 700 /etc/bootstrap_hdfs.sh

WORKDIR /

ENTRYPOINT ["/etc/bootstrap_hdfs.sh"]

EXPOSE 50010 50020 50070 50075 8020
