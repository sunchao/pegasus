FROM hdfs

USER root

ARG HIVE_VERSION

RUN apt-get update && \
  apt-get install -y postgresql && \
  apt-get install -y libpostgresql-jdbc-java && \
  apt-get install -y lsof man

# Setting up system users & groups
RUN groupadd -r hive && \
  useradd -r -m -s /bin/bash -g supergroup hive && \
  usermod -a -G hdfs hive && \
  usermod -a -G hive hive

# Download jars and set up directory
RUN curl -s https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
  | tar -xz -C /opt
RUN cd /opt && ln -s ./apache-hive-${HIVE_VERSION}-bin hive

# Environment variables
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_COMMON_HOME /opt/hadoop
ENV HADOOP_HDFS_HOME /opt/hadoop
ENV HADOOP_CONF_DIR /opt/hadoop/etc/hadoop

ENV HIVE_HOME /opt/hive
ENV HIVE_CONF_DIR $HIVE_HOME/conf
ENV PATH $HIVE_HOME/bin:$PATH

RUN sed -i -e s/HOSTNAME/hdfs-box/ $HADOOP_HOME/etc/hadoop/core-site.xml

ADD etc/hive-site.xml $HIVE_CONF_DIR
ADD etc/hive-log4j.properties $HIVE_CONF_DIR
ADD etc/log4j.properties $HIVE_CONF_DIR

# Download Postgres client jar
RUN curl -s https://jdbc.postgresql.org/download/postgresql-9.4.1209.jre7.jar -o \
  $HIVE_HOME/lib/postgresql-9.4.1209.jre7.jar

USER postgres

ENV PGPASSWORD "hive"

# Initialize hive metastore db
RUN cd $HIVE_HOME/scripts/metastore/upgrade/postgres/ && \
  /etc/init.d/postgresql start && \
  psql --command "CREATE DATABASE metastore;" && \
  psql --command "CREATE USER hive WITH PASSWORD 'hive';" && \
  psql --command "ALTER USER hive WITH SUPERUSER;" && \
  psql --command "GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;" && \
  psql -U hive -d metastore --no-password -h localhost -f hive-schema-${HIVE_VERSION}.postgres.sql

USER root

# Create HDFS directory for hive user
# RUN hdfs dfs -mkdir -p /user/hive
# RUN hdfs dfs -chown -R hive /user/hive

ADD bootstrap_hive.sh /etc/bootstrap_hive.sh
RUN chmod 700 /etc/bootstrap_hive.sh

CMD ["/etc/bootstrap_hive.sh"]

EXPOSE 9083 10000 10002 50111
