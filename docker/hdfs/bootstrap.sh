#!/bin/bash

# Replace the place-holder with the actual host name at runtime
sed -i -e s/HOSTNAME/$HOSTNAME/ $HADOOP_HOME/etc/hadoop/core-site.xml

# Start SSH service. Note this cannot be done in the Docker file itself
service ssh start

# Setting up related environment variables
. $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Start the daemons
$HADOOP_HOME/sbin/start-dfs.sh

# Create home directory
hdfs dfs -mkdir -p /user/root

# To make the container running and ssh-able
/bin/bash
