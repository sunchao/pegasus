#!/bin/bash

echo "Waiting for HDFS to come up ..."
/etc/wait-for-it.sh hdfs-box:8020 -t 60

echo "Waiting for Hive metastore to come up ..."
/etc/wait-for-it.sh hive-box:9083 -t 60

java -Dlog4j.configurationFile=conf/log4j2.yml \
     -cp "lib/*" \
     com.uber.pegasus.PegasusMaster 14000
