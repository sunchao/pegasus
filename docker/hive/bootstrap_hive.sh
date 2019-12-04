#!/bin/bash

/etc/init.d/postgresql start
echo "Waiting until postgres come up ..."
/etc/wait-for-it.sh localhost:5432 -t 60

$HIVE_HOME/bin/hive --service metastore &
echo "Waiting until metastore come up ..."
/etc/wait-for-it.sh localhost:9083 -t 60

$HIVE_HOME/bin/hive --service hiveserver2 &
echo "Waiting until hiveserver2 come up ..."
/etc/wait-for-it.sh localhost:10000 -t 60

echo "Creating HDFS home directory for user 'hive'"
hdfs dfs -mkdir -p /user/hive
hdfs dfs -chown -R hive /user/hive

/bin/bash
