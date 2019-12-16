#!/bin/bash

# TODO: make these steps blocking

/etc/init.d/postgresql start
echo "Waiting 10 seconds until Postgres come up ..."
sleep 10

$HIVE_HOME/bin/hive --service metastore &
echo "Waiting 10 seconds until metastore come up ..."
sleep 10

$HIVE_HOME/bin/hive --service hiveserver2 &
echo "Waiting 10 seconds until hiveserver2 come up ..."
sleep 10

echo "Creating HDFS home directory for user 'hive'"
hdfs dfs -mkdir -p /user/hive
hdfs dfs -chown -R hive /user/hive

/bin/bash
