#!/bin/bash

set -ex

PROJ_DIR=/opt/pegasus
VERSION=0.1.0-SNAPSHOT
PROJ_NAME=pegasus-client-assembly-${VERSION}

cd /tmp
if [ -d $PROJ_NAME ]; then
  rm -rf $PROJ_NAME
fi

echo "Launching client ..."
tar xvf $PROJ_DIR/pegasus-client/build/distributions/$PROJ_NAME.tar
java -Dlog4j.configurationFile=$PROJ_DIR/conf/log4j2.yml \
     -cp "$PROJ_NAME/lib/*" \
     com.uber.pegasus.example.TableCat localhost 14000
