#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
HT_SHELL=$HT_HOME/bin/hypertable
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`

. $HT_HOME/bin/ht-env.sh


# make sure that the first master is up
$HT_HOME/bin/serverup "repmaster"
if [ $? -ne "0" ];
then
  echo "not up!"
  exit -1
fi

echo "is up!"

exit 0
