#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
ABC_HOME=$INSTALL_DIR
SCRIPT_DIR=`dirname $0`
# reset INSTALL_DIR or start-test-servers.sh will fail
INSTALL_DIR=

mkdir $ABC_HOME/run &> /dev/null

killall Replication.Slave
killall Replication.Master

$HT_HOME/bin/ht start-test-servers.sh --clear

$ABC_HOME/bin/Replication.Master --Hypertable.Replication.Master.Port=38100 \
    $@ --pidfile $ABC_HOME/run/Replication.Master.38100.pid \
    --Hypertable.RangeServer.CommitLog.FragmentRemoval.Disable=true \
    --Hypertable.Connection.Retry.Interval=1000 2>&1 &> Replication.Master.38100.log&
sleep 2

$ABC_HOME/bin/Replication.Master --Hypertable.Replication.Master.Port=38101 \
    $@ --pidfile $ABC_HOME/run/Replication.Master.38101.pid \
    --Hypertable.RangeServer.CommitLog.FragmentRemoval.Disable=true \
    --Hypertable.Connection.Retry.Interval=1000 2>&1 &> Replication.Master.38101.log&

sleep 5

# make sure that the first master is up
$HT_HOME/bin/ht serverup "repmaster"
if [ $? -ne "0" ];
then
  echo "Replication.Master is not up!"
  exit -1
fi

# now kill it
kill `cat $ABC_HOME/run/Replication.Master.38100.pid`
wait `cat $ABC_HOME/run/Replication.Master.38100.pid`

sleep 3

# the second master now has to get the lock
$HT_HOME/bin/ht serverup "repmaster" --Hypertable.Replication.Master.Port=38101
if [ $? -ne "0" ];
then
  echo "Replication.Master is not up!"
  exit -1
fi

# clean up and leave
kill `cat $ABC_HOME/run/Replication.Master.38101.pid`
wait `cat $ABC_HOME/run/Replication.Master.38101.pid`

$HT_HOME/bin/ht serverup "repmaster"
if [ $? -eq "0" ];
then
  echo "Replication.Master is up, but shouldn't!"
  exit -1
fi

exit 0
