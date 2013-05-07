#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
ABC_HOME=$INSTALL_DIR
SCRIPT_DIR=`dirname $0`
# reset INSTALL_DIR or start-test-servers.sh will fail
INSTALL_DIR=

function on_exit() {
  killall Replication.Master
  killall Replication.Slave
  killall Hypertable.RangeServer
}
trap on_exit EXIT

function insert() {
  sleep 2
  $HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec \
      --max-bytes=1M \
      --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
      --Hypertable.Mutator.FlushDelay=250
  sleep 5
}

# start the Hypertable cluster
$HT_HOME/bin/ht start-test-servers.sh --clear --no-thriftbroker \
    --config=$SCRIPT_DIR/hypertable.cfg
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=rangeserver2.pid \
    --Hypertable.RangeServer.ProxyName=rs2 \
    --Hypertable.RangeServer.CommitLog.FragmentRemoval.Disable=true \
    --Hypertable.RangeServer.Port=38061 2&> rangeserver2.output&

# now start the replication masters, otherwise we cannot create the table
$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    &> repmaster1.log &
if [ $? -ne "0" ];
then
  echo "Local Replication.Master 1 failed to start"
  exit 1
fi
$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Master.Port=38101 \
    --Hypertable.Replication.TestMode=true \
    --Hypertable.Replication.BaseNamespace=/rep &> repmaster2.log &
if [ $? -ne "0" ];
then
  echo "Remote Replication.Master 2 failed to start"
  exit 1
fi

sleep 10

# setup the tables
cat $SCRIPT_DIR/setup.hql | $HT_HOME/bin/ht shell --namespace / --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg
if [ $? -ne "0" ];
then
  echo "Failed to create the tables"
  exit 1
fi

# insert some data
insert

# now start the local slaves
$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.Port=38107 \
    --Hypertable.Replication.Slave.ProxyName=repslave11 \
    &> repslave11.log &
if [ $? -ne "0" ];
then
  echo "Replication.Slave 1.1 failed to start"
  exit 1
fi

insert

$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.Port=38108 --verbose \
    --Hypertable.Replication.Slave.ProxyName=repslave12 \
    &> repslave12.log &
if [ $? -ne "0" ];
then
  echo "Replication.Slave 1.2 failed to start"
  exit 1
fi

sleep 5

insert

# and the remote slaves
$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.ProxyName=repslave21 \
    --Hypertable.Replication.Slave.Port=38105 \
    --Hypertable.Replication.Slave.MasterAddress=localhost:38101 \
    --Hypertable.Replication.BaseNamespace=/rep &> repslave21.log &
if [ $? -ne "0" ];
then
  echo "Remote Replication.Slave 2.1 failed to start"
  exit 1
fi

insert

$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.Port=38106 --verbose \
    --Hypertable.Replication.Slave.ProxyName=repslave22 \
    --Hypertable.Replication.Slave.MasterAddress=localhost:38101 \
    --Hypertable.Replication.BaseNamespace=/rep &> repslave22.log &
if [ $? -ne "0" ];
then
  echo "Replication.Slave 2.2 failed to start"
  exit 1
fi

# and more data
insert

# wait a bit till the replication finishes
sleep 30

# now compare both "clusters"
echo "SELECT * FROM LoadTest DISPLAY_TIMESTAMPS INTO FILE 'dump1.txt';" | \
    $HT_HOME/bin/ht shell --namespace / --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg
echo "SELECT * FROM LoadTest DISPLAY_TIMESTAMPS INTO FILE 'dump2.txt';" | \
    $HT_HOME/bin/ht shell --namespace /rep --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg

diff dump1.txt dump2.txt &> /dev/null
if [ $? -ne "0" ];
then
  echo "dumps differ"
  cp -r $HT_HOME/fs .
  exit 1
fi

exit 0
