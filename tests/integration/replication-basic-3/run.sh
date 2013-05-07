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

sleep 2

$HT_HOME/bin/ht start-test-servers.sh --clear --no-thriftbroker \
    --config=$SCRIPT_DIR/hypertable.cfg
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=rangeserver2.pid \
    --Hypertable.RangeServer.ProxyName=rs2 \
    --Hypertable.RangeServer.CommitLog.FragmentRemoval.Disable=true \
    --Hypertable.RangeServer.Port=38061 2&> rangeserver2.output&

$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Master.Port=38101 \
    --Hypertable.Replication.TestMode=true \
    --Hypertable.Replication.BaseNamespace=/rep &> repmaster2.log &
if [ $? -ne "0" ];
then
  echo "Replication.Master 2 failed to start"
  exit 1
fi
$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    &> repmaster1.log &
if [ $? -ne "0" ];
then
  echo "Replication.Master 1 failed to start"
  exit 1
fi

sleep 4

$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.Port=38102 \
    --Hypertable.Replication.Slave.MasterAddress=localhost:38101 \
    --Hypertable.Replication.Slave.ProxyName=repslave2 \
    --Hypertable.Replication.BaseNamespace=/rep &> repslave2.log &
if [ $? -ne "0" ];
then
  echo "Replication.Slave 2 failed to start"
  exit 1
fi
$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.Port=38103 \
    --Hypertable.Replication.Slave.ProxyName=repslave1 \
    &> repslave1.log &
if [ $? -ne "0" ];
then
  echo "Replication.Slave 1 failed to start"
  exit 1
fi

sleep 5

# setup the tables
cat $SCRIPT_DIR/setup.hql | $HT_HOME/bin/ht shell --namespace / --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg
if [ $? -ne "0" ];
then
  echo "Failed to create the tables"
  exit 1
fi

# wait for the schema update to propagate
sleep 10

# run load generator and fill the database with key/value pairs of about
# 10 Megabyte in total
$HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec \
    --max-bytes=5M \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.Mutator.FlushDelay=250

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
