#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
ABC_HOME=$INSTALL_DIR
SCRIPT_DIR=`dirname $0`
# reset INSTALL_DIR or start-test-servers.sh will fail
INSTALL_DIR=

function on_exit() {
  killall Replication.Master
  killall Replication.Slave
}
trap on_exit EXIT

sleep 2

$HT_HOME/bin/ht start-test-servers.sh --clear --no-thriftbroker \
    --config=$SCRIPT_DIR/hypertable.cfg

$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Master.Port=38101 \
    --Hypertable.Replication.TestMode=true \
    --Hypertable.Replication.BaseNamespace=/rep &> repmaster2.log &
$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    &> repmaster1.log &

sleep 5

$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.Port=38102 \
    --Hypertable.Replication.Slave.ProxyName=repslave2 \
    --Hypertable.Replication.Slave.MasterAddress=localhost:38101 \
    --Hypertable.Replication.BaseNamespace=/rep &> repslave2.log &
$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.ProxyName=repslave1 \
    --Hypertable.Replication.Slave.Port=38103 \
    &> repslave1.log &

sleep 5

# setup the tables
cat $SCRIPT_DIR/setup.hql | $HT_HOME/bin/ht shell --namespace / --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg
if [ $? -ne "0" ];
then
  echo "Failed to create the tables"
  exit 1
fi

sleep 10

# run load generator and fill the database with key/value pairs of about
# 5 Megabyte in total
$HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec \
    --max-bytes=5M \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.Mutator.FlushDelay=250

# wait a bit till the replication finishes
sleep 30

# now compare both clusters
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
  exit 1
fi

exit 0
