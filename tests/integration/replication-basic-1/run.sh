#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
ABC_HOME=$INSTALL_DIR
SCRIPT_DIR=`dirname $0`
# reset INSTALL_DIR or start-test-servers.sh will fail
INSTALL_DIR=

# start one cluster with two replication masters
$HT_HOME/bin/ht start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.CommitLog.FragmentRemoval.Disable=true \
    --config=$SCRIPT_DIR/hypertable.cfg

function on_exit() {
  killall Replication.Master
  killall Replication.Slave
}
trap on_exit EXIT

$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Master.Port=38101 \
    --Hypertable.Replication.TestMode=true \
    --Hypertable.Replication.BaseNamespace=/rep &> repmaster2.log &
$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    &> repmaster1.log &

sleep 3

$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.Port=38102 \
    --Hypertable.Replication.Slave.ProxyName=repslave2 \
    --Hypertable.Replication.Slave.MasterAddress=localhost:38101 \
    --Hypertable.Replication.BaseNamespace=/rep &> repslave2.log &

$ABC_HOME/bin/Replication.Slave --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Slave.Port=38103 \
    --Hypertable.Replication.Slave.ProxyName=repslave1 \
    &> repslave1.log &

sleep 5

function run_test() {
  ID=$1
  echo "Running test $1"
  cat $SCRIPT_DIR/$ID.hql | $HT_HOME/bin/ht shell --namespace / --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg &> $ID.out0
  sleep 30 
  cat $SCRIPT_DIR/show.hql | $HT_HOME/bin/ht shell --namespace / --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg &> $ID.out

  diff $ID.out $SCRIPT_DIR/$ID.golden
  if [ $? -ne "0" ];
  then
    echo "test $ID differs"
    exit 1
  fi
}

run_test test1

exit 0
