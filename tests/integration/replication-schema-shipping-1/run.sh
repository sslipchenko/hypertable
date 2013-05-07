#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
ABC_HOME=$INSTALL_DIR
SCRIPT_DIR=`dirname $0`
# reset INSTALL_DIR or start-test-servers.sh will fail
INSTALL_DIR=

function on_exit() {
  killall Replication.Master
}
trap on_exit EXIT

# start one cluster with two replication masters
$HT_HOME/bin/ht start-test-servers.sh --clear --no-thriftbroker \
    --config=$SCRIPT_DIR/hypertable.cfg

$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    --Hypertable.Replication.Master.Port=38101 \
    --Hypertable.Replication.TestMode=true \
    --Hypertable.Replication.BaseNamespace=/rep &> repmaster2.log &
sleep 2
$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    &> repmaster1.log &

sleep 5

function run_test() {
  ID=$1
  echo "Running test $1"
  cat $SCRIPT_DIR/$ID.hql | $HT_HOME/bin/ht shell --namespace / --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg &> $ID.out0
  sleep 5 
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
run_test test2
run_test test3
run_test test4

exit 0
