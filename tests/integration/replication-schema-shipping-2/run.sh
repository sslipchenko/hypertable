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
    --induce-failure=Replication.Master-Apply-Schema-1:throw:0 \
    --Hypertable.Replication.TestMode=true \
    --Hypertable.Replication.BaseNamespace=/rep &> repmaster2.log &
sleep 2
$ABC_HOME/bin/Replication.Master --config=$SCRIPT_DIR/hypertable.cfg \
    &> repmaster1.log &

sleep 5

cat $SCRIPT_DIR/test1.hql | $HT_HOME/bin/ht shell --namespace / --test-mode \
    --config=$SCRIPT_DIR/hypertable.cfg &> test1.out

grep "HYPERTABLE induced failure - Replication.Master notify_schema_update() failure: Replication.Master apply_schema_update() failure: induced failure code '45' 'Replication.Master-Apply-Schema-1'" $HT_HOME/log/Hypertable.Master.log
if [ $? -ne "0" ];
then
  echo "Did not find error message in Master's log"
  exit 1
fi

exit 0
