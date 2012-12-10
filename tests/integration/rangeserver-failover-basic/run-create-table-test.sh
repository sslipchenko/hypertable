#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current/"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
MAX_KEYS=${MAX_KEYS:-"500000"}
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid
RS3_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs3.pid
MASTER_PIDFILE=$HT_HOME/run/Hypertable.Master.pid
MASTER_LOG=$HT_HOME/log/Hypertable.Master.log
RUN_DIR=`pwd`

. $HT_HOME/bin/ht-env.sh

# get rid of all old logfiles
\rm -rf $HT_HOME/log/*

INDUCER_ARG=
let j=1
[ $TEST == $j ] && INDUCER_ARG="--induce-failure=user-load-range-4:exit:0"
let j+=1
[ $TEST == $j ] && INDUCER_ARG="--induce-failure=add-staged-range-2:exit:0"

wait_for_recovery() {
  grep "Leaving RecoverServer $1 state=COMPLETE" $MASTER_LOG
  while [ $? -ne "0" ]
  do
    sleep 2
    grep "Leaving RecoverServer $1 state=COMPLETE" $MASTER_LOG
  done
}

stop_rs2() {
  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs2.pid`
}

stop_rs3() {
  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs3.pid`
}

# stop and start servers
rm metadata.* keys.* rs*dump.* 
rm -rf fs fs_pre
$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
    --clear --config=${SCRIPT_DIR}/test.cfg

# start the rangeservers
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=38060 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs1.output&
sleep 10
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 $INDUCER_ARG \
   --Hypertable.RangeServer.Port=38061 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs2.output&
sleep 3
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS3_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs3 \
   --Hypertable.RangeServer.Port=38062 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs3.output&
sleep 10

# create table
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

# verify recovery ocurred
wait_for_recovery rs2

# write data 
$HT_HOME/bin/ht ht_load_generator update \
    --rowkey.component.0.order=ascending \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%020lld" \
    --row-seed=1 \
    --Field.value.size=20 \
    --max-keys=$MAX_KEYS

# dump keys
${HT_HOME}/bin/ht shell --config=${SCRIPT_DIR}/test.cfg --no-prompt --exec "use '/'; select * from LoadTest KEYS_ONLY into file '${RUN_DIR}/keys';"

# stop servers
$HT_HOME/bin/stop-servers.sh
stop_rs2
#stop_rs3

# check output
TOTAL_KEYS=`cat keys|tail -n +2|wc -l`
echo "Total keys returned=${TOTAL_KEYS}, expected keys=${MAX_KEYS}"

if [ "$TOTAL_KEYS" -ne "$MAX_KEYS" ]
then
  echo "Test failed, expected ${MAX_KEYS}, retrieved ${TOTAL_KEYS}"
  exit 1
fi

echo "Test passed"

exit 0
