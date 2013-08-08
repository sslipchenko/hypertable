#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
MAX_KEYS=${MAX_KEYS:-"500000"}
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid
RS3_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs3.pid
RS4_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs4.pid
RS5_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs5.pid
RUN_DIR=`pwd`

. $HT_HOME/bin/ht-env.sh

. $SCRIPT_DIR/utilities.sh

kill_all_rs
$HT_HOME/bin/stop-servers.sh

# get rid of all old logfiles
\rm -rf $HT_HOME/log/*
rm metadata.* dbdump-* rs*dump.* 
rm -rf fs fs_pre

# generate golden output file
gen_test_data

# stop and start servers
$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
    --clear --config=${SCRIPT_DIR}/test.cfg

# start the rangeservers
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=38060 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs1.output&
wait_for_server_connect
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=38061 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs2.output&
sleep 2
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS3_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs3 \
   --Hypertable.RangeServer.Port=38062 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs3.output&
sleep 2
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS4_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs4 \
   --Hypertable.RangeServer.Port=38063 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs4.output&
sleep 2
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS5_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs5 \
   --Hypertable.RangeServer.Port=38064 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs5.output&

# create table
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

# write data 
$HT_HOME/bin/ht load_generator --spec-file=$SCRIPT_DIR/data.spec \
    --max-keys=$MAX_KEYS --row-seed=$ROW_SEED --table=LoadTest \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=2M \
    --Hypertable.Mutator.FlushDelay=250 update
if [ $? != 0 ] ; then
    echo "Problem loading table 'LoadTest', exiting ..."
    save_failure_state
    kill_rs 1 2 3 4 5
    $HT_HOME/bin/stop-servers.sh
    exit 1
fi

sleep 2

# kill rs1 and rs2
stop_rs 1
kill_rs 2

# wait for recovery to complete 
wait_for_recovery rs1
wait_for_recovery rs2

# dump keys again
dump_keys dbdump-a.3

# bounce servers
$HT_HOME/bin/stop-servers.sh
kill_rs 3
kill_rs 4
kill_rs 5

# start master and rs3, rs4, rs5
$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
    --config=${SCRIPT_DIR}/test.cfg
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS3_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs3 \
   --Hypertable.RangeServer.Port=38062 --config=${SCRIPT_DIR}/test.cfg 2>&1 >> rangeserver.rs3.output&
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS4_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs4 \
   --Hypertable.RangeServer.Port=38063 --config=${SCRIPT_DIR}/test.cfg 2>&1 >> rangeserver.rs4.output&
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS5_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs5 \
   --Hypertable.RangeServer.Port=38064 --config=${SCRIPT_DIR}/test.cfg 2>&1 >> rangeserver.rs5.output&

# dump keys
dump_keys dbdump-b.3

# stop servers
$HT_HOME/bin/stop-servers.sh
kill_rs 3
kill_rs 4
kill_rs 5

echo "Test passed"

exit 0
