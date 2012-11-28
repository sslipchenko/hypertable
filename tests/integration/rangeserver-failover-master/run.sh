#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
DATA_SEED=42
DATA_SIZE=${DATA_SIZE:-"2000000"}
DIGEST="openssl dgst -md5"
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid
RS3_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs3.pid
RUN_DIR=`pwd`


. $HT_HOME/bin/ht-env.sh

# get rid of all old logfiles
\rm -rf $HT_HOME/log/*

# Dumping cores slows things down unnecessarily for normal test runs
#ulimit -c 0

wait_for_recovery() {
  local id=$1
  local n=0
  local s="Leaving RecoverServer $id state=COMPLETE"
  egrep "$s" master.output.$TEST
  while [ $? -ne "0" ]
  do
    (( n += 1 ))
    if [ "$n" -gt "300" ]; then
      echo "wait_for_recovery: time exceeded"
      exit 1
    fi
    sleep 2
    egrep "$s" master.output.$TEST
  done
}

gen_test_data() {
    if [ ! -s golden_dump.md5 ] ; then
        $HT_HOME/bin/ht load_generator --spec-file=$SCRIPT_DIR/data.spec \
            --max-keys=$DATA_SIZE --row-seed=$DATA_SEED --table=FailoverTest \
            --stdout update | cut -f1 | tail -n +2 | sort -u > golden_dump.txt
        $DIGEST < golden_dump.txt > golden_dump.md5
    fi
}

stop_range_servers() {
    local port
    let port=38059+$1
    while [ $port -ge 38060 ] ; do
        echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:$port
        let port-=1
    done
    sleep 1
    kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs?.pid`
}

stop_rs() {
    local port
    let port=38059+$1
    echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:$port
    kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs$1.pid`
}

kill_rs() {
    kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs$1.pid`
}


# Runs an individual test with two RangeServers; the master goes down
# during recovery
run_test() {
    local MASTER_INDUCED_FAILURE=$1
    local i port WAIT_ARGS INDUCED_FAILURE PIDFILE PORT
    shift
    let i=1
    while [ $# -gt 0 ] ; do
        INDUCED_FAILURE[$i]=$1
        PIDFILE[$i]=$HT_HOME/run/Hypertable.RangeServer.rs$i.pid
        let port=38059+$i
        PORT[$i]=$port
        let i+=1
        shift
    done
    let RS_COUNT=i-1

    echo "Running test $TEST." >> report.txt
    let i=1
    while [ $i -le $RS_COUNT ] ; do
        echo "$i: ${INDUCED_FAILURE[$i]}" >> report.txt
        let i+=1
    done

    stop_range_servers $RS_COUNT

    $HT_HOME/bin/start-test-servers.sh --no-master --no-rangeserver \
        --no-thriftbroker --clear --DfsBroker.DisableFileRemoval=true

    # start master-launcher script in background. it will restart the
    # master as soon as it crashes
    local INDUCER_ARG MASTER_EXIT
    if test -n $MASTER_INDUCED_FAILURE ; then
        INDUCER_ARG="--induce-failure=$MASTER_INDUCED_FAILURE"
    fi        
    echo $MASTER_INDUCED_FAILURE | grep ":exit:" > /dev/null
    if [ $? == 0 ] ; then
        MASTER_EXIT=true
        $SCRIPT_DIR/master-launcher.sh $INDUCER_ARG > master.output.$TEST 2>&1 &
    else
        $HT_HOME/bin/Hypertable.Master --verbose \
            --pidfile=$HT_HOME/run/Hypertable.Master.pid \
            --config=${SCRIPT_DIR}/test.cfg \
            $INDUCER_ARG > master.output.$TEST 2>&1 &
    fi
    sleep 10

    local j
    let j=1
    while [ $j -le $RS_COUNT ] ; do
        INDUCER_ARG=
        if test -n ${INDUCED_FAILURE[$j]} ; then
            INDUCER_ARG=--induce-failure=${INDUCED_FAILURE[$j]}
        fi
        $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=${PIDFILE[$j]} \
            --Hypertable.RangeServer.ProxyName=rs$j \
            --Hypertable.RangeServer.Port=${PORT[$j]} ${INDUCED_FAILURE[$j]} \
            --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs$j.output.$TEST &
        if [ $j -eq 1 ] ; then
            sleep 5
        fi
        let j+=1
    done

    $HT_SHELL --batch < $SCRIPT_DIR/create-test-table.hql
    if [ $? != 0 ] ; then
        echo "Unable to create table 'failover-test', exiting ..."
        exit 1
    fi

    $HT_HOME/bin/ht load_generator --spec-file=$SCRIPT_DIR/data.spec \
        --max-keys=$DATA_SIZE --row-seed=$DATA_SEED --table=FailoverTest \
        --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=100K \
        update
    if [ $? != 0 ] ; then
        echo "Problem loading table 'FailoverTest', exiting ..."
        exit 1
    fi
    
    sleep 10
    $HT_SHELL --namespace 'sys' --batch --exec \
        'select Location from METADATA revs=1;' > locations.$TEST

  # kill rs1
    stop_rs 1
    
  # wait for recovery to complete 
    wait_for_recovery "rs1"
    let j=2
    while [ $j -le $RS_COUNT ] ; do
        if test -n "${INDUCED_FAILURE[$j]}" ; then
            wait_for_recovery "rs$j"          
        fi
        let j+=1
    done

    if test -n "$MASTER_EXIT" ; then
        fgrep "CRASH" master.output.$TEST
        if [ $? != 0 ] ; then
            echo "ERROR: Failure was not induced in Master."
            exit 1
        fi
    fi
    
  # dump keys
    $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
        | grep -v "hypertable" > dbdump.$TEST
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'failover-test', exiting ..."
        exit 1
    fi
    
    $DIGEST < dbdump.$TEST > dbdump.md5
    diff golden_dump.md5 dbdump.md5 > out
    if [ $? != 0 ] ; then
        echo "Test $TEST FAILED." >> report.txt
        echo "Test $TEST FAILED." >> errors.txt
        cat out >> report.txt
        touch error
        $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
            | grep -v "hypertable" > dbdump.$TEST.again
        exec 1>&-
        sleep 86400
    else
        echo "Test $TEST PASSED." >> report.txt
    fi

    # shut everything down
    kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.*.pid`  
    $HT_HOME/bin/stop-servers.sh

}

# Runs an individual test with three RangeServers; one additional range server
# goes down during recovery, often in combination with a simulated
# crash in the master
run_test2() {
  local TEST_ID=$1
  local MS_IND=$2;
  local RS_IND=$3;
  
  echo "Running test $TEST_ID." >> report.txt

  stop_range_servers

  $HT_HOME/bin/start-test-servers.sh --no-master --no-rangeserver \
        --no-thriftbroker --clear

  # start master-launcher script in background. it will restart the
  # master as soon as it crashes
  $SCRIPT_DIR/master-launcher.sh $MS_IND > master.output.$TEST_ID 2>&1 &
  sleep 10

  # give rangeserver time to get registered etc 
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
     --Hypertable.RangeServer.ProxyName=rs1 \
     --Hypertable.RangeServer.Port=38060 \
     --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs1.output.$TEST_ID &
  sleep 5
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
    --Hypertable.RangeServer.ProxyName=rs2 \
    --Hypertable.RangeServer.Port=38061 $RS_IND \
    --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs2.output.$TEST_ID &
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS3_PIDFILE \
    --Hypertable.RangeServer.ProxyName=rs3 \
    --Hypertable.RangeServer.Port=38062 \
    --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs3.output.$TEST_ID &

  $HT_SHELL --batch < $SCRIPT_DIR/create-test-table.hql
  if [ $? != 0 ] ; then
    echo "Unable to create table 'failover-test', exiting ..."
    exit 1
  fi

  $HT_SHELL --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=100K \
      --batch < $SCRIPT_DIR/load.hql
  if [ $? != 0 ] ; then
    echo "Problem loading table 'failover-test', exiting ..."
    exit 1
  fi
  
  # wait a bit for splits 
  $HT_HOME/bin/ht_rsclient --exec "COMPACT RANGES USER; WAIT FOR MAINTENANCE;"
  sleep 10
  $HT_SHELL --namespace 'sys' --batch --exec \
     'select Location from METADATA revs=1;' > locations.$TEST_ID  
  
  # kill rs1; rs2 will be killed by the error inducer
  stop_rs1
  
  # wait for recovery to complete 
  wait_for_recovery "rs1"
  wait_for_recovery "rs2"

  fgrep "CRASH" master.output.$TEST_ID
  if [ $? != 0 ] ; then
    echo "ERROR: Failure was not induced in Master."
    exit 1
  fi

  fgrep "induced failure" rangeserver.rs2.output.$TEST_ID
  if [ $? != 0 ] ; then
    echo "ERROR: Failure was not induced in RangeServer."
    exit 1
  fi
  
  # dump keys
  $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
      | grep -v "hypertable" > dbdump.$TEST_ID
  if [ $? != 0 ] ; then
    echo "Problem dumping table 'failover-test', exiting ..."
    exit 1
  fi
  
  # shut everything down
  stop_rs2
  stop_rs3
  $HT_HOME/bin/stop-servers.sh

  $DIGEST < dbdump.$TEST_ID > dbdump.md5
  diff data.md5 dbdump.md5 > out
  if [ $? != 0 ] ; then
    echo "Test $TEST_ID FAILED." >> report.txt
    echo "Test $TEST_ID FAILED." >> errors.txt
    cat out >> report.txt
    touch error
    $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
        | grep -v "hypertable" > dbdump.$TEST_ID.again
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'failover-test', exiting ..."
        exit 1
    fi
  else
    echo "Test $TEST_ID PASSED." >> report.txt
  fi
}

# Runs an individual test with three RangeServers, and during recovery
# one of them goes down with an error-inducer code. The master stays 
# alive during the whole process
run_test3() {
  local TEST_ID=$1
  local RS_IND=$2;
  
  echo "Running test $TEST_ID." >> report.txt

  stop_range_servers

  $HT_HOME/bin/start-test-servers.sh --no-master --no-rangeserver \
        --no-thriftbroker --clear

  # start master-launcher script in background. it will restart the
  # master as soon as it crashes
  $SCRIPT_DIR/master-launcher.sh > master.output.$TEST_ID 2>&1 &
  sleep 10

  # give rangeserver time to get registered etc 
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
     --Hypertable.RangeServer.ProxyName=rs1 \
     --Hypertable.RangeServer.Port=38060 \
     --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs1.output.$TEST_ID &
  sleep 5
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
    --Hypertable.RangeServer.ProxyName=rs2 \
    --Hypertable.RangeServer.Port=38061 $RS_IND \
    --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs2.output.$TEST_ID &
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS3_PIDFILE \
    --Hypertable.RangeServer.ProxyName=rs3 \
    --Hypertable.RangeServer.Port=38062 \
    --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs3.output.$TEST_ID &

  $HT_SHELL --batch < $SCRIPT_DIR/create-test-table.hql
  if [ $? != 0 ] ; then
    echo "Unable to create table 'failover-test', exiting ..."
    exit 1
  fi

  $HT_SHELL --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=100K \
      --batch < $SCRIPT_DIR/load.hql
  if [ $? != 0 ] ; then
    echo "Problem loading table 'failover-test', exiting ..."
    exit 1
  fi
  
  # wait a bit for splits 
  $HT_HOME/bin/ht_rsclient --exec "COMPACT RANGES USER; WAIT FOR MAINTENANCE;"
  sleep 10
  $HT_SHELL --namespace 'sys' --batch --exec \
     'select Location from METADATA revs=1;' > locations.$TEST_ID  
  
  # kill rs1; rs2 will be killed by the error inducer
  stop_rs1
  
  # wait for recovery to complete 
  wait_for_recovery "rs1"
  wait_for_recovery "rs2"
  
  # dump keys
  $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
      | grep -v "hypertable" > dbdump.$TEST_ID
  if [ $? != 0 ] ; then
    echo "Problem dumping table 'failover-test', exiting ..."
    exit 1
  fi
  
  # shut everything down
  stop_rs2
  stop_rs3
  $HT_HOME/bin/stop-servers.sh

  $DIGEST < dbdump.$TEST_ID > dbdump.md5
  diff data.md5 dbdump.md5 > out
  if [ $? != 0 ] ; then
    echo "Test $TEST_ID FAILED." >> report.txt
    echo "Test $TEST_ID FAILED." >> errors.txt
    cat out >> report.txt
    touch error
    $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
        | grep -v "hypertable" > dbdump.$TEST_ID.again
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'failover-test', exiting ..."
        exit 1
    fi
  else
    echo "Test $TEST_ID PASSED." >> report.txt
  fi
}

# Runs an individual test with three RangeServers, and during recovery
# the master crashes. Before the master comes up again, the second range server
# will go down and force the master to re-create the recovery plan.
run_test4() {
  local TEST_ID=$1
  local MS_IND=$2;
  
  echo "Running test $TEST_ID." >> report.txt

  stop_range_servers

  $HT_HOME/bin/start-test-servers.sh --no-master --no-rangeserver \
        --no-thriftbroker --clear

  # start master-launcher script in background. it will restart the
  # master as soon as it crashes
  $SCRIPT_DIR/master-launcher.sh $MS_IND $RS2_PIDFILE \
      > master.output.$TEST_ID 2>&1 &
  sleep 10

  # give rangeserver time to get registered etc 
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
     --Hypertable.RangeServer.ProxyName=rs1 \
     --Hypertable.RangeServer.Port=38060 \
     --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs1.output.$TEST_ID &
  sleep 5
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
    --Hypertable.RangeServer.ProxyName=rs2 \
    --Hypertable.RangeServer.Port=38061 \
    --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs2.output.$TEST_ID &
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS3_PIDFILE \
    --Hypertable.RangeServer.ProxyName=rs3 \
    --Hypertable.RangeServer.Port=38062 \
    --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs3.output.$TEST_ID &

  $HT_SHELL --batch < $SCRIPT_DIR/create-test-table.hql
  if [ $? != 0 ] ; then
    echo "Unable to create table 'failover-test', exiting ..."
    exit 1
  fi

  $HT_SHELL --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=100K \
      --batch < $SCRIPT_DIR/load.hql
  if [ $? != 0 ] ; then
    echo "Problem loading table 'failover-test', exiting ..."
    exit 1
  fi
  
  # wait a bit for splits 
  $HT_HOME/bin/ht_rsclient --exec "COMPACT RANGES USER; WAIT FOR MAINTENANCE;"
  sleep 10
  $HT_SHELL --namespace 'sys' --batch --exec \
     'select Location from METADATA revs=1;' > locations.$TEST_ID  
  
  # kill rs1; rs2 will be killed by the error inducer
  stop_rs1
  
  # wait for recovery to complete 
  wait_for_recovery "rs1"
  wait_for_recovery "rs2"
  
  # dump keys
  $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
      | grep -v "hypertable" > dbdump.$TEST_ID
  if [ $? != 0 ] ; then
    echo "Problem dumping table 'failover-test', exiting ..."
    exit 1
  fi
  
  # shut everything down
  stop_rs2
  stop_rs3
  $HT_HOME/bin/stop-servers.sh

  $DIGEST < dbdump.$TEST_ID > dbdump.md5
  diff data.md5 dbdump.md5 > out
  if [ $? != 0 ] ; then
    echo "Test $TEST_ID FAILED." >> report.txt
    echo "Test $TEST_ID FAILED." >> errors.txt
    cat out >> report.txt
    touch error
    $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
        | grep -v "hypertable" > dbdump.$TEST_ID.again
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'failover-test', exiting ..."
        exit 1
    fi
  else
    echo "Test $TEST_ID PASSED." >> report.txt
  fi
}

if [ "$TEST_0" ] || [ "$TEST_1" ] ; then
    rm -f errors.txt
fi

rm -f report.txt

gen_test_data

let j=1
[ $TEST == $j ] && run_test "recover-server-ranges-root-initial-1:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-root-initial-2:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-root-load-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-root-replay-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-root-prepare-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-root-commit-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-root-ack-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-metadata-load-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-metadata-replay-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-metadata-prepare-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-metadata-commit-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-metadata-ack-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-load-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-replay-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-prepare-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-commit-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-ack-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-1:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-2:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-3:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-4:exit:0" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-phantom-load-ranges:throw:0" "" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-replay-fragments:throw:0" "" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-phantom-prepare-ranges:throw:0" "" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-phantom-commit-ranges:throw:0" "" "" ""
let j+=1
[ $TEST == $j ] && run_test "recover-server-ranges-user-acknowledge-load:throw:0" "" "" ""

[ "$TEST_24" ] && run_test1 24 "--induce-failure=recover-server-ranges-root-replay-commit-log:throw"
[ "$TEST_25" ] && run_test2 25 "--induce-failure=recover-server-ranges-user-load-2:exit:0" "--induce-failure=phantom-load-user-1:exit:0"
[ "$TEST_26" ] && run_test2 26 "--induce-failure=recover-server-ranges-user-load-2:exit:0" "--induce-failure=phantom-load-user-2:exit:0"
[ "$TEST_27" ] && run_test2 27 "--induce-failure=recover-server-ranges-user-2:exit:0" "--induce-failure=replay-fragments-user-1:exit:0"
[ "$TEST_28" ] && run_test2 28 "--induce-failure=recover-server-ranges-user-prepare-2:exit:0" "--induce-failure=phantom-prepare-ranges-user-1:exit:0"
[ "$TEST_29" ] && run_test2 29 "--induce-failure=recover-server-ranges-user-prepare-2:exit:0" "--induce-failure=phantom-prepare-ranges-user-2:exit:0"
[ "$TEST_30" ] && run_test2 30 "--induce-failure=recover-server-ranges-user-prepare-2:exit:0" "--induce-failure=phantom-prepare-ranges-user-3:exit:0"
[ "$TEST_31" ] && run_test2 31 "--induce-failure=recover-server-ranges-user-commit-2:exit:0" "--induce-failure=phantom-commit-user-1:exit:0"
[ "$TEST_32" ] && run_test2 32 "--induce-failure=recover-server-ranges-user-commit-2:exit:0" "--induce-failure=phantom-commit-user-2:exit:0"
[ "$TEST_33" ] && run_test2 33 "--induce-failure=recover-server-ranges-user-commit-2:exit:0" "--induce-failure=phantom-commit-user-3:exit:0"
[ "$TEST_34" ] && run_test2 34 "--induce-failure=recover-server-ranges-user-commit-2:exit:0" "--induce-failure=phantom-commit-user-4:exit:0"

[ "$TEST_47" ] && run_test2 47 "--induce-failure=recover-server-user-3:exit:0" "--induce-failure=phantom-receive-1:exit:0"
[ "$TEST_48" ] && run_test2 48 "--induce-failure=recover-server-user-4:exit:0" "--induce-failure=phantom-receive-1:exit:0"
[ "$TEST_49" ] && run_test2 49 "--induce-failure=recover-server-user-5:exit:0" "--induce-failure=phantom-receive-1:exit:0"
[ "$TEST_50" ] && run_test2 50 "--induce-failure=recover-server-ranges-user-3.1:exit:0" "--induce-failure=phantom-receive-1:exit:0"
[ "$TEST_51" ] && run_test2 51 "--induce-failure=recover-server-ranges-user-10:exit:0" "--induce-failure=phantom-prepare-ranges-4:exit:0"

[ "$TEST_52" ] && run_test2 52 "--induce-failure=recover-server-ranges-user-3.2:exit:0" "--induce-failure=replay-fragments-1:exit:0"
[ "$TEST_53" ] && run_test2 53 "--induce-failure=recover-server-ranges-user-3.2:exit:0" "--induce-failure=phantom-receive-1:exit:0"
[ "$TEST_54" ] && run_test2 54 "--induce-failure=recover-server-ranges-user-3.2:exit:0" "--induce-failure=phantom-receive-2:exit:0"
[ "$TEST_55" ] && run_test2 55 "--induce-failure=recover-server-ranges-user-3.2:exit:0" "--induce-failure=phantom-receive-1:exit:0"


#[ "$TEST_30" ] && run_test3 30 "--induce-failure=replay-fragments-1:exit:0"
#[ "$TEST_31" ] && run_test3 31 "--induce-failure=replay-fragments-2:exit:0"
#[ "$TEST_32" ] && run_test3 32 "--induce-failure=phantom-load-1:exit:0"
#[ "$TEST_33" ] && run_test3 33 "--induce-failure=phantom-prepare-ranges-1:exit:0"
#[ "$TEST_34" ] && run_test3 34 "--induce-failure=phantom-prepare-ranges-2:exit:0"
[ "$TEST_35" ] && run_test3 35 "--induce-failure=phantom-prepare-ranges-3:exit:0"
[ "$TEST_36" ] && run_test3 36 "--induce-failure=phantom-prepare-ranges-4:exit:0"
[ "$TEST_37" ] && run_test3 37 "--induce-failure=phantom-commit-1:exit:0"
[ "$TEST_38" ] && run_test3 38 "--induce-failure=phantom-commit-2:exit:0"
[ "$TEST_39" ] && run_test3 39 "--induce-failure=phantom-commit-3:exit:0"
[ "$TEST_40" ] && run_test3 40 "--induce-failure=phantom-commit-4:exit:0"
[ "$TEST_41" ] && run_test3 41 "--induce-failure=phantom-commit-5:exit:0"
[ "$TEST_42" ] && run_test3 42 "--induce-failure=phantom-commit-6:exit:0"

[ "$TEST_43" ] && run_test4 43 "--induce-failure=recover-server-ranges-root-4.1:exit:0"
[ "$TEST_44" ] && run_test4 44 "--induce-failure=recover-server-ranges-user-4.1:exit:0"
[ "$TEST_45" ] && run_test4 45 "--induce-failure=recover-server-ranges-user-7:exit:0"
[ "$TEST_46" ] && run_test4 46 "--induce-failure=recover-server-ranges-user-10:exit:0"

[ "$TEST_56" ] && run_test1 56 "--induce-failure=recover-server-ranges-root-4.2:exit:0"
[ "$TEST_57" ] && run_test4 57 "--induce-failure=recover-server-ranges-root-4.2:exit:0"
[ "$TEST_58" ] && run_test4 58 "--induce-failure=recover-server-ranges-user-4.2:exit:0"
[ "$TEST_59" ] && run_test1 59 "--induce-failure=recover-server-ranges-user-4.2:exit:0"
[ "$TEST_60" ] && run_test1 60 "--induce-failure=recover-server-ranges-metadata-4.2:exit:0"

if [ -e errors.txt ] && [ "$TEST_21" ] ; then
    ARCHIVE_DIR="archive-"`date | sed 's/ /-/g'`
    mkdir $ARCHIVE_DIR
    mv core.* dbdump.* rangeserver.output.* errors.txt $ARCHIVE_DIR
fi

echo ""
echo "**** TEST REPORT ****"
echo ""
cat report.txt
grep FAILED report.txt > /dev/null && exit 1
exit 0
