#!/usr/bin/env bash

HT_HOME=$INSTALL_DIR
SCRIPT_DIR=`dirname $0`

killall Replication.Slave
killall Replication.Master

function check_master_is_running() {
  $HT_HOME/bin/ht serverup Replication.Master
  if [ $? -ne "0" ];
  then
    echo "Replication.Master is not running, but it should"
    exit 1
  fi
}

function check_master_is_not_running() {
  $HT_HOME/bin/ht serverup Replication.Master
  if [ $? -eq "0" ];
  then
    echo "Replication.Master is running, but it shouldn't"
    exit 1
  fi
}

function check_slave_is_running() {
  $HT_HOME/bin/ht serverup Replication.Slave
  if [ $? -ne "0" ];
  then
    echo "Replication.Slave is not running, but it should"
    exit 1
  fi
}

function check_slave_is_not_running() {
  $HT_HOME/bin/ht serverup Replication.Slave
  if [ $? -eq "0" ];
  then
    echo "Replication.Slave is running, but it shouldn't"
    exit 1
  fi
}

CFG="--config=$SCRIPT_DIR/hypertable.cfg"

$HT_HOME/bin/ht start-test-servers.sh --clear --no-thriftbroker

$HT_HOME/bin/ht stop-replication.sh $CFG
check_master_is_not_running
check_slave_is_not_running

$HT_HOME/bin/ht start-replication.sh $CFG
check_master_is_running
check_slave_is_running

$HT_HOME/bin/ht stop-replication.sh --only-master $CFG
check_master_is_not_running
check_slave_is_running

$HT_HOME/bin/ht start-replication.sh --only-master $CFG
check_master_is_running
check_slave_is_running

$HT_HOME/bin/ht stop-replication.sh --only-slave $CFG
check_master_is_running
check_slave_is_not_running

$HT_HOME/bin/ht start-replication.sh --only-slave $CFG
check_master_is_running
check_slave_is_running

$HT_HOME/bin/ht stop-replication.sh $CFG
check_master_is_not_running
check_slave_is_not_running


exit 0
