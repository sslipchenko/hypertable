#!/usr/bin/env bash

TEST_DIR=`dirname $0`
BIN_DIR=$TEST_DIR/../bin
RET=0

function on_exit() {
  cd $TEST_DIR/cluster08 && cap stop
  cd $TEST_DIR/cluster12 && cap stop
  exit $RET
}

trap on_exit EXIT

#rm -rf /opt/hypertable/chris/current/log/*
cd $TEST_DIR/cluster12 && cap push_config
cd $TEST_DIR/cluster12 && cap cleandb
cd $TEST_DIR/cluster12 && cap start
cd $TEST_DIR/cluster08 && cap push_config
cd $TEST_DIR/cluster08 && cap cleandb
cd $TEST_DIR/cluster08 && cap start

# wait a few seconds
sleep 10

$HYPERTABLE_HOME/bin/ht hypertable --no-prompt < $TEST_DIR/create-table.hql

# now load lots of data
$HYPERTABLE_HOME/bin/ht ht_load_generator update --no-log-sync \
	--spec-file=$TEST_DIR/data.spec --max-bytes=1G \
	--table=Tabletest08
if [ $? != 0 ] ; then
  echo "Problem loading table 'Tabletest08', exiting ..."
  RET=1
  exit $RET
fi

# restart the replication services
cd $TEST_DIR/cluster08 && cap stop_replication
cd $TEST_DIR/cluster12 && cap stop_replication
cd $TEST_DIR/cluster08 && cap start_replication
cd $TEST_DIR/cluster12 && cap start_replication

# add more data
$HYPERTABLE_HOME/bin/ht ht_load_generator update --no-log-sync \
	--spec-file=$TEST_DIR/data.spec --max-bytes=1G \
	--table=Tabletest08
if [ $? != 0 ] ; then
  echo "Problem loading table 'Tabletest08', exiting ..."
  RET=1
  exit $RET
fi

# wait a few seconds
sleep 10

# dump table on both clusters, compare both dump files
echo 'SELECT * FROM Tabletest08 INTO FILE "dump08.txt";' | $HYPERTABLE_HOME/bin/ht hypertable --no-prompt --namespace /
ssh -t test12 'echo "SELECT * FROM Tabletest08 into file \"/tmp/dump12.txt\"; exit;" | /opt/hypertable/$USER/current/bin/ht shell --namespace /'

scp test12:/tmp/dump12.txt .

mv dump08.txt $TEST_DIR
mv dump12.txt $TEST_DIR

diff $TEST_DIR/dump08.txt $TEST_DIR/dump12.txt
if [ $? != 0 ] ; then
  echo "dumps are not identical"
  RET=1
  exit $RET
fi

echo "dumps are identical"
exit 0
