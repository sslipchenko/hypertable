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
cd $TEST_DIR/cluster12 && cap stop
cd $TEST_DIR/cluster12 && cap cleandb
cd $TEST_DIR/cluster12 && cap start
cd $TEST_DIR/cluster08 && cap push_config
cd $TEST_DIR/cluster08 && cap stop
cd $TEST_DIR/cluster08 && cap cleandb
cd $TEST_DIR/cluster08 && cap start

# wait a few seconds
sleep 15

echo "creating tables on test08"
$HYPERTABLE_HOME/bin/ht hypertable --no-prompt < $TEST_DIR/create-table-test08.hql
scp $TEST_DIR/create-table-test12.hql test12:/tmp
scp $TEST_DIR/data.spec test12:/tmp
echo "creating tables on test12"
ssh -t test12 "$HYPERTABLE_HOME/bin/ht shell --command-file=/tmp/create-table-test12.hql"

# now load lots of data
echo "inserting data on test08"
$HYPERTABLE_HOME/bin/ht ht_load_generator update --no-log-sync \
	--spec-file=$TEST_DIR/data.spec --max-bytes=1G \
	--table=Tabletest08 &
echo "inserting data on test12"
ssh -t test12 "$HYPERTABLE_HOME/bin/ht ht_load_generator update --no-log-sync \
	--spec-file=/tmp/data.spec --max-bytes=1G \
	--table=Tabletest12"
wait

# restart the replication services
cd $TEST_DIR/cluster08 && cap stop_replication
cd $TEST_DIR/cluster12 && cap stop_replication
cd $TEST_DIR/cluster08 && cap start_replication
cd $TEST_DIR/cluster12 && cap start_replication

# now load more data
echo "inserting data on test08"
$HYPERTABLE_HOME/bin/ht ht_load_generator update --no-log-sync \
	--spec-file=$TEST_DIR/data.spec --max-bytes=1G \
	--table=Tabletest08 &
echo "inserting data on test12"
ssh -t test12 "$HYPERTABLE_HOME/bin/ht ht_load_generator update --no-log-sync \
	--spec-file=/tmp/data.spec --max-bytes=1G \
	--table=Tabletest12"
wait

# again wait a few seconds
sleep 30

# dump Tabletest08 on both clusters, compare both dump files
echo 'SELECT * FROM Tabletest08 INTO FILE "Tabletest08-dump08.txt" KEYS_ONLY;' | $HYPERTABLE_HOME/bin/ht hypertable --no-prompt --namespace /
ssh -t test12 'echo "SELECT * FROM Tabletest08 into file \"/tmp/Tabletest08-dump12.txt\" KEYS_ONLY; exit;" | /opt/hypertable/$USER/current/bin/ht shell --namespace /'
scp test12:/tmp/Tabletest08-dump12.txt .
mv Tabletest08-dump08.txt $TEST_DIR
mv Tabletest08-dump12.txt $TEST_DIR
diff $TEST_DIR/Tabletest08-dump08.txt $TEST_DIR/Tabletest08-dump12.txt
if [ $? != 0 ] ; then
  echo "Tabletest08-dumps are not identical"
  RET=1
  exit $RET
fi

# dump Tabletest12 on both clusters, compare both dump files
echo 'SELECT * FROM Tabletest12 INTO FILE "Tabletest12-dump08.txt" KEYS_ONLY;' | $HYPERTABLE_HOME/bin/ht hypertable --no-prompt --namespace /
ssh -t test12 'echo "SELECT * FROM Tabletest12 into file \"/tmp/Tabletest12-dump12.txt\" KEYS_ONLY; exit;" | /opt/hypertable/$USER/current/bin/ht shell --namespace /'
scp test12:/tmp/Tabletest12-dump12.txt .
mv Tabletest12-dump08.txt $TEST_DIR
mv Tabletest12-dump12.txt $TEST_DIR
diff $TEST_DIR/Tabletest12-dump08.txt $TEST_DIR/Tabletest12-dump12.txt
if [ $? != 0 ] ; then
  echo "Tabletest12-dumps are not identical"
  RET=1
  exit $RET
fi

echo "dumps are identical"
exit 0
