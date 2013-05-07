#!/usr/bin/env bash
#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

usage() {
  echo ""
  echo "usage: start-replication.sh [OPTIONS] [<server-options>]"
  echo ""
  echo "OPTIONS:"
  echo "  --only-master only starts the Replication.Master"
  echo "  --only-slave  only starts the Replication.Slave"
  echo "  --valgrind    run replication servers with valgrind"
  echo "  --heapcheck   run replication servers with google-perf-tools Heapcheck"
  echo ""
}

START_MASTER="true"
START_SLAVE="true"

while [ "$1" != "${1##[-+]}" ]; do
  case $1 in
    --valgrind)
      VALGRIND="valgrind -v --log-file=vg.rs.%p --leak-check=full --num-callers=20 "
      shift
      ;;
    --heapcheck)
      HEAPCHECK="env HEAPCHECK=normal"
      shift
      ;;
    --heapprofile)
      HEAPCHECK="env HEAPPROFILE=/tmp/rs-$$.hprof"
      shift
      ;;
    --only-master)
      START_MASTER="true"
      START_SLAVE="false"
      shift
      ;;
    --only-slave)
      START_MASTER="false"
      START_SLAVE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

max_retries=200
report_interval=10
if [ $START_MASTER == "true" ] ; then
  start_server repmaster Replication.Master Replication.Master "$@"
fi
if [ $START_SLAVE == "true" ] ; then
  start_server repslave Replication.Slave Replication.Slave "$@"
fi
