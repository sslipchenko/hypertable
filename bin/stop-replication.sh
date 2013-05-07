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
  echo "usage: stop-replication.sh [OPTIONS] [<server-options>]"
  echo ""
  echo "  --only-master only stops the Replication.Master"
  echo "  --only-slave  only stops the Replication.Slave"
  echo ""
}

STOP_MASTER="true"
STOP_SLAVE="true"

while [ "$1" != "${1##[-+]}" ]; do
  case $1 in
    --only-master)
      STOP_MASTER="true"
      STOP_SLAVE="false"
      shift
      ;;
    --only-slave)
      STOP_MASTER="false"
      STOP_SLAVE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

if [ $STOP_MASTER == "true" ] ; then
  stop_server repmaster Replication.Master
fi
if [ $STOP_SLAVE == "true" ] ; then
  stop_server repslave Replication.Slave
fi

wait
