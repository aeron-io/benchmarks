#!/usr/bin/env bash
##
## Copyright 2015-2025 Real Logic Limited.
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
## https://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##

set -euxo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
AERON_SCRIPT_HOME=${DIR}/../../aeron

function usage() {
  echo "$0 [-t echo|failover]" 1>&2
  exit 2
}

TYPE=echo
while getopts ":t:" opt; do
  case "${opt}" in
    t)
      TYPE=$OPTARG
      ;;
    *)
      usage
      ;;
  esac
done
shift $((OPTIND -1))

case "$TYPE" in
  echo)
    TYPE_CONFIG=""
    TYPE_CLIENT="cluster-client"
    ;;
  failover)
    TYPE_CONFIG="${DIR}/failover.properties"
    TYPE_CLIENT="failover-cluster-client"
    ;;
  *)
    echo "Unknown type $TYPE"
    exit 2
    ;;
esac

function startNode() {
  node=$1
  JVM_OPTS="-Xms16M"
# Useful for logging (no javaagent needed since Aeron 1.53.0)...
#  JVM_OPTS="${JVM_OPTS} -Daeron.event.cluster.log=all -Daeron.event.cluster.log.disable=APPEND_POSITION,COMMIT_POSITION"
#  JVM_OPTS="${JVM_OPTS} -Daeron.event.log.filename=log_${node}.log"
  export JVM_OPTS

  ${AERON_SCRIPT_HOME}/cluster-node ${DIR}/cluster.properties $TYPE_CONFIG "${DIR}/node${node}.properties" > "node${node}.out" &
}

JVM_OPTS="-Xms16M"
echo "Starting media drivers"
${AERON_SCRIPT_HOME}/media-driver ${DIR}/cluster.properties ${DIR}/node0.properties > md_node0.out &
${AERON_SCRIPT_HOME}/media-driver ${DIR}/cluster.properties ${DIR}/node1.properties > md_node1.out &
${AERON_SCRIPT_HOME}/media-driver ${DIR}/cluster.properties ${DIR}/node2.properties > md_node2.out &
${AERON_SCRIPT_HOME}/media-driver ${DIR}/cluster.properties ${DIR}/client.properties > md_client.out &

echo "Cluster nodes"
startNode 0
startNode 1
startNode 2

echo "Start client"
export JVM_OPTS="-Xms16M"
${AERON_SCRIPT_HOME}/${TYPE_CLIENT} ${DIR}/cluster.properties ${DIR}/client.properties $TYPE_CONFIG
