#!/bin/bash
#    Copyright 2015 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

set -euxo pipefail

# Variables for running this script
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly DRUID_MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly WORKER_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly CONNECTOR_JAR="$(find /usr/lib/hadoop/lib -name 'gcs-connector-*.jar')"
readonly DRUID_VERSION='0.12.1'
readonly HTTP_PORT='8080'
readonly INIT_SCRIPT='/usr/lib/systemd/system/druid.service'

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function get_druid(){
  # Download and unpack Druid server
  wget http://static.druid.io/artifacts/releases/druid-${DRUID_VERSION}-bin.tar.gz
  tar -zxvf druid-${DRUID_VERSION}-bin.tar.gz
  mkdir -p /var/druid/data
}

function configure_metastore(){
  local metastore_uri
  metastore_uri=$(bdconfig get_property_value \
    --configuration_file /etc/hive/conf/hive-site.xml \
    --name hive.metastore.uris 2>/dev/null)
}

function configure_jvm(){
  cat > presto-server-${PRESTO_VERSION}/etc/jvm.config <<EOF
-server
-Xmx${PRESTO_JVM_MB}m
-Xmn512m
-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent
-XX:ReservedCodeCacheSize=150M
-XX:+ExplicitGCInvokesConcurrent
-XX:+CMSClassUnloadingEnabled
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-Dhive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
-Djava.library.path=/usr/lib/hadoop/lib/native/:/usr/lib/
EOF
}

function configure_master(){
  # Configure master properties
  if [[ ${WORKER_COUNT} == 0 ]]; then
    # master on single-node is also worker
    include_coordinator='true'
  else
    include_coordinator='false'
  fi
  cat > presto-server-${PRESTO_VERSION}/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=${include_coordinator}
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery-server.enabled=true
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
EOF

  # Install cli
  $(wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar -O /usr/bin/presto)
  $(chmod a+x /usr/bin/presto)
}

function configure_worker(){
  cat > presto-server-${PRESTO_VERSION}/etc/config.properties <<EOF
coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
EOF
}

function start_presto(){
  # Start presto as systemd job

  cat << EOF > ${INIT_SCRIPT}
[Unit]
Description=Presto DB

[Service]
Type=forking
ExecStart=/presto-server-${PRESTO_VERSION}/bin/launcher.py start
ExecStop=/presto-server-${PRESTO_VERSION}/bin/launcher.py stop
Restart=always


[Install]
WantedBy=multi-user.target
EOF

  chmod a+rw ${INIT_SCRIPT}

  systemctl daemon-reload
  systemctl enable presto
  systemctl start presto
  systemctl status presto
}

function configure_and_start_druid(){
  # Copy required Jars
  cp ${CONNECTOR_JAR} presto-server-${PRESTO_VERSION}/plugin/hive-hadoop2

  # Configure Presto
  mkdir -p presto-server-${PRESTO_VERSION}/etc/catalog

  configure_node_properties
  configure_hive
  configure_jvm

  if [[ "${HOSTNAME}" == "${PRESTO_MASTER_FQDN}" ]]; then
    configure_master
    start_presto
  fi

  if [[ "${ROLE}" == 'Worker' ]]; then
    configure_worker
    start_presto
  fi
}

function main(){
  get_druid
  configure_and_start_druid
}

main
