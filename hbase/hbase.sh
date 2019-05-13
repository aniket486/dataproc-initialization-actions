#!/bin/bash
#    Copyright 2018 Google, Inc.
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
#
# This initialization action installs Apache HBase on Dataproc Cluster.

set -euxo pipefail

readonly HBASE_HOME='/etc/hbase'
readonly CLUSTER_NAME="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"
readonly WORKER_COUNT="$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)"
readonly MASTER_ADDITIONAL="$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)"
readonly ENABLE_KERBEROS="$(/usr/share/google/get_metadata_value attributes/enable-kerberos)"
readonly DOMAIN=$(dnsdomainname)
readonly REALM=$(echo "${DOMAIN}" | awk '{print toupper($0)}')
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly FQDN=$(hostname -f)
readonly KEYTAB_DIR="/etc/security/keytab"
readonly HBASE_MASTER_KEYTAB_FILE="${KEYTAB_DIR}/hbase-master.keytab"
readonly HBASE_REGIONSERVER_KEYTAB_FILE="${KEYTAB_DIR}/hbase-region.keytab"


function retry_command() {
  cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function update_apt_get() {
  retry_command "apt-get update"
}

function install_apt_get() {
  pkgs="$@"
  retry_command "apt-get install -y $pkgs"
}

function add_to_hbase_site_xml_tmp() {
  local name=$1
  local value=$2

  bdconfig set_property \
    --configuration_file 'hbase-site.xml.tmp' \
    --name "$name" --value "$value" \
    --clobber
}

function configure_hbase() {
  cat << EOF > hbase-site.xml.tmp
  <configuration>
    <property>
      <name>hbase.cluster.distributed</name>
      <value>true</value>
    </property>
    <property>
      <name>hbase.zookeeper.property.initLimit</name>
      <value>20</value>
    </property>
  </configuration>
EOF

  cat << EOF > /etc/systemd/system/hbase-master.service
[Unit]
Description=HBase Master
Wants=network-online.target
After=network-online.target hadoop-hdfs-namenode.service

[Service]
User=root
Group=root
Type=simple
EnvironmentFile=/etc/environment
Environment=HBASE_HOME=/etc/hbase
ExecStart=/usr/bin/hbase \
  --config ${HBASE_HOME}/conf/ \
  master start

[Install]
WantedBy=multi-user.target
EOF

  cat << EOF > /etc/systemd/system/hbase-regionserver.service
[Unit]
Description=HBase Regionserver
Wants=network-online.target
After=network-online.target hadoop-hdfs-datanode.service

[Service]
User=root
Group=root
Type=simple
EnvironmentFile=/etc/environment
Environment=HBASE_HOME=/etc/hbase
ExecStart=/usr/bin/hbase \
  --config ${HBASE_HOME}/conf/ \
  regionserver start

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload

  # Prepare and merge configuration values:
  # hbase.rootdir
  local hbase_root_dir="$(/usr/share/google/get_metadata_value attributes/hbase-root-dir)"
  if [[ -z "${hbase_root_dir}" ]]; then
    if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
      hbase_root_dir="hdfs://${CLUSTER_NAME}:8020/hbase"
    else
      hbase_root_dir="hdfs://${CLUSTER_NAME}-m:8020/hbase"
    fi
  fi
  add_to_hbase_site_xml_tmp "hbase.rootdir" "${hbase_root_dir}"

  # zookeeper.quorum
  local zookeeper_nodes="$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg \
  | uniq | cut -d '=' -f 2 | cut -d ':' -f 1 | xargs echo | sed "s/ /,/g")"
  add_to_hbase_site_xml_tmp "hbase.zookeeper.quorum" "${zookeeper_nodes}"

  # Prepare kerberos specific config values for hbase-site.xml
  if [ "${ENABLE_KERBEROS}" = true ]; then
    # Kerberos authentication
    add_to_hbase_site_xml_tmp "hbase.security.authentication" "kerberos"

    # Security authorization
    add_to_hbase_site_xml_tmp "hbase.security.authorization" "true"

    # Kerberos master principal
    add_to_hbase_site_xml_tmp "hbase.master.kerberos.principal" "hbase/_HOST@${REALM}"

    # Kerberos region server principal
    add_to_hbase_site_xml_tmp "hbase.regionserver.kerberos.principal" "hbase/_HOST@${REALM}"

    # Kerberos master server keytab file path
    add_to_hbase_site_xml_tmp "hbase.master.keytab.file" \
        "$HBASE_MASTER_KEYTAB_FILE"

    # Kerberos region server keytab file path
    add_to_hbase_site_xml_tmp "hbase.regionserver.keytab.file" \
        "$HBASE_REGIONSERVER_KEYTAB_FILE"

    # Zookeeper authentication provider
    add_to_hbase_site_xml_tmp "hbase.zookeeper.property.authProvider.1" \
        "org.apache.zookeeper.server.auth.SASLAuthenticationProvider"

    # HBase coprocessor region classes
    add_to_hbase_site_xml_tmp "hbase.coprocessor.region.classes" \
        "org.apache.hadoop.hbase.security.token.TokenProvider"

    # Zookeeper remove host from principal
    add_to_hbase_site_xml_tmp \
        "hbase.zookeeper.property.kerberos.removeHostFromPrincipal" "true"

    # Zookeeper remove realm from principal
    add_to_hbase_site_xml_tmp \
        "hbase.zookeeper.property.kerberos.removeRealmFromPrincipal" "true"

    # Zookeeper znode
    add_to_hbase_site_xml_tmp "zookeeper.znode.parent" "/hbase-secure"

    # HBase RPC protection
    add_to_hbase_site_xml_tmp "hbase.rpc.protection" "privacy"
  fi

  # Merge all config values to hbase-site.xml
  bdconfig merge_configurations \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --source_configuration_file hbase-site.xml.tmp \
    --clobber

  ROOT_PRINCIPAL_PASSWORD_URI=$(get_dataproc_property kerberos.root.principal.password.uri)
  if [[ -z "${ROOT_PRINCIPAL_PASSWORD_URI}" ]] ; then
    err "Unable to find root principal password. Have you enabled 'KERBEROS' optional component?"
  fi
  ROOT_PRINCIPAL_PASSWORD=$(decrypt_with_kms_key "${ROOT_PRINCIPAL_PASSWORD_URI}")
  if [[ -z "${ROOT_PRINCIPAL_PASSWORD}" ]] ; then
    err "Root principal password cannot be empty!"
  fi
  if [[ "${ENABLE_KERBEROS}" == "true" ]]; then

  
  

  if [ "${ENABLE_KERBEROS}" = true ]; then
    local machine_nr=$(echo $HOSTNAME | sed 's/.*-.-\([0-9]\)*.*/\1/g')
    local masters=$(/usr/share/google/get_metadata_value attributes/dataproc-master),$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)

    if [[ $machine_nr -eq "0" ]] && [[ "${ROLE}" == "Master" ]]; then
      # Master
      export IFS=","
      for m in $masters;
      do
        sudo kadmin.local -q "addprinc -randkey hbase/${m}.${DOMAIN}@${REALM}"
        echo "Generating hbase keytab..."
        sudo kadmin.local -q "xst -k ${HBASE_HOME}/conf/hbase-${m}.keytab hbase/${m}.${DOMAIN}"
        sudo gsutil cp ${HBASE_HOME}/conf/hbase-${m}.keytab ${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/hbase-${m}.keytab
      done

      # Worker
      for (( c="0"; c<$WORKER_COUNT; c++ ))
      do
        sudo kadmin.local -q "addprinc -randkey hbase/${CLUSTER_NAME}-w-${c}.${DOMAIN}"
        echo "Generating hbase keytab..."
        sudo kadmin.local -q "xst -k ${HBASE_HOME}/conf/hbase-${CLUSTER_NAME}-w-${c}.keytab hbase/${CLUSTER_NAME}-w-${c}.${DOMAIN}"
        sudo gsutil cp ${HBASE_HOME}/conf/hbase-${CLUSTER_NAME}-w-${c}.keytab ${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/hbase-${CLUSTER_NAME}-w-${c}.keytab
      done
      sudo touch /tmp/_success
      sudo gsutil cp /tmp/_success ${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/_success
    fi
    success=1
    while [ $success -eq "1" ]; do
      sleep 1
      success=$(gsutil -q stat ${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/_success; echo $?)
    done

    # Define keytab path based on role
    if [[ "${ROLE}" == 'Master' ]]; then
      hbase_keytab_path=${HBASE_HOME}/conf/hbase-master.keytab
    else
      hbase_keytab_path=${HBASE_HOME}/conf/hbase-region.keytab
    fi

    # Copy keytab to machine
    sudo gsutil cp ${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/hbase-${HOSTNAME}.keytab $hbase_keytab_path

    # Change owner of keytab to hbase with read only permissions
    if [ -f $hbase_keytab_path ]; then
      sudo chown hbase:hbase $hbase_keytab_path
      sudo chmod 0400 $hbase_keytab_path
    fi

    # Change regionserver information
    for (( c="0"; c<$WORKER_COUNT; c++ ))
    do
      echo "${CLUSTER_NAME}-w-${c}.${DOMAIN}" >> /tmp/regionservers
    done
    sudo mv /tmp/regionservers ${HBASE_HOME}/conf/regionservers

    # Add server JAAS
    cat > /tmp/hbase-server.jaas << EOF
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  useTicketCache=false
  keyTab="${hbase_keytab_path}"
  principal="hbase/${FQDN}";
};
EOF

    # Copy JAAS file to hbase conf directory
    sudo mv /tmp/hbase-server.jaas ${HBASE_HOME}/conf/hbase-server.jaas

    # Add client JAAS
    cat > /tmp/hbase-client.jaas << EOF
Client {
      com.sun.security.auth.module.Krb5LoginModule required
      useKeyTab=false
      useTicketCache=true;
};
EOF

    # Copy JAAS file to hbase conf directory
    sudo mv /tmp/hbase-client.jaas ${HBASE_HOME}/conf/hbase-client.jaas


    # Extend hbase enviroment variable script
    cat ${HBASE_HOME}/conf/hbase-env.sh > /tmp/hbase-env.sh
    cat >> /tmp/hbase-env.sh << EOF
export HBASE_MANAGES_ZK=false
export HBASE_OPTS="\$HBASE_OPTS -Djava.security.auth.login.config=/etc/hbase/conf/hbase-client.jaas"
export HBASE_MASTER_OPTS="\$HBASE_MASTER_OPTS -Djava.security.auth.login.config=/etc/hbase/conf/hbase-server.jaas"
export HBASE_REGIONSERVER_OPTS="\$HBASE_REGIONSERVER_OPTS -Djava.security.auth.login.config=/etc/hbase/conf/hbase-server.jaas"
EOF

    # Copy script to hbase conf directory
    sudo mv /tmp/hbase-env.sh ${HBASE_HOME}/conf/hbase-env.sh
  fi

  # On single node clusters we must also start regionserver on it.
  if [[ "${WORKER_COUNT}" -eq 0 ]]; then
    systemctl start hbase-regionserver
  fi
}


function main() {
  update_apt_get || err 'Unable to update packages lists.'
  install_apt_get hbase || err 'Unable to install hbase.'

  configure_hbase

  if [[ "${ROLE}" == 'Master' ]]; then
    systemctl start hbase-master
  else
    systemctl start hbase-regionserver
  fi
}

main
