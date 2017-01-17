#!/bin/bash

# Helpful notes
# http://stackoverflow.com/questions/307503/whats-a-concise-way-to-check-that-environment-variables-are-set-in-unix-shellsc
# http://stackoverflow.com/questions/4069188/how-to-pass-an-associative-array-as-argument-to-a-function-in-bash
# http://stackoverflow.com/questions/6660010/bash-how-to-assign-an-associative-array-to-another-variable-name-e-g-rename-t
# http://notes-matthewlmcclure.blogspot.com/2009/12/return-array-from-bash-function-v-2.html

# ensure the spark environment is setup

#SPARK_HOME=/gscmnt/gc2802/halllab/idas/software/local/spark-2.0.2-bin-hadoop2.7
: "${SPARK_HOME:?Please set SPARK_HOME environment variable!}"
: "${SPARK_MASTER_PORT:=7077}"

# ensure that we're inside an LSF multi-node job
err="LSB_MCPU_HOSTS environment variable not found! "
err+="This is not a multi-node LSF job!"
: "${LSB_MCPU_HOSTS:?${err}}"

#get master host, the first execution host of the LSF job
SPARK_MASTER=`hostname`
SPARK_LOCAL_IP="127.0.0.1"

function log {
    local msg=$1
    echo "==== ${msg} ====" >&2
}

function note {
    local msg=$1
    echo "---> ${msg}" >&2
}

function subnote {
    local msg=$1
    echo "--->> ${msg}" >&2
}

function start_spark_master {
    $SPARK_HOME/sbin/start-master.sh > /dev/null
    log "Successfully launched spark master node"
    sleep 5
}

function start_spark_workers {
    local -A spark_workers
    local i=
    local -i count=0
    eval "local -A nodes=${1#*=}"

    for i in "${!nodes[@]}"; do
        count=$(( $count + 1 ))
        host=$i
        cpus=${nodes[$host]}
        note "Starting Spark Worker ${count} -- host : ${host} | cpus : ${cpus}"
#        set -o xtrace
        pid=$(ssh -tq \
                  -o StrictHostKeyChecking=no \
                  -o UserKnownHostsFile=/dev/null \
                  -l ${USER} \
                  ${host} \
                  "nohup \
                  $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
                  spark://$SPARK_MASTER:$SPARK_MASTER_PORT \
                  -c ${cpus} \
                  < /dev/null \
                  > /dev/null & echo \$!")
#        set +o xtrace
        subnote "Spark Worker ${count} -- host : ${host} | cpus : ${cpus} | pid : ${pid}"
        spark_workers[${host}]=${pid}
    done

    # Wait a few moments for all the workers to fully start up
    # and connect to the master node
    sleep 3

    declare -p spark_workers | sed -e 's/^declare -A spark_workers=//'
}

function stop_spark_workers {
    local -A spark_workers
    eval "local -A spark_workers=${1#*=}"

    for i in "${!spark_workers[@]}"; do
        host=$i
        pid=${spark_workers[$host]}
        note "Stopping Spark Worker -- host : ${host} | pid : ${pid}"
#        set -o xtrace
        ssh -tq \
            -o StrictHostKeyChecking=no \
            -o UserKnownHostsFile=/dev/null \
            -l ${USER} \
            ${host} \
            "kill ${pid}"
#        set +o xtrace
    done
}

function stop_spark_master {
    $SPARK_HOME/sbin/stop-master.sh > /dev/null
}

function run_spark_cmd {
    ${SPARK_HOME}/bin/spark-submit --master spark://$SPARK_MASTER:$SPARK_MASTER_PORT $@
}

function parse_lsf_cpu_hosts {
    local -a cpu_hosts=(${LSB_MCPU_HOSTS})
    declare -A host_cpu_dictionary
    local j=
    local host=
    local cpus=
    for (( i = 0; i < ${#cpu_hosts[@]}; i += 2 )); do
        j=$(( $i + 1 ))
        host=${cpu_hosts[$i]}
        cpus=${cpu_hosts[$j]}
        host_cpu_dictionary[${host}]=${cpus}
    done
    declare -p host_cpu_dictionary | sed -e 's/^declare -A host_cpu_dictionary=//'
}

function main {
    local -A lsf_hosts
    local -A spark_workers

    log "SPARK_HOME: ${SPARK_HOME}"
    log "SPARK_MASTER: ${SPARK_MASTER}"

    log "Gathering HOST and CPU info"
    cpu_hosts_string=$(parse_lsf_cpu_hosts)
    eval "local -A lsf_hosts="${cpu_hosts_string}

    log "Starting spark master node: spark://${SPARK_MASTER}:${SPARK_MASTER_PORT}"
    start_spark_master

    log "Starting spark worker nodes"
    spark_workers_string=$(start_spark_workers "$(declare -p lsf_hosts)")
    eval "local -A spark_workers="${spark_workers_string}

    log "Start Spark Application"
    run_spark_cmd $@

    log "Stop Spark Master"
    stop_spark_master

    log "Stop Spark Workers"
    stop_spark_workers "$(declare -p spark_workers)"
}

main $@
