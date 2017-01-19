#!/bin/bash

ROOT=/gscmnt/gc2802/halllab/idas

# needed because of the c/c++ code embedded in hail
GCC_BIN=/opt/gcc-4.8.4/bin
GCC_LIBS=/opt/gcc-4.8.4/lib64
OPENBLAS_LIBS=/gscmnt/gc2802/halllab/idas/software/local/lib
CMAKE_BIN=/gscuser/idas/software/cmake/bin

# setup the needed environment variables
export PATH=${CMAKE_BIN}:${GCC_BIN}:${PATH}
export LD_LIBRARY_PATH=${GCC_LIBS}:${OPENBLAS_LIBS}:${LD_LIBRARY_PATH}

# SPARK software location
export SPARK_HOME=${ROOT}/software/local/spark-2.0.2-bin-hadoop2.7

# HAIL software locations
export HAIL_HOME=${ROOT}/software/downloads/github/hail
export LOCAL_HAIL=${HAIL_HOME}/build/install/hail/bin/hail
export SPARK_HAIL_JAR=${HAIL_HOME}/build/libs/hail-all-spark.jar

# the PYTHON that should be used
export PYSPARK_PYTHON=/gscuser/idas/.pyenv/versions/2.7.10/bin/python

# set -o xtrace
# /bin/bash ${LSF_SPARK_WRAPPER} \
#     --class org.broadinstitute.hail.driver.Main \
#     ${SPARK_HAIL_JAR} \
#     $@
# set +o xtrace

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

function die {
    local msg=$1
    echo "ERROR: ${msg}" >&2
    exit 1
}

function check_in_lsf_cluster {
    err="LSB_MCPU_HOSTS environment variable not found! "
    err+="This is not a multi-node LSF job!"
    : "${LSB_MCPU_HOSTS:?${err}}"
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

function start_spark_master {
    ${SPARK_HOME}/sbin/start-master.sh > /dev/null
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

function run_pyhail_cmd {
    local spark_python_libs=${SPARK_HOME}/python/lib
    local hail_python_libs=${HAIL_HOME}/python/lib

    local py4j=${spark_python_libs}/py4j-0.10.3-src.zip
    local pyspark=${spark_python_libs}/pyspark.zip
    local pyhail=${hail_python_libs}/pyhail.zip

    set -o xtrace
    ${SPARK_HOME}/bin/spark-submit \
        --jars ${SPARK_HAIL_JAR} \
        --py-files ${py4j},${pyspark},${pyhail} \
        --master spark://$SPARK_MASTER:$SPARK_MASTER_PORT \
        $@
    set +o xtrace

}

function cluster {
    check_in_lsf_cluster

    # setup SPARK CLUSTER environment variables
    export SPARK_MASTER=`hostname`
    export SPARK_LOCAL_IP="127.0.0.1"
    : "${SPARK_MASTER_PORT:=7077}"

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

    log "Exec pyhail command"
    run_pyhail_cmd $@

    log "Stop Spark Master"
    stop_spark_master

    log "Stop Spark Workers"
    stop_spark_workers "$(declare -p spark_workers)"
}

function local_pyspark {
    local py4j=${SPARK_HOME}/python/lib/py4j-0.10.3-src.zip
    local spark_python_libs=${SPARK_HOME}/python
    local hail_python_libs=${HAIL_HOME}/python
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.3-src.zip:$HAIL_HOME/python
    export SPARK_CLASSPATH=${SPARK_HAIL_JAR}
    ${PYSPARK_PYTHON}
}

function main {
    local exec_type=$1
    shift
    
    case ${exec_type} in 
        local)
            log "Running pyhail repl in local mode."
            local_pyspark
            ;;
        cluster)
            log "Running Spark in cluster mode."
            log "the pyhail args are:" $@
            cluster $@
            ;;
        *)
            msg="The first argument must specify Apache Spark's run mode."
            msg+="Options are 'cluster' or 'local'"
            die "${msg}"
            ;;
    esac
}

main $@
