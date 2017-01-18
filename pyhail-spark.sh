#!/bin/bash

ROOT=/gscmnt/gc2802/halllab/idas

# needed because of the c/c++ code embedded in hail
GCC_BIN=/opt/gcc-4.8.4/bin
GCC_LIBS=/opt/gcc-4.8.4/lib64
OPENBLAS_LIBS=/gscmnt/gc2802/halllab/idas/software/local/lib
CMAKE_BIN=/gscuser/idas/software/cmake/bin

export PATH=${CMAKE_BIN}:${GCC_BIN}:${PATH}
export LD_LIBRARY_PATH=${GCC_LIBS}:${OPENBLAS_LIBS}:${LD_LIBRARY_PATH}

export PYSPARK_PYTHON=/gscuser/idas/.pyenv/versions/2.7.10/bin/python
export SPARK_HOME=${ROOT}/software/local/spark-2.0.2-bin-hadoop2.7
export LSF_SPARK_WRAPPER=${ROOT}/hail-play/lsf-spark-wrapper.sh
export HAIL_HOME=${ROOT}/software/downloads/github/hail
export LOCAL_HAIL=${HAIL_HOME}/build/install/hail/bin/hail
export SPARK_HAIL_JAR=${HAIL_HOME}/build/libs/hail-all-spark.jar

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

function die {
    local msg=$1
    echo "ERROR: ${msg}" >&2
    exit 1
}

function local_pyspark {
    local py4j=${SPARK_HOME}/python/lib/py4j-0.10.3-src.zip
    local spark_python_libs=${SPARK_HOME}/python
    local hail_python_libs=${HAIL_HOME}/python
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.3-src.zip:$HAIL_HOME/python
    export SPARK_CLASSPATH=${SPARK_HAIL_JAR}
    ${PYSPARK_PYTHON}
}

function cluster {
    echo "Please implement me!"
}

function main {
    local exec_type=$1
    shift
    
    case ${exec_type} in 
        local)
            log "Running Spark in local mode."
            local_pyspark
            ;;
        cluster)
            log "Running Spark in clustr mode."
            echo "remaining args are:" $@
            ;;
        *)
            msg="The first argument must specify Apache Spark's run mode."
            msg+="Options are 'cluster' or 'local'"
            die "${msg}"
            ;;
    esac

}

main $@
