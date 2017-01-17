#!/bin/bash

ROOT=/gscmnt/gc2802/halllab/idas

export SPARK_HOME=${ROOT}/software/local/spark-2.0.2-bin-hadoop2.7
export LSF_SPARK_WRAPPER=${ROOT}/hail-play/lsf-spark-wrapper.sh
export LOCAL_HAIL=${ROOT}/software/downloads/github/hail/build/install/hail/bin/hail
export SPARK_HAIL_JAR=${ROOT}/software/downloads/github/hail/build/libs/hail-all-spark.jar

set -o xtrace
/bin/bash ${LSF_SPARK_WRAPPER} \
    --class org.broadinstitute.hail.driver.Main \
    ${SPARK_HAIL_JAR} \
    $@
set +o xtrace
