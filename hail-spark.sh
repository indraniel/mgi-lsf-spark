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
export LOCAL_HAIL=${ROOT}/software/downloads/github/hail/build/install/hail/bin/hail
export SPARK_HAIL_JAR=${ROOT}/software/downloads/github/hail/build/libs/hail-all-spark.jar

set -o xtrace
/bin/bash ${LSF_SPARK_WRAPPER} \
    --class org.broadinstitute.hail.driver.Main \
    ${SPARK_HAIL_JAR} \
    $@
set +o xtrace
