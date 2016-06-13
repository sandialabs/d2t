#!/bin/bash

cd $PBS_O_WORKDIR

export EXE_PATH=`pwd`

export JOB_NAME=txn.pbs${PBS_JOBID}
export EXPERIMENT_PATH=`pwd`/results.$JOB_NAME
mkdir -p ${EXPERIMENT_PATH}
cd ${EXPERIMENT_PATH}

export CORES=`echo " ( ( $PBS_NUM_NODES - 4 ) * $PBS_NUM_PPN ) " | bc`

$EXE_PATH/run.placement.sh $EXE_PATH/txn.sh
