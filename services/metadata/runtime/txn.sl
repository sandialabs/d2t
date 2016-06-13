#!/bin/bash

#SBATCH --account=FY1310104
#SBATCH --job-name=txn.testing
#SBATCH --nodes=2
#SBATCH --time=0:02:00

echo starting txn testing

WORK_DIR=/gscratch2/gflofst/txn

export METADATA_CONFIG_FILE=md_config.txt

cd $WORK_DIR
#lfs setstripe . -i -1 -c 96 -s 1048576
export LOG_DIR=
export FILE_NAME=server
#mkdir ${WORK_DIR}/${LOG_DIR}

#export SERVER_CONTACT_INFO=${WORK_DIR}/${LOG_DIR}/contact
#export NETCDF_CONTACT_INFO=${WORK_DIR}/${LOG_DIR}/contact.info

#server env vars
export SERVER_LOG_LEVEL=5
export SERVER_LOG_FILE=${WORK_DIR}/${LOG_DIR}/${FILE_NAME}.log

#mpiexec -np 1 --npernode 1 numa_wrapper --ppn 1 ${WORK_DIR}/metadata_server &> ${SERVER_LOG_FILE} &
mpiexec -np 1 --npernode 1 ${WORK_DIR}/metadata_server METADATA_CONFIG_FILE &> ${SERVER_LOG_FILE} &

sleep 10

#export NETCDF_CONFIG_FILE=${WORK_DIR}/${LOG_DIR}/config.file.xml

#${WORK_DIR}/create.netcdf.config.sh ${NETCDF_CONFIG_FILE} ${SERVER_CONTACT_INFO} ${WRITE_MODE} ${USE_SUBCHUNKING}

#client env vars
export NSSI_LOG_LEVEL=5
#export NSSI_LOG_FILE_PER_NODE=TRUE
#export NSSI_LOG_FILE=${WORK_DIR}/${LOG_DIR}/client.log

#mpiexec -np 1 --npernode 1 numa_wrapper --ppn 1 ${WORK_DIR}/test_client 
mpiexec -np 1 --npernode 1 ${WORK_DIR}/test_client 

sleep 10
