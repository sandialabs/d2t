#!/bin/bash

#SBATCH --account=FY1310104
#SBATCH --job-name=txn.testing
#SBATCH --nodes=5
#SBATCH --time=0:04:00

#8 cores per node
# 4 x 4 x 4 = 64 cores = 8 nodes
# metadata = 1 node
# datastore = 1 node
#
# a.out fn npx npy npz ndx ndy ndz
# fn is the output filename
# np[xyz] are number of procs wide in that dimension
# nd[xyz] size of each local block for each process in each dimension
echo starting txn testing

WORK_DIR=/gscratch2/gflofst/txn

export METADATA_CONFIG_FILE=md_config.txt
export METADATA_CONFIG_FILE2=md2_config.txt
export DATASTORE_CONFIG_FILE=ds_config.txt
export DATASTORE_CONFIG_FILE2=ds2_config.txt

cd $WORK_DIR
#lfs setstripe . -i -1 -c 96 -s 1048576
export MD_LOG_DIR=
export MD_FILE_NAME=md_server
export DS_LOG_DIR=
export DS_FILE_NAME=ds_server
#mkdir ${WORK_DIR}/${LOG_DIR}

#export SERVER_CONTACT_INFO=${WORK_DIR}/${LOG_DIR}/contact
#export NETCDF_CONTACT_INFO=${WORK_DIR}/${LOG_DIR}/contact.info

#server env vars
export MD_SERVER_LOG_LEVEL=1
export MD_SERVER_LOG_FILE=${WORK_DIR}/${MD_LOG_DIR}/${MD_FILE_NAME}.log
export MD_SERVER_LOG_FILE2=${WORK_DIR}/${MD_LOG_DIR}/${MD_FILE_NAME}2.log
export DS_SERVER_LOG_LEVEL=1
export DS_SERVER_LOG_FILE=${WORK_DIR}/${DS_LOG_DIR}/${DS_FILE_NAME}.log
export DS_SERVER_LOG_FILE2=${WORK_DIR}/${DS_LOG_DIR}/${DS_FILE_NAME}2.log

#mpiexec -np 1 --npernode 1 numa_wrapper --ppn 1 ${WORK_DIR}/metadata_server &> ${SERVER_LOG_FILE} &
mpiexec -np 1 --npernode 1 ${WORK_DIR}/metadata_server METADATA_CONFIG_FILE &> ${MD_SERVER_LOG_FILE} &
mpiexec -np 1 --npernode 1 ${WORK_DIR}/datastore_server DATASTORE_CONFIG_FILE &> ${DS_SERVER_LOG_FILE} &

mpiexec -np 1 --npernode 1 ${WORK_DIR}/metadata_server METADATA_CONFIG_FILE2 &> ${MD_SERVER_LOG_FILE2} &
mpiexec -np 1 --npernode 1 ${WORK_DIR}/datastore_server DATASTORE_CONFIG_FILE2 &> ${DS_SERVER_LOG_FILE2} &

sleep 10

#export NETCDF_CONFIG_FILE=${WORK_DIR}/${LOG_DIR}/config.file.xml

#${WORK_DIR}/create.netcdf.config.sh ${NETCDF_CONFIG_FILE} ${SERVER_CONTACT_INFO} ${WRITE_MODE} ${USE_SUBCHUNKING}

#client env vars
export NSSI_LOG_LEVEL=1
#export NSSI_LOG_FILE_PER_NODE=TRUE
#export NSSI_LOG_FILE=${WORK_DIR}/${LOG_DIR}/client.log

UNAME=`uname -a | grep glory`
if [ -n "$UNAME" ]
then
#mpiexec -np 1 --npernode 1 numa_wrapper --ppn 1 ${WORK_DIR}/test_client 
echo Writing Test
mpiexec -np 8 --npernode 8 ${WORK_DIR}/write_test out 2 2 2 32 32 32
else
#mpiexec -np 1 --npernode 1 numa_wrapper --ppn 1 ${WORK_DIR}/test_client 
echo Writing Test
mpiexec -np 8 --npernode 8 numa_wrapper --ppn 8 ${WORK_DIR}/write_test out 2 2 2 32 32 32
fi

sleep 10

if [ -n "$UNAME" ]
then
echo Update Test
#mpiexec -np 8 --npernode 8 numa_wrapper --ppn 8 valgrind --leak-check=full ${WORK_DIR}/update_test 
mpiexec -np 8 --npernode 8 ${WORK_DIR}/update_test 
else
echo Update Test
#mpiexec -np 8 --npernode 8 numa_wrapper --ppn 8 valgrind --leak-check=full ${WORK_DIR}/update_test 
mpiexec -np 8 --npernode 8 numa_wrapper --ppn 8 ${WORK_DIR}/update_test 
fi

sleep 10
