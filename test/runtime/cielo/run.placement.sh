#!/bin/bash

NUM_NODES=$PBS_NUM_NODES
export CORES_PER_NODE=$PBS_NUM_PPN
CLIENT_NODES_PER_SERVER=4

if [ -z "$NUM_NODES" ] ; then
  NUM_NODES=5
fi

if [ -z "$CORES_PER_NODE" ] ; then
  export CORES_PER_NODE=16
fi

NUM_SERVER_NODES="`expr $NUM_NODES / \( $CLIENT_NODES_PER_SERVER + 1 \)`"  ## +1 is the server node
NUM_CLIENT_NODES="`expr $NUM_SERVER_NODES \* $CLIENT_NODES_PER_SERVER`"

SPN=1
NS="`expr $NUM_SERVER_NODES \* $SPN`"
CPN=1
NC="`expr $NUM_CLIENT_NODES \* $CPN`"

NUM_CLIENTS=$NC
NUM_SERVERS=$NS

mkdir -p ${EXPERIMENT_PATH}
cd ${EXPERIMENT_PATH}

export WORK_DIR=${EXPERIMENT_PATH}

export METADATA_CONFIG_FILE=md_config.txt
export METADATA_CONFIG_FILE2=md2_config.txt
export DATASTORE_CONFIG_FILE=ds_config.txt
export DATASTORE_CONFIG_FILE2=ds2_config.txt

cd $WORK_DIR
export MD_LOG_DIR=
export MD_FILE_NAME=md_server
export DS_LOG_DIR=
export DS_FILE_NAME=ds_server
#mkdir ${WORK_DIR}/${LOG_DIR}

#export SERVER_CONTACT_INFO=${WORK_DIR}/${LOG_DIR}/contact
#export NETCDF_CONTACT_INFO=${WORK_DIR}/${LOG_DIR}/contact.info

#server env vars
export MD_SERVER_LOG_LEVEL=3
export MD_SERVER_LOG_FILE=${WORK_DIR}/${MD_LOG_DIR}/${MD_FILE_NAME}.log
export MD_SERVER_LOG_FILE2=${WORK_DIR}/${MD_LOG_DIR}/${MD_FILE_NAME}2.log
export DS_SERVER_LOG_LEVEL=3
export DS_SERVER_LOG_FILE=${WORK_DIR}/${DS_LOG_DIR}/${DS_FILE_NAME}.log
export DS_SERVER_LOG_FILE2=${WORK_DIR}/${DS_LOG_DIR}/${DS_FILE_NAME}2.log

export NSSI_LOG_LEVEL=3

#aprun -p TXN -n 1 -N 1 ${EXE_PATH}/metadata_server METADATA_CONFIG_FILE &> ${MD_SERVER_LOG_FILE} &
#aprun -p TXN -n 1 -N 1 ${EXE_PATH}/datastore_server DATASTORE_CONFIG_FILE &> ${DS_SERVER_LOG_FILE} &

#aprun -p TXN -n 1 -N 1 ${EXE_PATH}/metadata_server METADATA_CONFIG_FILE2 &> ${MD_SERVER_LOG_FILE2} &
#aprun -p TXN -n 1 -N 1 ${EXE_PATH}/datastore_server DATASTORE_CONFIG_FILE2 &> ${DS_SERVER_LOG_FILE2} &

#sleep 10

CPN=16
NC="`expr $NUM_CLIENT_NODES \* $CPN`"

export NUM_NODES STRATEGY SPN NS CPN NC snodes cnodes

$1
