#!/bin/bash

NUM_JOBS=$1 ; shift
TEST_TYPE=$1 ; shift
CMDLINE_NODE_COUNT=$1 ; shift

LAST_JOBID=`msub -lwalltime=0:10:00,nodes=5:ppn=8 -j oe -N txn.${TEST_TYPE} ./place.${TEST_TYPE}.sh`
LAST_JOBID=`echo $LAST_JOBID | sed -e 's/[[:space:]]*//g' -e 's/[[:space:]]*$//g'`

for SERVER_COUNT in `seq 2 ${NUM_JOBS}` ; do
 if [ -z "$CMDLINE_NODE_COUNT" ] ; then
   NODE_COUNT=`echo " ( 4 + ( 2 ^ ( $SERVER_COUNT - 2 ) ) ) " | bc`
 else
   NODE_COUNT=$CMDLINE_NODE_COUNT
 fi
 LAST_JOBID=`msub -lwalltime=0:10:00,nodes=${NODE_COUNT}:ppn=16,depend=${LAST_JOBID} -j oe -N txn.${TEST_TYPE} ./place.${TEST_TYPE}.sh`
 LAST_JOBID=`echo $LAST_JOBID | sed -e 's/[[:space:]]*//g' -e 's/[[:space:]]*$//g'`
done

