#!/bin/bash

#rm -f $NETCDF_CONTACT_INFO

aprun -p TXN -n 1 -N 1 ${EXE_PATH}/metadata_server METADATA_CONFIG_FILE &> ${MD_SERVER_LOG_FILE} &
aprun -p TXN -n 1 -N 1 ${EXE_PATH}/datastore_server DATASTORE_CONFIG_FILE &> ${DS_SERVER_LOG_FILE} &

aprun -p TXN -n 1 -N 1 ${EXE_PATH}/metadata_server METADATA_CONFIG_FILE2 &> ${MD_SERVER_LOG_FILE2} &
aprun -p TXN -n 1 -N 1 ${EXE_PATH}/datastore_server DATASTORE_CONFIG_FILE2 &> ${DS_SERVER_LOG_FILE2} &

sleep 10

if [ "$CORES_PER_NODE" -gt "$CORES" ] ; then
    export CORES=$CORES_PER_NODE
fi

case $CORES in
	8)       nx=2 ;  ny=2 ;  nz=2  ; rank_to_fail=7
	   ;;
	16)      nx=2 ;  ny=2 ;  nz=4  ; rank_to_fail=15
	   ;;
	32)      nx=2 ;  ny=4 ;  nz=4  ; rank_to_fail=31
	   ;;
	64)      nx=4 ;  ny=4 ;  nz=4  ; rank_to_fail=63
	   ;;
	128)     nx=4 ;  ny=4 ;  nz=8  ; rank_to_fail=127
	   ;;
	256)     nx=4 ;  ny=8 ;  nz=8  ; rank_to_fail=255
	   ;;
	512)     nx=8 ;  ny=8 ;  nz=8  ; rank_to_fail=511
	   ;;
	1024)    nx=8 ;  ny=8 ;  nz=16 ; rank_to_fail=1023
	   ;;
	2048)    nx=8 ;  ny=16 ; nz=16 ; rank_to_fail=2047
	   ;;
	4096)    nx=16 ; ny=16 ; nz=16 ; rank_to_fail=4095
	   ;;
	8192)    nx=16 ; ny=16 ; nz=32 ; rank_to_fail=8191
	   ;;
	16384)   nx=16 ; ny=32 ; nz=32 ; rank_to_fail=16383
	   ;;
	32768)   nx=32 ; ny=32 ; nz=32 ; rank_to_fail=32767
	   ;;
	65536)   nx=32 ; ny=32 ; nz=64 ; rank_to_fail=65535
	   ;;
esac

echo Writing Test
aprun -p TXN -n $CORES -N $CORES_PER_NODE ${EXE_PATH}/write_test out $nx $ny $nz 32 32 32

sleep 10

echo Update Test
#for i in `seq 5`
#do
aprun -p TXN -n $CORES -N $CORES_PER_NODE ${EXE_PATH}/update_test $rank_to_fail
#mv ${CORES}_${rank_to_fail}.metrics ${CORES}_${rank_to_fail}.${i}.metrics
#done

sleep 10
