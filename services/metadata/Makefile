UNAME := $(shell uname)
UNAME2 := $(shell uname -a | grep mz)

ifeq ($(UNAME),Darwin)
TRILINOS_ROOT=//Users/$(LOGNAME)/build/Trilinos
CC = mpic++
else
ifeq ($(UNAME2),)
TRILINOS_ROOT=/ascldap/users/$(LOGNAME)/Trilinos
CC = mpiCC
else
TRILINOS_ROOT=/home/$(LOGNAME)/projects/trilinos/install/gnu
CC = CC
endif
endif

ifneq ($(UNAME2),)
NSSI_LIBDIR=-L${TRILINOS_ROOT}/lib \
-L.
NSSI_LIBS=-ltrios_nssi -ltrios_nnti -ltrios_support -ldl
NSSI_INCLUDE=-I${TRILINOS_ROOT}/include
else
NSSI_LIBDIR=-L${TRILINOS_ROOT}/cmake/packages/trios/libraries/support \
-L${TRILINOS_ROOT}/cmake/packages/trios/libraries/nessie/nssi \
-L${TRILINOS_ROOT}/cmake/packages/trios/libraries/nessie/nnti \
-L.
NSSI_LIBS=-ltrios_nssi -ltrios_nnti -ltrios_support
NSSI_INCLUDE=-I${TRILINOS_ROOT}/cmake/packages/trios \
-I${TRILINOS_ROOT}/packages/trios/libraries/nessie/client \
-I${TRILINOS_ROOT}/packages/trios/libraries/nessie/server \
-I${TRILINOS_ROOT}/packages/trios/libraries/nessie/nssi \
-I${TRILINOS_ROOT}/packages/trios/libraries/nessie/nnti \
-I${TRILINOS_ROOT}/packages/trios/libraries/support/xdr \
-I${TRILINOS_ROOT}/cmake/packages/trios/libraries/nessie/nssi \
-I${TRILINOS_ROOT}/cmake/packages/trios/libraries/nessie/nnti
endif

all: server client_lib client

server: metadata_server.C metadata_args.h client_lib
	gcc -c sqlite3.c -DSQLITE_ENABLE_RTREE=1
	$(CC) -g metadata_server.C sqlite3.o -o metadata_server ${NSSI_INCLUDE} ${NSSI_LIBDIR} ${NSSI_LIBS} -lmetadata_client

client_lib: metadata_client.C metadata_args.h metadata_args.c
	g++ -c -g metadata_client.C metadata_args.c ${NSSI_INCLUDE}
	ar rcs libmetadata_client.a metadata_client.o metadata_args.o
	ranlib libmetadata_client.a

client: test_client.C client_lib
	$(CC) -g test_client.C -o test_client ${NSSI_INCLUDE} ${NSSI_LIBDIR} -lmetadata_client ${NSSI_LIBS}

metadata_args.c: metadata_args.x
	rm -f $@
	@echo "#include <Trios_config.h>" >> $@
ifeq ($(UNAME),Darwin)
	rpcgen -Cc metadata_args.x | sed -e "s/uint/u_int/g" > metadata_args.c
else
	rpcgen -Cc metadata_args.x > metadata_args.c
endif

metadata_args.h: metadata_args.x
	rm -f $@
	@echo "#include <Trios_config.h>" >> $@
ifeq ($(UNAME),Darwin)
	rpcgen -Ch metadata_args.x >> $@
else
	rpcgen -Ch metadata_args.x | sed -e 's@rpc/rpc.h@rpc/xdr.h@' >> $@
endif

clean:
	rm -f *.o metadata_server test_client metadata_args.h metadata_args.c

cp:
	cp metadata_server test_client /gscratch2/gflofst/txn
