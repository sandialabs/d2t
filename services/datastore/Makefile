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
TRILINOS_ROOT=/home/$(LOGNAME)/projects/trilinos/install/gnu/
CC = CC
endif
endif

ifneq ($(UNAME2),)
NSSI_LIBDIR=-L${TRILINOS_ROOT}/lib \
-L.
NSSI_LIBS=-ltrios_nssi -ltrios_nnti -ltrios_support
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

server: datastore_server.C datastore_args.h client_lib
	$(CC) -g datastore_server.C -o datastore_server ${NSSI_INCLUDE} ${NSSI_LIBDIR} ${NSSI_LIBS} -ldatastore_client

client_lib: datastore_client.C datastore_args.h datastore_args.c
	g++ -c -g datastore_client.C datastore_args.c ${NSSI_INCLUDE}
	ar rcs libdatastore_client.a datastore_client.o datastore_args.o
	ranlib libdatastore_client.a

client: test_client.C client_lib
	$(CC) -g test_client.C -o test_client ${NSSI_INCLUDE} ${NSSI_LIBDIR} -ldatastore_client ${NSSI_LIBS}

datastore_args.c: datastore_args.x
	rm -f $@
	@echo "#include <Trios_config.h>" >> $@
ifeq ($(UNAME),Darwin)
	rpcgen -Cc datastore_args.x | sed -e 's/uint/u_int/g' > datastore_args.c
else
	rpcgen -Cc datastore_args.x > datastore_args.c
endif

datastore_args.h: datastore_args.x
	rm -f $@
	@echo "#include <Trios_config.h>" >> $@
ifeq ($(UNAME),Darwin)
	rpcgen -Ch datastore_args.x | sed -e 's/uint/u_int/g' >> $@
else
	rpcgen -Ch datastore_args.x | sed -e 's@rpc/rpc.h@rpc/xdr.h@' >> $@
endif

clean:
	rm -f *.o datastore_server test_client datastore_args.h datastore_args.c

cp:
	cp datastore_server test_client /gscratch2/gflofst/txn
