UNAME := $(shell uname)
UNAME2 := $(shell uname -a | grep mz)

ifeq ($(UNAME),Darwin)
TRILINOS_ROOT=//Users/$(LOGNAME)/build/Trilinos
CC = mpic++
else
ifeq ($(UNAME2),)
TRILINOS_ROOT=$(HOME)/Trilinos
CC = mpiCC
else
TRILINOS_ROOT=/home/$(LOGNAME)/projects/trilinos/install/gnu
CC = CC
endif
endif

ifneq ($(UNAME2),)
NSSI_LIBDIR=-L${TRILINOS_ROOT}/lib \
-L../services/metadata \
-L../services/datastore \
-L../library/src \
-L.

NSSI_LIBS=-lmetadata_client -ldatastore_client -ltransaction_client -ltrios_nssi -ltrios_nnti -ltrios_support

NSSI_INCLUDE=-I${TRILINOS_ROOT}/include \
-I../services/metadata \
-I../services/datastore \
-I../library/src
else
NSSI_LIBDIR=-L${TRILINOS_ROOT}/cmake/packages/trios/libraries/support \
-L${TRILINOS_ROOT}/cmake/packages/trios/libraries/nessie/nssi \
-L${TRILINOS_ROOT}/cmake/packages/trios/libraries/nessie/nnti \
-L../services/metadata \
-L../services/datastore \
-L../library/src \
-L.

NSSI_LIBS=-lmetadata_client -ldatastore_client -ltransaction_client -ltrios_nssi -ltrios_nnti -ltrios_support

NSSI_INCLUDE=-I${TRILINOS_ROOT}/cmake/packages/trios \
-I${TRILINOS_ROOT}/packages/trios/libraries/nessie/client \
-I${TRILINOS_ROOT}/packages/trios/libraries/nessie/server \
-I${TRILINOS_ROOT}/packages/trios/libraries/nessie/nssi \
-I${TRILINOS_ROOT}/packages/trios/libraries/nessie/nnti \
-I${TRILINOS_ROOT}/packages/trios/libraries/support/xdr \
-I${TRILINOS_ROOT}/packages/trios/libraries/support/logger \
-I${TRILINOS_ROOT}/cmake/packages/trios/libraries/nessie/nssi \
-I${TRILINOS_ROOT}/cmake/packages/trios/libraries/nessie/nnti \
-I../services/metadata \
-I../services/datastore \
-I../library/src
endif

all: client analysis

client: write_test.c
	$(CC) -g write_test.c -o write_test ${NSSI_INCLUDE} ${NSSI_LIBDIR} ${NSSI_LIBS}

analysis: update_test.c
	$(CC) -g update_test.c -o update_test ${NSSI_INCLUDE} ${NSSI_LIBDIR} ${NSSI_LIBS}

clean:
	rm -f *.o write_test update_test

cp:
	cp write_test update_test /lscratch2/gflofst
