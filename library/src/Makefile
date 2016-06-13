UNAME := $(shell uname)
UNAME2 := $(shell uname -a | grep mz)

ifeq ($(UNAME),Darwin)
TRILINOS_ROOT=/Users/$(LOGNAME)/build/Trilinos
CC = mpic++
else
ifeq ($(UNAME2),)
TRILINOS_ROOT=/ascldap/users/$(LOGNAME)/Trilinos
CC = mpiCC
else
TRILINOS_ROOT=/home/$(LOGNAME)/projects/Trilinos
CC = CC
endif
endif

FLAGS = -Wall -g -DDEBUG 
LIBDIRS = $(HOME)/$(ARC)/lib/:.

all: client_lib

client_lib: txn_transaction.c txn_client.cpp txn_client_mpi.cpp txn_client_mpi.h
	$(CC) $(FLAGS) -c -I. txn_transaction.c txn_client.cpp txn_client_mpi.cpp
	ar rcs libtransaction_client.a txn_transaction.o txn_client.o txn_client_mpi.o
	ranlib libtransaction_client.a

clean:
	rm -f *.o libtransaction_client.a
