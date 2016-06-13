/* 
 * Copyright 2014 Sandia Corporation. Under the terms of Contract
 * DE-AC04-94AL85000, there is a non-exclusive license for use of this work by
 * or on behalf of the U.S. Government. Export of this program may require a
 * license from the United States Government.
 *
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Sandia Corporation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#ifndef _TXN_CLIENT_MPI_H
#define _TXN_CLIENT_MPI_H

#include <mpi.h>
#include "txn_transaction.h"
#include "txn_globals.h"
#include <stdlib.h>

// ROOT_COMM1 = 'world' comm (unused)
// ROOT_COMM2 = unused
// SUB_COMM1 = coordinator & sub_coordinators only
// SUB_COMM2 = subordinates and the local sub_coordinator
#define ROOT_COMM1 0
#define ROOT_COMM2 1
#define SUB_COMM1 2
#define SUB_COMM2 3

#define VOTE 0
#define INIT 1

/*
 * MPI Tags that the asynchronous collectives use.
 */
enum MSG_TAG
{
    TAG_BEGIN_TX = 6500,
    TAG_BCAST_SUB_TX_SIZE,
    TAG_BCAST_SUB_TX,
    TAG_NEW_SUB_TX,
    TAG_BEGIN_SUB_TX,
    TAG_BEGIN_WRITE,
    TAG_REQ_VOTE_VAR,
    TAG_VOTE_VAR,
    TAG_PRE_COMMIT,
    TAG_PRE_COMMIT_RESP,    
    TAG_FINALIZE_TX,
    TAG_COMMIT,
    TAG_ABORT,
    TAG_REQ_VOTE_TX,
    TAG_VOTE_TX,
    TAG_VOTE_TX_RESP,
    TAG_FAILED_RANK,

    TAG_PROCESS_FAILURE,
    TAG_NEW_SUB_COORDINATOR,
    TAG_NEW_COORDINATOR
};
typedef enum MSG_TAG MSG_TAG;

/*
 * MPI library uses this to broadcast state updates to transactions.
 */
struct _mpi_tx
{
    int tx_id;
    txn_state state;
    txn_status status;    
    txn_vote vote;
    // no other purpose other than testing.
    // might use for reporting purposes later.
    int from_app_rank;
};
typedef struct _mpi_tx mpi_tx;

/*
 * Used to respond to your parent coordinator with your sub_transaction
 * information.  Used only once, when the sub-coordinators gather their
 * begin transaction responses from their subordinates.
 */
struct _mpi_tx_response
{
    mpi_tx tx;
    sub_transaction sub_tx[MAX_SUB_TRANSACTIONS];
    int num_sub_transactions;   
};
typedef struct _mpi_tx_response mpi_tx_response;
//=============================================================================
/*
 * This function initializes MPI for the transaction layer.
 * This is completely separate from the application's MPI.
 * Sets up the various groups, colors, and communicators.
 */
struct transaction_comm * txn_mpi_init(MPI_Comm, int);

/* cleanup what we allocated */
void txn_mpi_finalize (struct transaction_comm * comm);

/*
 * Begins a transaction.  Goes up and down the tree and returns a
 * success or failure code. 
 */
int mpi_begin_transaction(struct transaction * txn, mpi_tx * tx);

/*
 * Votes on a specific transaction.  Will determine if all
 * of the sub_transactions have completed their required
 * operations or not.  Requires two complete trips up
 * and down the tree.  One two init and gather the responses
 * and the 2nd will disperse the coordinators decision.
 */
int mpi_vote_transaction(struct transaction_comm * comms, mpi_tx*, txn_vote);

/*
 * Commits a transaction.  If there's an error here,
 * you can still abort the transaction.
 */
int mpi_commit_transaction(struct transaction_comm * comms, mpi_tx * tx);

/*
 * Aborts the transaction.
 */
int mpi_abort_transaction(struct transaction_comm * comms, mpi_tx * tx);

/*
 * Used to commit or abort a transaction based on the result
 * of the voting phase.  If all agree to commit, and no errors occurred, 
 * then STATUS will be COMMIT, else, it will be ABORT.
 */
int mpi_finalize_transaction(struct transaction_comm * comms, mpi_tx * tx, txn_vote THE_VOTE);

/*
 * Broadcasts a transaction from the root to via the
 * communicator specified at the comm_index.
 */
int mpi_bcast_transaction(struct transaction_comm * comms, mpi_tx*, MSG_TAG);

/*
 * Broadcasts a transaction from the root to via the
 * communicator specified at the comm_index.
 */
int mpi_bcast_sub_transactions (struct transaction * txn);

/*
 * TODO: have to generalize this, and gather_transactions so that 
 * both mpi_tx and mpi_tx_response; will require generalizing of
 * the associated reducer functions also.
 * I could also use function pointers to reducer operations, based on
 * the type, but we risk compile time type-safety. 
 */
int mpi_gather_response(struct transaction_comm * comms, mpi_tx_response*, MSG_TAG);

/*
 * Aggregates the transactions responses from a node's children.
 * Basically gathers the "OK" or abort after the init.
 */
int mpi_gather_transaction(struct transaction_comm * comms, mpi_tx*, MSG_TAG);

/*
 * Gets all messages from the MPI unexpected messages buffer
 * of a given tag. Makes one iteration through.
 */
int mpi_get_all_tag(struct transaction_comm * comms, char*, int, int, int, MSG_TAG, int);

/*
 * Helper function that calls mpi_get_all_tag, but repeatedly
 * tries to get all messages
 * until all expected are recieved, or until a timeout period.
 */
int mpi_get_all_tag_timed(struct transaction_comm * comms, char*, int, int, int,
				 MSG_TAG, double, int*, int);

void txn_mpi_print_failed_ranks (struct transaction_comm * comm);
#endif
