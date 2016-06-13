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

/*
 * Severely needs to be generalized so it does not strictly use MPI.
 * Currently, this is just hardcoded so we can do the CTH/Transactions
 * work on time. 
 */

//#define __STDC_FORMAT_MACROS
//#include <inttypes.h>
#include <stdint.h>
#include <assert.h>
#include <mpi.h>

#include "txn_client_mpi.h"
#include "txn_client.h"
#include "txn_transaction.h"

/* Ignore for now. */
int (*init_transaction_fxns[NUM_TRANSPORTS])(transaction*);
int (*begin_transaction_fxns[NUM_TRANSPORTS])(transaction*);
int (*vote_transaction_fxns[NUM_TRANSPORTS])(transaction*, txn_vote);
int (*commit_transaction_fxns[NUM_TRANSPORTS])(transaction*, txn_status);
int (*finalize_transaction_fxns[NUM_TRANSPORTS])(transaction*);

struct transaction_comm * 
txn_init(MPI_Comm comm, uint32_t num_groups)
{
    return txn_mpi_init (comm, num_groups);
}

struct transaction *
txn_create_transaction (struct transaction_comm * comm, uint64_t txid)
{
    struct transaction * txn = new transaction ();

    memset (txn, 0, sizeof (struct transaction));
    txn->status = TXN_STATUS_NEW;
    txn->state = TXN_STATE_IN_PROGRESS;
    memset (&txn->timestamp, 0, sizeof (struct txn_timestamp));
    txn->comm = comm;
    txn->tx_id = txid;
    return txn;
}

// 1. bcast the txid from the coordinator to everyone to ensure syned (optional)
// 2. gather # of sub txns from all subordinates to sub_coords
// 3. gather sub txn info from all subs to sub_coords
// 4. sort into a unique list at each sub_coord
// 5. gather # of sub txns from all sub_coords to coords
// 6. sort into a unique list at coord
uint32_t 
txn_begin_transaction(transaction * tx)
{
    int ret;

    tx->status = TXN_STATUS_IN_PROGRESS;

    mpi_tx * mpi = new mpi_tx();
    mpi->tx_id = tx->tx_id;
    mpi->status = tx->status;
    mpi->state = tx->state;

    // need to loop through each sub_transaction and create it mpi_sub_tx
    // out of it.
    ret = mpi_begin_transaction(tx, mpi);
    if (ret != TXN_SUCCESS)
        tx->status = TXN_STATUS_ABORTED;
    delete mpi;

    return ret;
}

uint32_t 
txn_vote_transaction(transaction * trans, uint32_t rank, txn_vote vote)
{
    for (int i = 0; vote != TXN_VOTE_ABORT && i < trans->num_sub_transactions; i++)
    {
        struct sub_transaction * sub = trans->sub_tx_list [i];
        if (   sub->vote == TXN_VOTE_UNKNOWN
            && (   sub->all_procs
                || (   !sub->all_procs
                    && sub->proc_id == rank
                    && sub->vote == TXN_VOTE_UNKNOWN
                   )
               )
           )
        {
            return TXN_INCOMPLETE_VOTING;
        }
        else
        {
            if (   sub->all_procs
                || (   !sub->all_procs
                    && sub->proc_id == rank
                   )
               )
                vote = sub->vote;
        }
    }

    uint32_t retval;
    txn_state old_state = trans->state;
    trans->status = TXN_STATUS_VOTING;

    mpi_tx * tx = new mpi_tx();
    tx->tx_id = trans->tx_id;
    // this gets overwritten for subordinates and subcoordinators
    tx->status = TXN_STATUS_VOTING;

    retval = mpi_vote_transaction(trans->comm, tx, vote);
    if (retval != TXN_SUCCESS)
    {
        tx->state = old_state;
    }
    else
    {
        tx->state = TXN_STATE_COMMIT_READY;
    }

    trans->status = tx->status;
    delete tx;

    return retval;
}

uint32_t
txn_commit_transaction(transaction * trans)
{
    uint32_t retval;
    txn_status old_status = trans->status;

    mpi_tx * tx = new mpi_tx();
    tx->tx_id = trans->tx_id;
    tx->status = TXN_STATUS_COMMITTING;

    retval = mpi_commit_transaction(trans->comm, tx);

    if(retval != TXN_SUCCESS)
    {
	trans->status = old_status;
    }
    else
    {
        trans->status = TXN_STATUS_COMMITTED;
    }

    delete tx;

    return retval;
}

uint32_t
txn_abort_transaction(transaction * trans)
{
    uint32_t retval;
    txn_status old_status = trans->status;

    trans->status = TXN_STATUS_ABORTING;

    mpi_tx * tx = new mpi_tx();
    tx->tx_id = trans->tx_id;
    tx->status = TXN_STATUS_ABORTING;
    tx->state = TXN_STATE_ABORT_READY;
    retval = mpi_abort_transaction(trans->comm, tx);

    if(retval != TXN_SUCCESS)
    {
	trans->status = old_status;
    }
    else
    {
        trans->status = TXN_STATUS_ABORTED;
    }

    delete tx;

    return retval;
}

uint32_t
txn_finalize_transaction(transaction * tx)
{
    uint32_t pass = TXN_SUCCESS;

    for (int i = 0; i < tx->num_sub_transactions; i++)
    {
        txn_finalize_sub_transaction (tx, tx->sub_tx_list [i]);
        delete tx->sub_tx_list [i];
    }
    // can't free this here because it is global for the comm and not this txn
    //if (tx->comm)
    //    delete tx->comm;
    delete tx;

    return pass;
}

txn_vote 
txn_vote_sub_transaction(transaction * trans, sub_transaction * sub_trans, txn_vote vote)
{
    sub_trans->vote = vote;
    bool found = false;

    for (int i = 0; i < trans->num_sub_transactions && !found; i++)
    {
        if (txn_compare_sub_transactions (sub_trans, trans->sub_tx_list [i]) == true)
        {
            found = true;
            trans->sub_tx_list [i]->vote = vote;
        }
    }

    if (found)
        return vote;
    else
        return TXN_VOTE_UNKNOWN;
}

txn_vote
txn_commit_sub_transaction(transaction * trans, sub_transaction * sub_trans)
{
    sub_trans->vote = TXN_VOTE_COMMIT;
    bool found = false;

    for (int i = 0; i < trans->num_sub_transactions && !found; i++)
    {
        if (txn_compare_sub_transactions (sub_trans, trans->sub_tx_list [i]) == true)
        {
            found = true;
            trans->sub_tx_list [i]->vote = TXN_VOTE_COMMIT;
        }
    }

    if (found)
        return TXN_VOTE_COMMIT;
    else
        return TXN_VOTE_UNKNOWN;
}

txn_vote
txn_abort_sub_transaction(transaction * trans, sub_transaction * sub_trans)
{
    sub_trans->vote = TXN_VOTE_ABORT;
    bool found = false;

    for (int i = 0; i < trans->num_sub_transactions && !found; i++)
    {
        if (txn_compare_sub_transactions (sub_trans, trans->sub_tx_list [i]) == true)
        {
            found = true;
            trans->sub_tx_list [i]->vote = TXN_VOTE_ABORT;
        }
    }

    if (found)
        return TXN_VOTE_ABORT;
    else
        return TXN_VOTE_UNKNOWN;
}

txn_res
txn_finalize_sub_transaction(transaction * trans, sub_transaction * sub_trans)
{
    txn_res res = TXN_SUCCESS;

    return res;
}

void
txn_finalize (struct transaction_comm * comm, bool shutdown_service)
{
    if (shutdown_service)
    {
        // kill the server, if it is used
    }

    txn_mpi_finalize (comm);
}

extern "C"
void txn_print_sub_transactions (transaction * trans, int rank) 
{
    printf ("Subtransactions (%d):\n", rank);
    for (int i = 0; i < trans->num_sub_transactions; i++)
    {
        printf ("\t[%d] %s(%s) {%llu} proc_id: %d state: %d vote: %s\n"
               ,rank
               ,trans->sub_tx_list [i]->name
               ,(trans->sub_tx_list [i]->all_procs ? "yes" : "no")
               ,(unsigned long long int) trans->sub_tx_list [i]
               ,trans->sub_tx_list [i]->proc_id
               ,trans->sub_tx_list [i]->state
               ,(trans->sub_tx_list [i]->vote == TXN_VOTE_UNKNOWN
                   ? "UNKNOWN"
                   : (trans->sub_tx_list [i]->vote == TXN_VOTE_COMMIT
                        ? "COMMIT" : "ABORT"
                     )
                )
               );
    }
}

// true or false for whether or not these are the same content
extern "C"
bool txn_compare_sub_transactions (const sub_transaction * sub1, const sub_transaction * sub2)
{
    bool rc;
assert (sub1);
assert (sub2);
#if 0
    printf ("\tleft %s(%s) proc_id: %d state: %d vote: %s\n"
           ,sub1->name
           ,(sub1->all_procs ? "yes" : "no")
           ,sub1->proc_id
           ,sub1->state
           ,(sub1->vote == TXN_VOTE_UNKNOWN
               ? "UNKNOWN"
               : (sub1->vote == TXN_VOTE_COMMIT
                    ? "COMMIT" : "ABORT"
                 )
            )
           );

    printf ("\tright %s(%s) proc_id: %d state: %d vote: %s\n"
           ,sub2->name
           ,(sub2->all_procs ? "yes" : "no")
           ,sub2->proc_id
           ,sub2->state
           ,(sub2->vote == TXN_VOTE_UNKNOWN
               ? "UNKNOWN"
               : (sub2->vote == TXN_VOTE_COMMIT
                    ? "COMMIT" : "ABORT"
                 )
            )
           );
#endif

    // ANY CHANGES HERE PROPOGATE TO txn_transaction.c:find_sub_tx
    rc = (!strncmp (sub1->name, sub2->name, TRANSACTION_NAME_SIZE)) ? true : false;
    if (rc)
        rc = (sub1->all_procs == sub2->all_procs) ? true : false;
    if (rc)
        if (sub1->all_procs == false) // will only get here is same so far
            rc = (sub1->proc_id == sub2->proc_id) ? true : false;

    return rc;
}

extern "C"
void txn_print_failed_ranks (transaction_comm * comm)
{
    txn_mpi_print_failed_ranks (comm);
}
