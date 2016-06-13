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

#include "assert.h"

#include "txn_transaction.h"
#include "txn_client_mpi.h"

// this is from txn_client.h/implemented in txn_client.cpp
// true if content is the same otherwise false
#ifdef __cplusplus
extern "C"
#endif
bool
txn_compare_sub_transactions (const sub_transaction * sub1, const sub_transaction * sub2);

static sub_transaction * find_sub_tx(const char * name, bool all_procs, int source_proc_id, sub_transaction ** list, int list_size);

#if 0
static void set_timestamp_update_time(txn_timestamp * timestamp)
{
    timestamp->last_update_time = time(NULL);
    return;
}

static void set_timestamp_init_time(txn_timestamp * timestamp)
{
    timestamp->init_time = time(NULL);
    return;
}

static void set_timestamp_finalize_time(txn_timestamp * timestamp)
{
    timestamp->finalize_time = time(NULL);
    return;
}
#endif

#ifdef __cplusplus
extern "C"
#endif
sub_transaction * txn_create_sub_transaction(transaction * tx, const char * name, uint32_t proc_id)
{
    if (tx->status != TXN_STATUS_NEW)
    {
        fprintf (stderr, "Can only create single proc sub_txns before begin\n");

        return NULL;
    }

    sub_transaction * temp = find_sub_tx(name, false, proc_id, tx->sub_tx_list, tx->num_sub_transactions);

    if(!temp)
    {
        temp = new sub_transaction ();

        strncpy (temp->name, name, TRANSACTION_NAME_SIZE);
        memset (&temp->timestamp, 0, sizeof (struct txn_timestamp));
        temp->all_procs = false;
        temp->proc_id = proc_id;
        temp->state = TXN_STATE_IN_PROGRESS;
        temp->vote = TXN_VOTE_UNKNOWN;

	tx->sub_tx_list[tx->num_sub_transactions] = temp;
	tx->num_sub_transactions++;
    }

    return temp;
}

#ifdef __cplusplus
extern "C"
#endif
sub_transaction * txn_create_sub_transaction_all (transaction * tx, const char * name, uint32_t proc_id)
{
    sub_transaction * temp = find_sub_tx(name, true, proc_id, tx->sub_tx_list, tx->num_sub_transactions);

    if(!temp)
    {
        temp = new sub_transaction ();

        strncpy (temp->name, name, TRANSACTION_NAME_SIZE);
        memset (&temp->timestamp, 0, sizeof (struct txn_timestamp));
        temp->all_procs = true;
        temp->proc_id = proc_id;
        temp->state = TXN_STATE_IN_PROGRESS;
        temp->vote = TXN_VOTE_UNKNOWN;

	tx->sub_tx_list[tx->num_sub_transactions] = temp;
	tx->num_sub_transactions++;
    }
    else
    {
        assert (false); // code below does not work properly because of the
                        // assignment of temp into the sub_tx_list. Need to
                        // fix. For now, just assert false to kill app to
                        // prevent problems.
    }

    mpi_tx_response response;
    response.tx.tx_id = tx->tx_id;
    response.tx.status = TXN_STATUS_SUCCESS;
    response.num_sub_transactions = 1;
    memcpy (response.sub_tx, temp, sizeof (struct sub_transaction));
    int rc = mpi_gather_response (tx->comm, &response, TAG_NEW_SUB_TX);
//printf ("sub create gather rank: %d res: %d\n", proc_id, rc); fflush (stdout);

    mpi_tx t;
    t.tx_id = tx->tx_id;
    t.state = TXN_STATE_IN_PROGRESS;
    t.status = (rc == TXN_SUCCESS ? TXN_STATUS_SUCCESS : TXN_STATUS_PARTICIPANT_FAILURE);
    t.vote = TXN_VOTE_UNKNOWN;
    t.from_app_rank = proc_id;
    int rc2 = mpi_bcast_transaction (tx->comm, &t, TAG_BEGIN_SUB_TX);
//printf ("sub bcast transaction rank: %d res: %d\n", proc_id, rc2); fflush (stdout);

    if (rc != TXN_SUCCESS || rc2 != TXN_SUCCESS)
    {
        delete temp;
        temp = 0;
    }

    return temp;
}

/*
 * TODO: Figure out which one to use between find_sub_tx.. which parameters make more
 * sense based on where/when/how the function will be called.
 * ALSO: MAKE SURE THAT WE REALLY HANDLE ALL OF THE SUB_TXN ATTRIBUTES RATHER
 * THAN JUST NAME.
 */
#if 0
static
sub_transaction* get_sub_transaction(transaction * tx, char name[TRANSACTION_NAME_SIZE], bool all_procs, int source_proc_id)
{     
    return (find_sub_tx(name, all_procs, source_proc_id, tx->sub_tx_list, tx->num_sub_transactions));
}
#endif

static sub_transaction * find_sub_tx(const char * name, bool all_procs, int source_proc_id, sub_transaction ** list, int list_size)
{
    int i;
    assert (name);
    sub_transaction temp;
    strncpy (temp.name, name, TRANSACTION_NAME_SIZE);
    temp.all_procs = all_procs;
    temp.proc_id = source_proc_id;
    temp.state = TXN_STATE_IN_PROGRESS;
    temp.vote = TXN_VOTE_UNKNOWN;

    for(i = 0; i<list_size; i++)
    {
	//if (list [i] && !strncmp(name, list [i]->name, TRANSACTION_NAME_SIZE))
	if (txn_compare_sub_transactions (list [i], &temp) == true)
	    return list [i];
    }

    return NULL;
}
