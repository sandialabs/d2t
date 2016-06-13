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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#include <mpi.h>

#if 0
#include <Trios_config.h>
#include <Trios_nssi_rpc.h>
#include <Trios_nssi_client.h>
#endif

#include "txn_client.h"

int main (int argc, char ** argv)
{
    // loop
    // 1. get the metadata catalog
    // 2. select oldest version of one var
    // 3. mark as in process in MD
    // 4. retrieve list of chunks for this var
    // 5. process
    // 6. write out new chunks
    // 7. make new MD entries
    // 8. delete old var version
    // end after x versions

    int size;
    int rank;

    MPI_Init (&argc, &argv);
    MPI_Comm_size (MPI_COMM_WORLD, &size);
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);

    // number of sub-coordinators
    uint32_t num_groups = 2;
    struct transaction_comm * comm_id; // which comm we initialized for transactions
    
    // global setup for this communicator. num_groups will probably fold into
    // itself shortly
    comm_id = txn_init(MPI_COMM_WORLD, num_groups);

    int rc;

    uint64_t txid = 10;
    txn_vote vote = TXN_VOTE_UNKNOWN;
    transaction * mdtrans = NULL;
    struct _subtxn
    {
        const char * name;
        sub_transaction * sub;
    } sub_txns [] = {{"metadata_catalog", NULL}   // 0
                    ,{"get_chunk_list",NULL}      // 1
                    ,{"processing_var",NULL}      // 2
                    ,{"datastore_get",NULL}       // 3
                    ,{"create_var",NULL}          // 4
                    ,{"datastore_put",NULL}       // 5
                    ,{"delete_var",NULL}          // 6
                    ,{"location_remove",NULL}     // 7
                    ,{"activate_var",NULL}        // 8
                    };
    
    // create a transaction.
    mdtrans = txn_create_transaction (comm_id, txid);

//============================================================================
    if (rank == 0)
    {
        // begin the transaction. Any add_sub_txn before this will be done as a
        // single action rather than individually

//txn_print_sub_transactions (mdtrans);
        // create a sub transaction for the metadata_catalog call
        sub_txns [0].sub = txn_create_sub_transaction(mdtrans, sub_txns [0].name, rank);
        assert (sub_txns [0].sub);
//txn_print_sub_transactions (mdtrans);

        // create a sub transaction for the metadata_get_chunk_list call
        sub_txns [1].sub = txn_create_sub_transaction(mdtrans, sub_txns [1].name, rank);
        assert (sub_txns [1].sub);
//txn_print_sub_transactions (mdtrans);

        // Creating sub transaction for metadata_processing_var call.
        sub_txns [2].sub = txn_create_sub_transaction(mdtrans, sub_txns [2].name, rank);
        assert (sub_txns [2].sub);
//txn_print_sub_transactions (mdtrans);

        // sub_txn [3] is _all

        // create the new var that we are writing
        sub_txns [4].sub = txn_create_sub_transaction(mdtrans, sub_txns [4].name, rank);
        assert (sub_txns [4].sub);
//txn_print_sub_transactions (mdtrans);

        // sub_txn [5] is _all

        // delete the var once we are done processing and already wrote
        sub_txns [6].sub = txn_create_sub_transaction(mdtrans, sub_txns [6].name, rank);
        assert (sub_txns [6].sub);
//txn_print_sub_transactions (mdtrans);

        // sub_txn [7] is _all

        // delete the var once we are done processing and already wrote
        sub_txns [8].sub = txn_create_sub_transaction(mdtrans, sub_txns [8].name, rank);
        assert (sub_txns [8].sub);
//txn_print_sub_transactions (mdtrans);
    }
//============================================================================

    int pass = txn_begin_transaction(mdtrans);
    assert (pass == TXN_SUCCESS);

    if(pass != TXN_SUCCESS)
    {
        txn_finalize (comm_id, true);
        mdtrans = NULL;
        fprintf(stderr, "transaction could not be initialized.\n");
        // do something to fail.
    }

    // get the first var at the root
    // get the list of chunks
    // scatter a chunk to each process for analysis routines
    // mark the var as 'in process' hiding it from others
    if (rank == 0)
    {
        rc = NSSI_OK;
        // if it succeeded, then we can commit.
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (sub_txns [0].sub);
            vote = TXN_VOTE_COMMIT;
        }
        else
        {
            txn_abort_sub_transaction (sub_txns [0].sub);
            vote = TXN_VOTE_ABORT;
        }

        if (strcmp (entries [0].name, var_name) == 0 && vote == TXN_VOTE_COMMIT)
        {
            // sub_txns [1]
            rc = NSSI_OK;
            // if it's successful, we can commit this.
            if(rc == NSSI_OK)
            {
                txn_commit_sub_transaction (sub_txns [1].sub);
            }
            else
            {
                txn_abort_sub_transaction (sub_txns [1].sub);
                vote = TXN_VOTE_ABORT;
            }
        }

        if (vote == TXN_VOTE_COMMIT)
        {
            // sub_txn [2]
            rc = NSSI_OK;
            if(rc == NSSI_OK)
            {
                txn_commit_sub_transaction (sub_txns [2].sub);
            }
            else
            {
                txn_abort_sub_transaction (sub_txns [2].sub);
                vote = TXN_VOTE_ABORT;
            }
        }
    }
    else
    {
        // nothing to do here
    }

    if (vote != TXN_VOTE_ABORT)
    {
        sub_txns [3].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [3].name, rank);
        assert (sub_txns [3].sub);
//txn_print_sub_transactions (mdtrans);
        rc = NSSI_OK;
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (sub_txns [3].sub);
            vote = TXN_VOTE_COMMIT;
        }
        else
        {
            txn_abort_sub_transaction (sub_txns [3].sub);
            vote = TXN_VOTE_ABORT;
        }
    }

    // write to the next ds and md service
    if (rank == 0)
    {
        rc = NSSI_OK;
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (sub_txns [4].sub);
        }
        else
        {
            txn_abort_sub_transaction (sub_txns [4].sub);
            vote = TXN_VOTE_ABORT;
        }
    }

    sub_txns [5].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [5].name, rank);
    assert (sub_txns [5].sub);
//txn_print_sub_transactions (mdtrans);
    if (vote == TXN_VOTE_COMMIT)
    {
        rc = NSSI_OK;
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (sub_txns [5].sub);
        }
        else
        {
            txn_abort_sub_transaction (sub_txns [5].sub);
            vote = TXN_VOTE_ABORT;
        }
    }

    // remove the chunks and md from the original set
    if (rank == 0)
    {
        rc = NSSI_OK;
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (sub_txns [6].sub);
        }
        else
        {
            txn_abort_sub_transaction (sub_txns [6].sub);
        }
    }

    sub_txns [7].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [7].name, rank);
    assert (sub_txns [7].sub);
//txn_print_sub_transactions (mdtrans);
    rc = NSSI_OK;
    if(rc == NSSI_OK)
    {
        txn_commit_sub_transaction (sub_txns [7].sub);
    }
    else
    {
        txn_abort_sub_transaction (sub_txns [7].sub);
    }

    if (rank == 0)
    {
        // activate the var in the new md
        rc = NSSI_OK;
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (sub_txns [8].sub);
        }
        else
        {
            txn_abort_sub_transaction (sub_txns [8].sub);
        }
    }
//txn_print_sub_transactions (mdtrans);

    free (chunks);

    // see where things are now
    free (entries);

    pass = txn_vote_transaction(mdtrans, vote);
    if(pass == TXN_SUCCESS && vote == TXN_VOTE_COMMIT){
        pass = txn_commit_transaction(mdtrans);
    }else{
        pass = txn_abort_transaction(mdtrans);
        printf ("aborting transaction\n");
    }
    // if the protocol fails to commit after pass, then we have to abort.

    txn_finalize_transaction (mdtrans);

    // no need to block before the finalizes because only the root will do
    // the actual shutdown.

    // cleans up all initiazed comms. assume no server or managed elsewhere
    txn_finalize (comm_id, false);

    // wait for everyone before shutting down MPI
    MPI_Barrier (MPI_COMM_WORLD);

    MPI_Finalize ();
}
