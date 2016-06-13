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

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#include <Trios_config.h>
#include <Trios_nssi_rpc.h>
#include <Trios_nssi_client.h>
#include <Trios_logger.h>

#include "metadata_client.h"
#include "datastore_client.h"

#include "md_config.h"
#include "ds_config.h"

#include "txn_client.h"

#define DO_PRINT 0

#define DO_OPS 1

#if DO_OPS
#define PERFORM(a) a
#else
#define PERFORM(a)
#endif

static void print_catalog (uint32_t items, struct md_catalog_entry * entries)
{
#if DO_PRINT
    for (int i = 0; i < items; i++)
    {
        printf ("var_id: %lld name: %s path: %s version: %d num_dims: %d ", entries [i].var_id, entries [i].name, entries [i].path, entries [i].version, entries [i].num_dims); fflush (stdout);
        char v = 'x';
        for (int j = 0; j < entries [i].num_dims; j++)
        {
            printf ("%c: (%d/%d) ", v, entries [i].dim [j].min, entries [i].dim [j].max); fflush (stdout);
            v++;
        }
        printf ("\n"); fflush (stdout);
    }
#endif
}

static void print_chunks (uint32_t items, struct md_chunk_entry * chunks)
{
#if DO_PRINT
    for (int i = 0; i < items; i++)
    {
        printf ("chunk_id: %lld length_of_chunk: %lld, connection: %s num_dims: %d ", chunks [i].chunk_id, chunks [i].length_of_chunk, chunks [i].connection, chunks [i].num_dims); fflush (stdout);
        char v = 'x';
        for (int j = 0; j < chunks [i].num_dims; j++)
        {
            printf ("%c: (%d/%d) ", v, chunks [i].dim [j].min, chunks [i].dim [j].max); fflush (stdout);
            v++;
        }
        printf ("\n"); fflush (stdout);
    }
#endif
}

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

    int rank_to_fail = -1;
    if (argc > 1)
        rank_to_fail = atoi (argv [1]);

    memset (&timing, 0, sizeof (struct timing_metrics));

    MPI_Init (&argc, &argv);
    MPI_Comm_size (MPI_COMM_WORLD, &size);
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);

    // number of sub-coordinators (min 2, otherwise 256 procs per)
    uint32_t num_groups = (size < 512) ? 2 : size / 256;
    struct transaction_comm * comm_id; // which comm we initialized for transactions
    
    struct md_config metadata_config;
    struct metadata_server * md_server;
    struct md_config metadata_config2;
    struct metadata_server * md_server2;

    struct ds_config datastore_config;
    struct datastore_server * ds_server;
    struct ds_config datastore_config2;
    struct datastore_server * ds_server2;

    // global setup for this communicator. num_groups will probably fold into
    // itself shortly
    comm_id = txn_init(MPI_COMM_WORLD, num_groups);

    logger_init ((log_level) atoi (getenv ("NSSI_LOG_LEVEL")), NULL);

    // for the sim to write to the first staging area
    PERFORM (metadata_get_config_from_env ("METADATA_CONFIG_FILE", &metadata_config));
    PERFORM (metadata_init (metadata_config.server_urls [0], &md_server));
    // for the analysis code to write to the next step analysis process
    PERFORM (metadata_get_config_from_env ("METADATA_CONFIG_FILE2", &metadata_config2));
    PERFORM (metadata_init (metadata_config2.server_urls [0], &md_server2));

    // for the sim to write to the first staging area
    PERFORM (datastore_get_config_from_env ("DATASTORE_CONFIG_FILE", &datastore_config));
    PERFORM (datastore_init (datastore_config.server_urls [0], &ds_server));
    // for the analysis code to write to the next step analysis process
    PERFORM (datastore_get_config_from_env ("DATASTORE_CONFIG_FILE2", &datastore_config2));
    PERFORM (datastore_init (datastore_config2.server_urls [0], &ds_server2));

    int rc = NSSI_OK;
    uint32_t catalog_items = 0;
    struct md_catalog_entry * entries = NULL;
    uint32_t chunk_items = 0;
    struct md_chunk_entry * chunks = NULL;
    uint32_t var_version = 0;
    uint64_t old_var_id = 0;
    uint64_t new_var_id = 0;
    const char * var_name = "A";
    const char * var_path = "/";

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
                    ,{"pre-begin-test",NULL}      // 9
                    };
    
    // create a transaction.
    mdtrans = txn_create_transaction (comm_id, txid);

//============================================================================
    if (rank == 0)
    {
        // begin the transaction. Any add_sub_txn before this will be done as a
        // single action rather than individually

        // create a sub transaction for the metadata_catalog call
        sub_txns [0].sub = txn_create_sub_transaction(mdtrans, sub_txns [0].name, rank);
        assert (sub_txns [0].sub);

        // create a sub transaction for the metadata_get_chunk_list call
        sub_txns [1].sub = txn_create_sub_transaction(mdtrans, sub_txns [1].name, rank);
        assert (sub_txns [1].sub);

        // Creating sub transaction for metadata_processing_var call.
        sub_txns [2].sub = txn_create_sub_transaction(mdtrans, sub_txns [2].name, rank);
        assert (sub_txns [2].sub);

        // sub_txn [3] is _all

        // create the new var that we are writing
        sub_txns [4].sub = txn_create_sub_transaction(mdtrans, sub_txns [4].name, rank);
        assert (sub_txns [4].sub);

        // sub_txn [5] is _all

        // delete the var once we are done processing and already wrote
        sub_txns [6].sub = txn_create_sub_transaction(mdtrans, sub_txns [6].name, rank);
        assert (sub_txns [6].sub);

        // sub_txn [7] is _all

        // delete the var once we are done processing and already wrote
        sub_txns [8].sub = txn_create_sub_transaction(mdtrans, sub_txns [8].name, rank);
        assert (sub_txns [8].sub);
    }
    // make one global sub-txn before the begin transaction to see if that works
    sub_txns [9].sub = txn_create_sub_transaction_all(mdtrans, sub_txns [9].name, rank);
    assert (sub_txns [9].sub);
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
        // sub_txns [0]
        PERFORM (rc = metadata_catalog (md_server, txid, &catalog_items, &entries));
        PERFORM (print_catalog (catalog_items, entries));
        // if it succeeded, then we can commit.
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (mdtrans, sub_txns [0].sub);
            vote = TXN_VOTE_COMMIT;
        }
        else
        {
            txn_abort_sub_transaction (mdtrans, sub_txns [0].sub);
            vote = TXN_VOTE_ABORT;
        }

#if DO_OPS
        if (strcmp (entries [0].name, var_name) == 0 && vote == TXN_VOTE_COMMIT)
        {
            var_version = entries [0].version;
            old_var_id = entries [0].var_id;

            // sub_txns [1]
            rc = metadata_get_chunk_list (md_server, txid, var_name, var_path, var_version, &chunk_items, &chunks);
            print_chunks (chunk_items, chunks);
            // if it's successful, we can commit this.
#endif
            if(rc == NSSI_OK)
            {
                txn_commit_sub_transaction (mdtrans, sub_txns [1].sub);
            }
            else
            {
                txn_abort_sub_transaction (mdtrans, sub_txns [1].sub);
                vote = TXN_VOTE_ABORT;
            }
#if DO_OPS
            MPI_Scatter (chunks, sizeof (struct md_chunk_entry), MPI_CHAR, MPI_IN_PLACE, sizeof (struct md_chunk_entry), MPI_CHAR, 0, MPI_COMM_WORLD);
        }
#endif

        if (vote == TXN_VOTE_COMMIT)
        {
            // sub_txn [2]
            PERFORM (rc = metadata_processing_var (md_server, txid, var_name, var_path, var_version));
            if(rc == NSSI_OK)
            {
                txn_commit_sub_transaction (mdtrans, sub_txns [2].sub);
            }
            else
            {
                txn_abort_sub_transaction (mdtrans, sub_txns [2].sub);
                vote = TXN_VOTE_ABORT;
            }
        }
    }
    else
    {
        chunks = (struct md_chunk_entry *) malloc (sizeof (struct md_chunk_entry));
#if DO_OPS
        MPI_Scatter (NULL, 0, MPI_CHAR, chunks, sizeof (struct md_chunk_entry), MPI_CHAR, 0, MPI_COMM_WORLD);
#endif
    }

    size_t chunk_size;
    void * chunk_data;

    if (vote != TXN_VOTE_ABORT)
    {
        sub_txns [3].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [3].name, rank);
        assert (sub_txns [3].sub);
        PERFORM (rc = datastore_location_get (ds_server, txid, chunks [0].chunk_id, chunks [0].connection, &chunk_size, &chunk_data));
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (mdtrans, sub_txns [3].sub);
            vote = TXN_VOTE_COMMIT;
        }
        else
        {
            txn_abort_sub_transaction (mdtrans, sub_txns [3].sub);
            vote = TXN_VOTE_ABORT;
        }
    }

    // process the chunks
    double * d = (double *) chunk_data;
    size_t chunk_item_count = chunk_size / sizeof (double);

#if DO_OPS
    for (int i = 0; i < chunk_item_count; i++)
    {
        d [i] *= rank; // just something stupid for now
    }
#endif

    // write to the next ds and md service
    if (rank == 0)
    {
        PERFORM (rc = metadata_create_var (md_server2, txid, &new_var_id, &entries [0]));
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (mdtrans, sub_txns [4].sub);
        }
        else
        {
            txn_abort_sub_transaction (mdtrans, sub_txns [4].sub);
            vote = TXN_VOTE_ABORT;
        }
        PERFORM (MPI_Bcast (&new_var_id, 1, MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD));
    }
    else
    {
        PERFORM (MPI_Bcast (&new_var_id, 1, MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD));
    }

if (rank != rank_to_fail)
{
    sub_txns [5].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [5].name, rank);
}
if (rank != rank_to_fail)
{
    assert (sub_txns [5].sub);
    if (vote == TXN_VOTE_COMMIT)
    {
        PERFORM (rc = datastore_put (ds_server2, txid, &chunks [0].chunk_id, chunk_size, chunk_data));
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (mdtrans, sub_txns [5].sub);
        }
        else
        {
            txn_abort_sub_transaction (mdtrans, sub_txns [5].sub);
            vote = TXN_VOTE_ABORT;
        }
    }

    PERFORM (metadata_insert_chunk (md_server2, new_var_id, &chunks [0]));

    // remove the chunks and md from the original set
    if (rank == 0)
    {
        PERFORM (rc = metadata_delete_var (md_server, old_var_id, var_name, var_path, var_version));
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (mdtrans, sub_txns [6].sub);
        }
        else
        {
            txn_abort_sub_transaction (mdtrans, sub_txns [6].sub);
        }
    }

    sub_txns [7].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [7].name, rank);
    assert (sub_txns [7].sub);
    PERFORM (rc = datastore_location_remove (ds_server, chunks [0].connection, txid, chunks [0].chunk_id));
    if(rc == NSSI_OK)
    {
        txn_commit_sub_transaction (mdtrans, sub_txns [7].sub);
    }
    else
    {
        txn_abort_sub_transaction (mdtrans, sub_txns [7].sub);
    }

    if (rank == 0)
    {
        // activate the var in the new md
        PERFORM (rc = metadata_activate_var (md_server2, txid, var_name, var_path, var_version));
        PERFORM (rc = datastore_activate (ds_server2, txid,  chunks [0].chunk_id));
        if(rc == NSSI_OK)
        {
            txn_commit_sub_transaction (mdtrans, sub_txns [8].sub);
        }
        else
        {
            txn_abort_sub_transaction (mdtrans, sub_txns [8].sub);
        }
    }
    txn_commit_sub_transaction (mdtrans, sub_txns [9].sub);

    if (chunks)
        free (chunks);

    // see where things are now
    if (entries)
        free (entries);

#if 0
switch (vote)
{
    case TXN_VOTE_COMMIT: printf ("rank: %d vote: TXN_VOTE_COMMIT\n", rank); fflush (stdout);
break;
    case TXN_VOTE_ABORT: printf ("rank: %d vote: TXN_VOTE_ABORT\n", rank); fflush (stdout);
break;
    case TXN_VOTE_UNKNOWN: printf ("rank: %d vote: TXN_VOTE_UNKNOWN\n", rank); fflush (stdout);
break;
}
#endif
    pass = txn_vote_transaction(mdtrans, rank, vote);
    if(pass == TXN_SUCCESS && vote == TXN_VOTE_COMMIT){
        pass = txn_commit_transaction(mdtrans);
    }else{
        pass = txn_abort_transaction(mdtrans);
    }
    // if the protocol fails to commit after pass, then we have to abort.

    txn_finalize_transaction (mdtrans);

    // no need to do this within a transaction because it is read only and
    // we do not need to read things in progress
    if (rank == 0)
    {
        printf ("Metadata catalog 1:\n"); fflush (stdout);
        PERFORM (rc = metadata_catalog (md_server, txid, &catalog_items, &entries));
        print_catalog (catalog_items, entries);
        if (entries)
            free (entries);

        printf ("Metadata catalog 2:\n"); fflush (stdout);
        PERFORM (rc = metadata_catalog (md_server2, txid, &catalog_items, &entries));
        print_catalog (catalog_items, entries);
        if (entries)
            free (entries);
    }

    // no need to block before the finalizes because only the root will do
    // the actual shutdown.

    // wait for everyone before shutting down MPI
    MPI_Barrier (MPI_COMM_WORLD);

    // clean up and shut down
    PERFORM (datastore_finalize (ds_server, &datastore_config, rank == 0 ? true : false));
    PERFORM (metadata_finalize (md_server, &metadata_config, rank == 0 ? true : false));
    PERFORM (datastore_finalize (ds_server2, &datastore_config2, rank == 0 ? true : false));
    PERFORM (metadata_finalize (md_server2, &metadata_config2, rank == 0 ? true : false));

    // cleans up all initiazed comms. assume no server or managed elsewhere
    txn_finalize (comm_id, false);
}

    MPI_Finalize ();
}
