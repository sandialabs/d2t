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

static void print_sub_txns (transaction * trans, int rank, int size)
{
#if DO_PRINT
    MPI_Barrier (MPI_COMM_WORLD);

    if (rank == 0)
    {
        char x;
        txn_print_sub_transactions (trans, rank);
        MPI_Send (&x, 1, MPI_CHAR, 1, 0, MPI_COMM_WORLD);
    }
    else
    {
        char x;
        MPI_Status status;

        MPI_Recv (&x, 1, MPI_CHAR, rank - 1, 0, MPI_COMM_WORLD, &status);
        txn_print_sub_transactions (trans, rank);

        if (rank != size - 1)
        {
            MPI_Send (&x, 1, MPI_CHAR, rank + 1, 0, MPI_COMM_WORLD);
        }
        else
        {
            printf ("==============\n"); fflush (stdout);
        }
    }
#endif
}

struct timeval_writer
{
    struct timeval t;  // time value
    int pid;            // process id
};

struct timing_metrics
{
    // all
    struct timeval t0;   // before txn_init
    struct timeval t0a;  // after txn_init
    struct timeval t1;   // before create_transaction
    struct timeval t2;   // after create_transaction

    // only rank 0
    struct timeval t3;   // before create_sub_txn 0
    struct timeval t4;   // after create_sub_txn 0
    struct timeval t5;   // before create_sub_txn 1
    struct timeval t6;   // after create_sub_txn 1
    struct timeval t7;   // before create_sub_txn 2
    struct timeval t8;   // after create_sub_txn 2
    struct timeval t9;   // before create_sub_txn 4
    struct timeval t10;  // after create_sub_txn 4
    struct timeval t11;  // before create_sub_txn 6
    struct timeval t12;  // after create_sub_txn 6
    struct timeval t13;  // before create_sub_txn 8
    struct timeval t14;  // after create_sub_txn 8

    // all
    struct timeval t15;  // before create_sub_txn_all 9
    struct timeval t16;  // after create_sub_txn_all 9
    struct timeval t17;  // before begin_txn
    struct timeval t18;  // after begin_txn

    // only rank 0
    struct timeval t19;  // before commit_sub_txn 0
    struct timeval t20;  // after commit_sub_txn 0

    // all
    struct timeval t21;  // before create_sub_txn_all 3
    struct timeval t22;  // after create_sub_txn_all 3
    struct timeval t23;  // before commit_sub_txn 3
    struct timeval t24;  // after commit_sub_txn 3
    struct timeval t25;  // before create_sub_txn_all 5
    struct timeval t26;  // after create_sub_txn_all 5
    struct timeval t27;  // before commit_sub_txn 5
    struct timeval t28;  // after commit_sub_txn 5
    struct timeval t29;  // before create_sub_txn_all 7
    struct timeval t30;  // after create_sub_txn_all 7
    struct timeval t31;  // before vote_transaction
    struct timeval t32;  // after vote_transaction
    struct timeval t33;  // before commit_transaction
    struct timeval t34;  // after commit_transaction
    struct timeval t35;  // before finalize_transaction
    struct timeval t36;  // after finalize_transaction
    struct timeval t37;  // before txn_finalize
    struct timeval t38;  // after txn_finalize
};

static struct timing_metrics timing;

// Subtract the `struct timeval' values X and Y,
// storing the result in RESULT.
// Return 1 if the difference is negative, otherwise 0.
static int timeval_subtract (struct timeval * result
                            ,struct timeval * x, struct timeval * y
                            )
{
    // Perform the carry for the later subtraction by updating y.
    if (x->tv_usec < y->tv_usec)
    {
        int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
        y->tv_usec -= 1000000 * nsec;
        y->tv_sec += nsec;
    }
    if (x->tv_usec - y->tv_usec > 1000000)
    {
        int nsec = (x->tv_usec - y->tv_usec) / 1000000;
        y->tv_usec += 1000000 * nsec;
        y->tv_sec -= nsec;
    }

    // Compute the time remaining to wait.
    // tv_usec is certainly positive.
    result->tv_sec = x->tv_sec - y->tv_sec;
    result->tv_usec = x->tv_usec - y->tv_usec;

    // Return 1 if result is negative.
    return x->tv_sec < y->tv_sec;
}

static void timeval_add (struct timeval * result
                        ,struct timeval * x, struct timeval * y
                        )
{
    result->tv_usec = x->tv_usec + y->tv_usec;
    result->tv_sec = x->tv_sec + y->tv_sec;
    if (result->tv_usec > 1000000)
    {
        result->tv_usec -= 1000000;
        result->tv_sec++;
    }
}

static
void print_metric (FILE * f, int rank, struct timing_metrics * t)
{
    struct timeval diff;

    fprintf (f, "%d", rank);

    timeval_subtract (&diff, &t->t0a, &t->t0);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t2, &t->t1);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t4, &t->t3);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t6, &t->t5);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t8, &t->t7);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t10, &t->t9);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t12, &t->t11);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t14, &t->t13);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t16, &t->t15);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t18, &t->t17);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t20, &t->t19);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t22, &t->t21);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t24, &t->t23);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t26, &t->t25);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t28, &t->t27);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t30, &t->t29);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t32, &t->t31);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t34, &t->t33);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t36, &t->t35);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    timeval_subtract (&diff, &t->t38, &t->t37);
    fprintf (f, "\t%02d.%06d", (int) diff.tv_sec, (int) diff.tv_usec);

    fprintf (f, "\n");

    fflush (f);
}

static
void print_metrics (MPI_Comm comm, int rank_to_fail)
{
    int rank;
    int size;

    MPI_Comm_rank (comm, &rank);
    MPI_Comm_size (comm, &size);

    MPI_Barrier (comm);

    if (rank == 0)
    {
        char fn [128];
        sprintf (fn, "%d_%d.metrics", size, rank_to_fail);
        FILE * f = fopen (fn, "w");
        struct timing_metrics * t;
        t = new timing_metrics [size];
        assert (t);
        memcpy (&t [0], &timing, sizeof (struct timing_metrics)); 
        MPI_Gather (NULL, 0, MPI_BYTE
                   ,t, sizeof (struct timing_metrics), MPI_BYTE
                   ,0, comm
                   );

        for (int i = 0; i < size; i++)
            print_metric (f, i, &t [i]);

        fclose (f);

        delete t;
    }
    else
    {
        MPI_Gather (&timing, sizeof (struct timing_metrics), MPI_BYTE
                   ,NULL, 0, MPI_BYTE
                   ,0, comm
                   );
    }

    MPI_Barrier (comm);
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
gettimeofday (&timing.t0, NULL);
    comm_id = txn_init(MPI_COMM_WORLD, num_groups);
gettimeofday (&timing.t0a, NULL);

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
gettimeofday (&timing.t1, NULL);
    mdtrans = txn_create_transaction (comm_id, txid);
gettimeofday (&timing.t2, NULL);

//============================================================================
    if (rank == 0)
    {
        // begin the transaction. Any add_sub_txn before this will be done as a
        // single action rather than individually

        // create a sub transaction for the metadata_catalog call
gettimeofday (&timing.t3, NULL);
        sub_txns [0].sub = txn_create_sub_transaction(mdtrans, sub_txns [0].name, rank);
gettimeofday (&timing.t4, NULL);
        assert (sub_txns [0].sub);

        // create a sub transaction for the metadata_get_chunk_list call
gettimeofday (&timing.t5, NULL);
        sub_txns [1].sub = txn_create_sub_transaction(mdtrans, sub_txns [1].name, rank);
gettimeofday (&timing.t6, NULL);
        assert (sub_txns [1].sub);

        // Creating sub transaction for metadata_processing_var call.
gettimeofday (&timing.t7, NULL);
        sub_txns [2].sub = txn_create_sub_transaction(mdtrans, sub_txns [2].name, rank);
gettimeofday (&timing.t8, NULL);
        assert (sub_txns [2].sub);

        // sub_txn [3] is _all

        // create the new var that we are writing
gettimeofday (&timing.t9, NULL);
        sub_txns [4].sub = txn_create_sub_transaction(mdtrans, sub_txns [4].name, rank);
gettimeofday (&timing.t10, NULL);
        assert (sub_txns [4].sub);

        // sub_txn [5] is _all

        // delete the var once we are done processing and already wrote
gettimeofday (&timing.t11, NULL);
        sub_txns [6].sub = txn_create_sub_transaction(mdtrans, sub_txns [6].name, rank);
gettimeofday (&timing.t12, NULL);
        assert (sub_txns [6].sub);

        // sub_txn [7] is _all

        // delete the var once we are done processing and already wrote
gettimeofday (&timing.t13, NULL);
        sub_txns [8].sub = txn_create_sub_transaction(mdtrans, sub_txns [8].name, rank);
gettimeofday (&timing.t14, NULL);
        assert (sub_txns [8].sub);
    }
    // make one global sub-txn before the begin transaction to see if that works
gettimeofday (&timing.t15, NULL);
    sub_txns [9].sub = txn_create_sub_transaction_all(mdtrans, sub_txns [9].name, rank);
gettimeofday (&timing.t16, NULL);
    assert (sub_txns [9].sub);
//============================================================================

//txn_print_sub_transactions (mdtrans, rank);
gettimeofday (&timing.t17, NULL);
    int pass = txn_begin_transaction(mdtrans);
gettimeofday (&timing.t18, NULL);
    assert (pass == TXN_SUCCESS);
//txn_print_sub_transactions (mdtrans, rank);

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
gettimeofday (&timing.t19, NULL);
            txn_commit_sub_transaction (mdtrans, sub_txns [0].sub);
gettimeofday (&timing.t20, NULL);
            vote = TXN_VOTE_COMMIT;
        }
        else
        {
            txn_abort_sub_transaction (mdtrans, sub_txns [0].sub);
            vote = TXN_VOTE_ABORT;
        }
//txn_print_sub_transactions (mdtrans, rank);

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

//printf ("rank: %d chunk_id: %lld size: %lld connection: %s\n", rank, chunks [0].chunk_id, chunk_size, chunks [0].connection); fflush (stdout);
    if (vote != TXN_VOTE_ABORT)
    {
gettimeofday (&timing.t21, NULL);
        sub_txns [3].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [3].name, rank);
gettimeofday (&timing.t22, NULL);
        assert (sub_txns [3].sub);
//txn_print_sub_transactions (mdtrans);
        PERFORM (rc = datastore_location_get (ds_server, txid, chunks [0].chunk_id, chunks [0].connection, &chunk_size, &chunk_data));
        if(rc == NSSI_OK)
        {
gettimeofday (&timing.t23, NULL);
            txn_commit_sub_transaction (mdtrans, sub_txns [3].sub);
gettimeofday (&timing.t24, NULL);
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
gettimeofday (&timing.t25, NULL);
    sub_txns [5].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [5].name, rank);
gettimeofday (&timing.t26, NULL);
}
if (rank_to_fail != -1)
print_metrics (MPI_COMM_WORLD, rank_to_fail);
if (rank != rank_to_fail)
{
//txn_print_failed_ranks (comm_id);
    assert (sub_txns [5].sub);
//printf ("success rank: %d\n", rank); fflush (stdout);
//txn_print_sub_transactions (mdtrans, rank);
    if (vote == TXN_VOTE_COMMIT)
    {
        PERFORM (rc = datastore_put (ds_server2, txid, &chunks [0].chunk_id, chunk_size, chunk_data));
        if(rc == NSSI_OK)
        {
gettimeofday (&timing.t27, NULL);
            txn_commit_sub_transaction (mdtrans, sub_txns [5].sub);
gettimeofday (&timing.t28, NULL);
        }
        else
        {
            txn_abort_sub_transaction (mdtrans, sub_txns [5].sub);
            vote = TXN_VOTE_ABORT;
        }
    }
//printf ("[5] %d\n", rank); fflush (stdout);

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
//printf ("[6] %d\n", rank); fflush (stdout);

gettimeofday (&timing.t29, NULL);
    sub_txns [7].sub = txn_create_sub_transaction_all (mdtrans, sub_txns [7].name, rank);
gettimeofday (&timing.t30, NULL);
    assert (sub_txns [7].sub);
//txn_print_sub_transactions (mdtrans);
    PERFORM (rc = datastore_location_remove (ds_server, chunks [0].connection, txid, chunks [0].chunk_id));
    if(rc == NSSI_OK)
    {
        txn_commit_sub_transaction (mdtrans, sub_txns [7].sub);
    }
    else
    {
        txn_abort_sub_transaction (mdtrans, sub_txns [7].sub);
    }
//printf ("[7] %d\n", rank); fflush (stdout);

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
//printf ("[8] %d\n", rank); fflush (stdout);
//goto cleanup;
    txn_commit_sub_transaction (mdtrans, sub_txns [9].sub);
//printf ("[9] %d\n", rank); fflush (stdout);
//goto cleanup;
//txn_print_sub_transactions (mdtrans, rank);

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
gettimeofday (&timing.t31, NULL);
    pass = txn_vote_transaction(mdtrans, rank, vote);
gettimeofday (&timing.t32, NULL);
//printf ("rank: %d pass: %d\n", rank, pass); fflush (stdout);
    if(pass == TXN_SUCCESS && vote == TXN_VOTE_COMMIT){
gettimeofday (&timing.t33, NULL);
        pass = txn_commit_transaction(mdtrans);
gettimeofday (&timing.t34, NULL);
    }else{
        pass = txn_abort_transaction(mdtrans);
//        printf ("aborting transaction\n"); fflush (stdout);
    }
    // if the protocol fails to commit after pass, then we have to abort.

gettimeofday (&timing.t35, NULL);
    txn_finalize_transaction (mdtrans);
gettimeofday (&timing.t36, NULL);

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
gettimeofday (&timing.t37, NULL);
    txn_finalize (comm_id, false);
gettimeofday (&timing.t38, NULL);
}
    print_metrics (MPI_COMM_WORLD, rank_to_fail);

cleanup:

    MPI_Finalize ();
}
