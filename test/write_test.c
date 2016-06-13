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
        printf ("var_id: %lld name: %s path: %s version: %d num_dims: %d ", entries [i].var_id, entries [i].name, entries [i].path, entries [i].version, entries [i].num_dims);
        char v = 'x';
        for (int j = 0; j < entries [i].num_dims; j++)
        {
            printf ("%c: (%d/%d) ", v, entries [i].dim [j].min, entries [i].dim [j].max);
            v++;
        }
        printf ("\n");
    }
#endif
}

static void print_chunks (uint32_t items, struct md_chunk_entry * chunks)
{
#if DO_PRINT
    for (int i = 0; i < items; i++)
    {
        printf ("chunk_id: %lld length_of_chunk: %lld, connection: %s num_dims: %d ", chunks [i].chunk_id, chunks [i].length_of_chunk, chunks [i].connection, chunks [i].num_dims);
        char v = 'x';
        for (int j = 0; j < chunks [i].num_dims; j++)
        {
            printf ("%c: (%d/%d) ", v, chunks [i].dim [j].min, chunks [i].dim [j].max);
            v++;
        }
        printf ("\n");
    }
#endif
}

int main(int argc, char **argv)
{
  int i;
  int status;
  int  npx, npy, npz, ndx, ndy, ndz, nx, ny, nz;
  int  offx, offy, offz, posx, posy, posz;
  int  offsets[3], lsize[3], gsize[3];
  size_t cube_start[3];
  size_t cube_count[3];
  double *ddata=NULL;
  int rank;
  int nprocs;
  MPI_Comm comm = MPI_COMM_WORLD;
  double start_time, end_time, t_time,sz, gps;
    char filename [256];

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    sprintf (filename, "%s", argv [1]);

  npx = atoi(argv[2]);
  npy = atoi(argv[3]);
  npz = atoi(argv[4]);

  ndx = atoi(argv[5]);
  ndy = atoi(argv[6]);
  ndz = atoi(argv[7]);
  
  nx = npx * ndx;
  ny = npy * ndy;
  nz = npz * ndz;
  
  posx = rank%npx;
  posy = (rank/npx) %npy;
  posz = rank/(npx*npy);
  
  offx = posx * ndx;
  offy = posy * ndy;
  offz = posz * ndz;

  size_t local_data_size = sizeof (double) * ndx * ndy * ndz;
  ddata = (double *) malloc (local_data_size);

  for (i = 0; i < ndx * ndy * ndz; i++)
  {
    ddata [i] = rank;
  } 
  
  
  cube_start[0] = offx;
  cube_start[1] = offy;
  cube_start[2] = offz;
 
  cube_count[0] = ndx;
  cube_count[1] = ndy;
  cube_count[2] = ndz;
// I will need the number of procs divisible by 8

//printf ("%03d start(0) %3d start(1) %3d start(2) %3d size(0) %3d size(1) %3d size(2) %3d\n", rank, cube_start[0], cube_start[1], cube_start[2], cube_count[0], cube_count[1], cube_count[2]);

// start the timing now.

    uint32_t num_groups = 2;
    struct transaction_comm * comm_id;

    struct ds_config datastore_config;
    struct datastore_server * ds_server;
    struct md_config metadata_config;
    struct metadata_server * md_server;

    comm_id = txn_init (MPI_COMM_WORLD, num_groups);

    logger_init ((log_level) atoi (getenv ("NSSI_LOG_LEVEL")), NULL);
    PERFORM (datastore_get_config_from_env ("DATASTORE_CONFIG_FILE", &datastore_config));
    PERFORM (metadata_get_config_from_env ("METADATA_CONFIG_FILE", &metadata_config));
  
    PERFORM (datastore_init (datastore_config.server_urls [0], &ds_server));
    PERFORM (metadata_init (metadata_config.server_urls [0], &md_server));

     status = MPI_Barrier (MPI_COMM_WORLD);
     start_time = MPI_Wtime ();

    const int var_count = 10;
    uint64_t txid = 5;
    uint64_t var_id [var_count];
    uint64_t ds_obj_id [var_count];
    const char * path = "/";
    uint32_t version = 1;
    int rc = NSSI_OK;
    const int max_len = 255;
    char ds_connect [max_len];

    transaction * trans;

    trans = txn_create_transaction (comm_id, txid);

    int pass = txn_begin_transaction (trans);
    if (pass != TXN_SUCCESS)
    {
        txn_finalize (comm_id, true); // cleanup whatever is there and abort
        trans = NULL;
        fprintf (stderr, "transaction could not be initialized\n");
        // do something to fail.
    }

    PERFORM (datastore_get_connect_string (ds_server, max_len, ds_connect));

    // create the root var entries in the metadata service
    if (rank == 0)
    {
        char var_name [] = "A";
        for (int i = 0; i < var_count; i++)
        {
            struct md_catalog_entry entry;
            strcpy (entry.name, var_name);
            strcpy (entry.path, path);
            entry.version = version;
            entry.type = 1;
            entry.num_dims = 3;
            entry.dim [0].min = 0;
            entry.dim [0].max = nx - 1;
            entry.dim [1].min = 0;
            entry.dim [1].max = ny - 1;
            entry.dim [2].min = 0;
            entry.dim [2].max = nz - 1;
            PERFORM (rc = metadata_create_var (md_server, txid, &var_id [i], &entry));
            var_name [0]++;  // shift to next letter for next var
        }
    }
    // send the var_ids to everyone
    MPI_Bcast (var_id, var_count, MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD);

    // have each process create the local portion in the metadata service
    for (int i = 0; i < var_count; i++)
    {
        struct md_chunk_entry new_chunk;
        PERFORM (rc = datastore_put (ds_server, txid, &ds_obj_id [i], local_data_size, ddata));
        new_chunk.chunk_id = ds_obj_id [i];

        strcpy (new_chunk.connection, ds_connect);
        new_chunk.num_dims = 3;
        new_chunk.dim [0].min = cube_start [0];
        new_chunk.dim [0].max = cube_start [0] + cube_count [0] - 1;
        new_chunk.dim [1].min = cube_start [1];
        new_chunk.dim [1].max = cube_start [1] + cube_count [1] - 1;
        new_chunk.dim [2].min = cube_start [2];
        new_chunk.dim [2].max = cube_start [2] + cube_count [2] - 1;
        new_chunk.length_of_chunk = local_data_size;
//printf ("rank: %d var: %d chunk id: %ld x: (%d/%d) y (%d/%d) z (%d/%d)\n", rank, i, new_chunk.chunk_id, new_chunk.dim [0].min, new_chunk.dim [0].max, new_chunk.dim [1].min, new_chunk.dim [1].max, new_chunk.dim [2].min, new_chunk.dim [2].max);

        PERFORM (rc = metadata_insert_chunk (md_server, var_id [i], &new_chunk));
    }

    MPI_Barrier (MPI_COMM_WORLD); // represent the synchronization
    for (int i = 0; i < var_count; i++)
    {
        PERFORM (datastore_activate (ds_server, txid, ds_obj_id [i]));
    }
    if (rank == 0)
    {
        char var_name [] = "A";
        for (int i = 0; i < var_count; i++)
        {
            PERFORM (metadata_activate_var (md_server, txid, var_name, path, version));
            var_name [0]++;
        }
        var_name [0] = 'A';

        uint32_t items;
        struct md_catalog_entry * entries = NULL;
        struct md_chunk_entry * chunks = NULL;

        PERFORM (rc = metadata_catalog (md_server, txid, &items, &entries));

        PERFORM (print_catalog (items, entries));

        PERFORM (rc = metadata_get_chunk_list (md_server, txid, var_name, path, version, &items, &chunks));

        PERFORM (print_chunks (items, chunks));

        if (entries)
            free (entries);
        if (chunks)
            free (chunks);
    }

    txn_finalize_transaction (trans);
 
    status = MPI_Barrier (MPI_COMM_WORLD);

    end_time = MPI_Wtime ();

    PERFORM (datastore_finalize (ds_server, &datastore_config, false));
    PERFORM (metadata_finalize (md_server, &metadata_config, false));

    txn_finalize (comm_id, false);

    t_time = end_time - start_time;
    sz = (8.0 * 8.0 * nx * ny * ((double) nz)) / (1024.0 * 1024.0 * 1024.0);
    gps = sz / t_time;
    if (rank == 0)
        printf ("%s %d %d %d %d %lf %lf %lf\n", filename, nprocs, ndx, ndy, ndz, sz, t_time, gps);
    if (ddata)
        free (ddata);
    MPI_Finalize ();

    return 0;
}
