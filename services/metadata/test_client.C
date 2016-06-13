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
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <mpi.h>

#include <Trios_config.h>
#include <Trios_nssi_rpc.h>
#include <Trios_nssi_client.h>

#include "metadata_client.h"

#include "md_config.h"

static void print_catalog (uint32_t items, struct md_catalog_entry * entries)
{
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
}

static void print_chunk_list (uint32_t items, struct md_chunk_entry * chunks)
{
    for (int i = 0; i < items; i++)
    {
        printf ("chunk id: %lld connection: %s length: %lld num_dims: %d\n", chunks [i].chunk_id, chunks [i].connection, chunks [i].length_of_chunk, chunks [i].num_dims);
        char v = 'x';
        for (int j = 0; j < chunks [i].num_dims; j++)
        {
            printf ("%c: (%d/%d) ", v, chunks [i].dim [j].min, chunks [i].dim [j].max);
            v++;
        }
        printf ("\n");
    }
}

int main (int argc, char ** argv)
{
    int rc;

    uint64_t txid = 5;
    int32_t mins [3];
    int32_t maxes [3];
    double * blob;
    const char * connection;
    uint64_t length;
    struct md_catalog_entry new_var;
    struct md_chunk_entry new_chunk;
    uint64_t var_id0;
    uint64_t var_id1;
    uint32_t version0 = 4;
    uint32_t version1 = 1;
    const char * name0 = "var0";
    const char * path0 = "/root/";
    const char * name1 = "var1";
    const char * path1 = "/";
    char type0 = 5;
    char type1 = 1;

    struct md_config metadata_config;
    struct metadata_server * md_server;

    metadata_get_config_from_env ("METADATA_CONFIG_FILE", &metadata_config);
    rc = metadata_init (metadata_config.server_urls [0], &md_server);

    strcpy (new_var.name, name0);
    strcpy (new_var.path, path0);
    new_var.version = version0;
    new_var.type = type0;
    new_var.num_dims = 2;
    new_var.dim [0].min = -10;
    new_var.dim [0].max = 10;
    new_var.dim [1].min = -10;
    new_var.dim [1].max = 10;
    rc = metadata_create_var (md_server, txid, &var_id0, &new_var);

    strcpy (new_var.name, name1);
    strcpy (new_var.path, path1);
    new_var.version = version1;
    new_var.type = type1;
    new_var.num_dims = 3;
    new_var.dim [0]. min = 1;
    new_var.dim [0]. max = 100;
    new_var.dim [1]. min = 1;
    new_var.dim [1]. max = 100;
    new_var.dim [2]. min = 1;
    new_var.dim [2]. max = 100;
    rc = metadata_create_var (md_server, txid, &var_id1, &new_var);

    strcpy (new_chunk.connection, "chunk 1 location");
    new_chunk.num_dims = 3;
    new_chunk.dim [0].min = 1;
    new_chunk.dim [0].max = 10;
    new_chunk.dim [1].min = 2;
    new_chunk.dim [1].max = 10;
    new_chunk.dim [2].min = 3;
    new_chunk.dim [2].max = 10;
    new_chunk.length_of_chunk = (new_chunk.dim [0].max - new_chunk.dim [0].min + 1)  * (new_chunk.dim [1].max - new_chunk.dim [1].min + 1)  * (new_chunk.dim [2].max - new_chunk.dim [2].min + 1);
    //blob = (double *) malloc (sizeof (double) * length);
    rc = metadata_insert_chunk (md_server, var_id1, &new_chunk);

    strcpy (new_chunk.connection, "chunk 2 location");
    new_chunk.num_dims = 3;
    new_chunk.dim [0].min = 11;
    new_chunk.dim [0].max = 20;
    new_chunk.dim [1].min = 4;
    new_chunk.dim [1].max = 10;
    new_chunk.dim [2].min = 5;
    new_chunk.dim [2].max = 10;
    new_chunk.length_of_chunk = (new_chunk.dim [0].max - new_chunk.dim [0].min + 1)  * (new_chunk.dim [1].max - new_chunk.dim [1].min + 1)  * (new_chunk.dim [2].max - new_chunk.dim [2].min + 1);
    //blob = (double *) malloc (sizeof (double) * length);
    rc = metadata_insert_chunk (md_server, var_id1, &new_chunk);

    strcpy (new_chunk.connection, "chunk 3 location");
    new_chunk.num_dims = 3;
    new_chunk.dim [0].min = 21;
    new_chunk.dim [0].max = 30;
    new_chunk.dim [1].min = 6;
    new_chunk.dim [1].max = 10;
    new_chunk.dim [2].min = 7;
    new_chunk.dim [2].max = 10;
    new_chunk.length_of_chunk = (new_chunk.dim [0].max - new_chunk.dim [0].min + 1)  * (new_chunk.dim [1].max - new_chunk.dim [1].min + 1)  * (new_chunk.dim [2].max - new_chunk.dim [2].min + 1);
    //blob = (double *) malloc (sizeof (double) * length);
    rc = metadata_insert_chunk (md_server, var_id1, &new_chunk);

    strcpy (new_chunk.connection, "chunk 4 location");
    new_chunk.num_dims = 3;
    new_chunk.dim [0].min = 41;
    new_chunk.dim [0].max = 50;
    new_chunk.dim [1].min = 8;
    new_chunk.dim [1].max = 10;
    new_chunk.dim [2].min = 9;
    new_chunk.dim [2].max = 10;
    new_chunk.length_of_chunk = (new_chunk.dim [0].max - new_chunk.dim [0].min + 1)  * (new_chunk.dim [1].max - new_chunk.dim [1].min + 1)  * (new_chunk.dim [2].max - new_chunk.dim [2].min + 1);
    //blob = (double *) malloc (sizeof (double) * length);
    rc = metadata_insert_chunk (md_server, var_id1, &new_chunk);

    strcpy (new_chunk.connection, "chunk 5 location");
    new_chunk.num_dims = 3;
    new_chunk.dim [0].min = 51;
    new_chunk.dim [0].max = 60;
    new_chunk.dim [1].min = 10;
    new_chunk.dim [1].max = 11;
    new_chunk.dim [2].min = 11;
    new_chunk.dim [2].max = 12;
    new_chunk.length_of_chunk = (new_chunk.dim [0].max - new_chunk.dim [0].min + 1)  * (new_chunk.dim [1].max - new_chunk.dim [1].min + 1)  * (new_chunk.dim [2].max - new_chunk.dim [2].min + 1);
    //blob = (double *) malloc (sizeof (double) * length);
    rc = metadata_insert_chunk (md_server, var_id1, &new_chunk);

//============================================================================
    uint32_t items = 0;
    struct md_catalog_entry * entries = NULL;

    rc = metadata_catalog (md_server, txid, &items, &entries);

    printf ("catalog: \n");
    print_catalog (items, entries);

    free (entries);

    struct md_chunk_entry * chunks = NULL;

    rc = metadata_get_chunk_list (md_server, txid, new_var.name, new_var.path, new_var.version, &items, &chunks);

    printf ("chunk list for varid: %lld txid: %lld name: %s path: %s version: %d (count: %d) \n", var_id1, txid, new_var.name, new_var.path, new_var.version, items);
    print_chunk_list (items, chunks);

    if (chunks) free (chunks);

    // search for chunks that have part of this range for the var
    new_var.var_id = var_id1;
    new_var.dim [0].min = 8;
    new_var.dim [0].max = 25;
    new_var.dim [1].min = 7;
    new_var.dim [1].max = 26;
    new_var.dim [2].min = 6;
    new_var.dim [2].max = 27;
    metadata_get_chunk (md_server, txid, &new_var, &items, &chunks);

    printf ("matching chunk list for varid: %lld txid: %lld name: %s path: %s version: %d (count: %d) \n", var_id1, txid, new_var.name, new_var.path, new_var.version, items);
    print_chunk_list (items, chunks);

    if (chunks) free (chunks);

    printf ("deleting var_id: %lld name: %s path: %s, version: %d\n", var_id0, name0, path0, version0);
    rc = metadata_delete_var (md_server, var_id0, name0, path0, version0);

    rc = metadata_catalog (md_server, txid, &items, &entries);

    printf ("catalog: \n");
    print_catalog (items, entries);

    free (entries);

    rc = metadata_get_chunk_list (md_server, txid, new_var.name, new_var.path, new_var.version, &items, &chunks);

    printf ("chunk list for varid: %lld txid: %lld name: %s path: %s version: %d (count: %d) \n", var_id1, txid, new_var.name, new_var.path, new_var.version, items);
    print_chunk_list (items, chunks);

    if (chunks) free (chunks);

    printf ("deleting var_id: %lld name: %s path: %s, version: %d\n", var_id1, name1, path1, version1);
    rc = metadata_delete_var (md_server, var_id1, name1, path1, version1);

    rc = metadata_catalog (md_server, txid, &items, &entries);

    printf ("catalog: \n");
    print_catalog (items, entries);

    free (entries);

    rc = metadata_finalize (md_server, &metadata_config, true);
    printf ("done with testing\n");

    return rc;
}
