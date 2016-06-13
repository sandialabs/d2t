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

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#include <mpi.h>

#include <Trios_config.h>
#include <Trios_nssi_server.h>
#include <Trios_logger.h>

#include "metadata_args.h"
#include "metadata_client.h"
#include "md_config.h"

#include "sqlite3.h"

static sqlite3 * db = NULL;

static int callback (void * NotUsed, int argc, char ** argv, char ** ColName)
{
    int i;
    for (i = 0; i < argc; i++)
    {
        printf ("%s = %s\n", ColName [i], argv [i] ? argv [i] : "NULL");
    }
    printf ("\n");
    return 0;
}

static void generate_contact_info (const char * env_var, const char * myid);
static nssi_service service;
static int metadata_server_init ();
static int metadata_server_finalize ();

int md_create_var_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_create_var_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    int i;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    uint64_t var_id;
    struct md_dim_bounds * buf = NULL;
    size_t len = 0;

    len = args->num_dims * sizeof (struct md_dim_bounds);
    buf = (struct md_dim_bounds *) malloc (len);

    /* Fetch the data from the client */
    rc = nssi_get_data(caller, buf, len, data_addr);
    if (rc != NSSI_OK) {
//        log_error(debug_level, "Could not fetch var data from client");
        goto cleanup;
    }
    rc = sqlite3_prepare_v2 (db, "insert into global_catalog (id, name, path, version, active, txn_id, num_dims, d0_min, d0_max, d1_min, d1_max, d2_min, d2_max) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", -1, &stmt, &tail);
    if (rc != SQLITE_OK)
    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, sqlite3_errmsg (db));
        sqlite3_close (db);
        goto cleanup;
    }
    
    rc = sqlite3_bind_null (stmt, 1); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 2, strdup (args->name), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 3, strdup (args->path), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 4, args->var_version); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 5, 0); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 6, args->txid); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 7, args->num_dims); assert (rc == SQLITE_OK);
    for (i = 1; i <= args->num_dims; i++)
    {
        rc = sqlite3_bind_int (stmt, 7 + i * 2 - 1, buf [i - 1].min); assert (rc == SQLITE_OK);
        rc = sqlite3_bind_int (stmt, 7 + i * 2, buf [i - 1].max); assert (rc == SQLITE_OK);
    }

    rc = sqlite3_step (stmt); //assert (rc == SQLITE_OK || rc == SQLITE_DONE);
    if (rc != SQLITE_OK && rc != SQLITE_DONE)
    {
        fprintf (stderr, "Line: %d SQL error: %s (%d)\n", __LINE__, sqlite3_errmsg (db), rc);
        sqlite3_close (db);
        goto cleanup;
    }

    var_id = (int) sqlite3_last_insert_rowid (db);
//printf ("generated new global id:%d\n", *var_id);

    rc = sqlite3_finalize (stmt);

cleanup:
    rc = nssi_put_data (caller, &var_id, sizeof (uint64_t), data_addr, -1);

    free (buf);

    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    return rc;
}

int md_insert_chunk_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_insert_chunk_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    uint32_t count;
    int i;
    char * ErrMsg = NULL;
    sqlite3_stmt * stmt_index = NULL;
    const char * tail_index = NULL;
    int rowid;
    struct md_dim_bounds * buf = NULL;
    size_t len = 0;

    len = args->num_dims * sizeof (struct md_dim_bounds);
    buf = (struct md_dim_bounds *) malloc (len);

    /* Fetch the data from the client */
    rc = nssi_get_data (caller, buf, len, data_addr);
    if (rc != NSSI_OK) {
//        log_error(debug_level, "Could not fetch var data from client");
        goto cleanup;
    }

    rc = sqlite3_prepare_v2 (db, "insert into var_data (global_id, chunk_id, connection, length, d0_min, d0_max, d1_min, d1_max, d2_min, d2_max) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", -1, &stmt_index, &tail_index);
    if (rc != SQLITE_OK)
    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, sqlite3_errmsg (db));
        sqlite3_close (db);
        goto cleanup;
    }

    rc = sqlite3_bind_int (stmt_index, 1, args->var_id); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt_index, 2, args->chunk_id); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt_index, 3, strdup (args->connection), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt_index, 4, args->length_of_chunk); assert (rc == SQLITE_OK);
    for (i = 0; i < args->num_dims; i++)
    {
        rc = sqlite3_bind_int (stmt_index, 5 + i * 2, buf [i].min); assert (rc == SQLITE_OK);
        rc = sqlite3_bind_int (stmt_index, 6 + i * 2, buf [i].max); //assert (rc == SQLITE_OK);
        if (rc != SQLITE_OK)
        {
            fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, sqlite3_errmsg (db));
            sqlite3_close (db);
            goto cleanup;
        }
    }
    rc = sqlite3_step (stmt_index); assert (rc == SQLITE_OK || rc == SQLITE_DONE);

    rc = sqlite3_finalize (stmt_index);
//    rowid = sqlite3_last_insert_rowid (db);
//printf ("rowid for chunk: %d\n", rowid);

//printf ("insert chunk completed\n");

//    rc = nssi_put_data (caller, &count, sizeof (uint32_t), data_addr, -1);

cleanup:
    free (buf);

    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    return rc;
}

int md_delete_var_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_delete_var_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc = NSSI_OK;
    char * ErrMsg = NULL;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;

    const char * query1 = "delete from var_data where exists (select var_data.chunk_id from var_data, global_catalog where var_data.global_id = global_catalog.id and global_catalog.id = ? and global_catalog.name = ? and global_catalog.path = ? and global_catalog.version = ?) ";
    const char * query2 = "delete from global_catalog where global_catalog.id = ? and global_catalog.name = ? and global_catalog.path = ? and global_catalog.version = ? ";

    rc = sqlite3_exec (db, "begin;", callback, 0, &ErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, ErrMsg);
        sqlite3_free (ErrMsg);
        sqlite3_close (db);
        goto cleanup;
    }

    rc = sqlite3_prepare_v2 (db, query1, -1, &stmt, &tail);
    if (rc != SQLITE_OK)
    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, sqlite3_errmsg (db));
        sqlite3_close (db);
        goto cleanup;
    }

    rc = sqlite3_bind_int (stmt, 1, args->var_id); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 2, strdup (args->name), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 3, strdup (args->path), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 4, args->var_version); assert (rc == SQLITE_OK);
    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_DONE);

    rc = sqlite3_finalize (stmt);

    rc = sqlite3_prepare_v2 (db, query2, -1, &stmt, &tail);
    if (rc != SQLITE_OK)
    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, sqlite3_errmsg (db));
        sqlite3_close (db);
        goto cleanup;
    }

    rc = sqlite3_bind_int (stmt, 1, args->var_id); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 2, strdup (args->name), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 3, strdup (args->path), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 4, args->var_version); assert (rc == SQLITE_OK);
    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_DONE);

    rc = sqlite3_finalize (stmt);

    rc = sqlite3_exec (db, "end;", callback, 0, &ErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, ErrMsg);
        sqlite3_free (ErrMsg);
        sqlite3_close (db);
        goto cleanup;
    }

cleanup:
    if (rc != SQLITE_OK)
    {
        rc = sqlite3_exec (db, "rollback;", callback, 0, &ErrMsg);
    }
    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    return rc;
}

static int get_chunk_list_count (const md_get_chunk_list_args * args, uint32_t * count)
{
    int rc;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    const char * query = "select count (*) from var_data vd, global_catalog gc where gc.name = ? and gc.path = ? and gc.version = ? and gc.id = vd.global_id and (gc.txn_id = ? or gc.active = 1)";

    rc = sqlite3_prepare_v2 (db, query, -1, &stmt, &tail);
    if (rc != SQLITE_OK)
    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, sqlite3_errmsg (db));
        sqlite3_close (db);
        goto cleanup;
    }
    rc = sqlite3_bind_text (stmt, 1, strdup (args->name), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 2, strdup (args->path), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 3, args->var_version); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 4, args->txid); assert (rc == SQLITE_OK);

    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_ROW);
    *count = sqlite3_column_int (stmt, 0);
//printf ("database says %u chunks\n", *count);
    rc = sqlite3_finalize (stmt); assert (rc == SQLITE_OK);

cleanup:
    return rc;
}

int md_get_chunk_list_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_get_chunk_list_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    struct md_chunk_entry * entries = NULL;
    int size;
    uint32_t count;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    const char * query = "select vd.chunk_id, vd.length, vd.connection, gc.num_dims, vd.d0_min, vd.d0_max, vd.d1_min, vd.d1_max, vd.d2_min, vd.d2_max from var_data vd, global_catalog gc where gc.name = ? and gc.path = ? and gc.version = ? and gc.id = vd.global_id and (gc.txn_id = ? or gc.active = 1)";

    rc = get_chunk_list_count (args, &count);
//printf ("CL chunks: %d\n", count);

    if (count > 0)
    {
        size = sizeof (struct md_chunk_entry) * count;
        entries = (struct md_chunk_entry *) malloc (size);

        rc = sqlite3_prepare_v2 (db, query, -1, &stmt, &tail);
        if (rc != SQLITE_OK)
        {
            fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, sqlite3_errmsg (db));
            sqlite3_close (db);
            goto cleanup;
        }
        rc = sqlite3_bind_text (stmt, 1, strdup (args->name), -1, free); assert (rc == SQLITE_OK);
        rc = sqlite3_bind_text (stmt, 2, strdup (args->path), -1, free); assert (rc == SQLITE_OK);
        rc = sqlite3_bind_int (stmt, 3, args->var_version); assert (rc == SQLITE_OK);
        rc = sqlite3_bind_int (stmt, 4, args->txid); assert (rc == SQLITE_OK);
        rc = sqlite3_step (stmt);
//printf ("CL rc: %d\n", rc);

        int j = 0;
        while (rc == SQLITE_ROW)
        {
            int id;
//printf ("CL: entry: %d\n", j);

            entries [j].chunk_id = sqlite3_column_int (stmt, 0);
            entries [j].length_of_chunk = sqlite3_column_int (stmt, 1);
            strcpy (entries [j].connection, (char *) sqlite3_column_text (stmt, 2));
            entries [j].num_dims = sqlite3_column_int (stmt, 3);
//printf ("CL num_dims: %d\n", entries [j].num_dims);

//printf ("CL id: %d length: %ld connection: %s num_dims: %d ", id, entries [j].length_of_chunk, entries [j].connection, entries [j].num_dims);
            char v = 'x';
            int i = 0;
            while (i < entries [j].num_dims)
            {
                entries [j].dim [i].min = sqlite3_column_int (stmt, 4 + (i * 2));
                entries [j].dim [i].max = sqlite3_column_int (stmt, 5 + (i * 2));
//printf ("%c: (%d/%d) ", v++, entries [j].dim [i].min, entries [j].dim [i].max);
                i++;
            }
//printf ("\n");

            rc = sqlite3_step (stmt);
            j++;
        }
        rc = sqlite3_finalize (stmt); assert (rc == SQLITE_OK);
    }

cleanup:
    rc = nssi_put_data (caller, entries, size, data_addr, -1);

    rc = nssi_send_result (caller, request_id, NSSI_OK, NULL, res_addr);

    // need to wait until after send_result to do the free
    // need to free the connection strings too
    //if (entries) free (entries);
printf ("need to clean up this: %s %d\n", __FILE__, __LINE__);

    return rc;
}

int md_get_chunk_list_count_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_get_chunk_list_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    uint32_t count;

    rc = get_chunk_list_count (args, &count);

    rc = nssi_put_data (caller, &count, sizeof (uint32_t), data_addr, -1);
    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    return rc;
}

int md_catalog_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,struct md_catalog_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    uint32_t count;
    int i = 0;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    const char * query = "select id, name, path, version, active, txn_id, num_dims, d0_min, d0_max, d1_min, d1_max, d2_min, d2_max from global_catalog where txn_id = ? or active = 1 order by path, name, version";
    struct md_catalog_entry * data = NULL;
    size_t size = 0;

    rc = sqlite3_prepare_v2 (db, "select count (*) from global_catalog where txn_id = ? or active = 1", -1, &stmt, &tail); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 1, args->txid); assert (rc == SQLITE_OK);
    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_ROW);
    count = sqlite3_column_int (stmt, 0);
    rc = sqlite3_finalize (stmt); assert (rc == SQLITE_OK);

//printf ("rows in global_catalog: %d\n", count);
    size = sizeof (struct md_catalog_entry) * count;
    data = (struct md_catalog_entry *) malloc (size);
    memset (data, 0, size);

    rc = sqlite3_prepare_v2 (db, query, -1, &stmt, &tail); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 1, args->txid); assert (rc == SQLITE_OK);
    rc = sqlite3_step (stmt); assert (rc == SQLITE_ROW || rc == SQLITE_OK);

    while (rc == SQLITE_ROW)
    {
        int j = 0;
        int id;

        data [i].var_id = sqlite3_column_int (stmt, 0);
        strcpy (data [i].name, (char *) sqlite3_column_text (stmt, 1));
        strcpy (data [i].path, (char *) sqlite3_column_text (stmt, 2));
        data [i].version = sqlite3_column_int (stmt, 3);
        data [i].active = sqlite3_column_int (stmt, 4);
        data [i].txn_id = sqlite3_column_int (stmt, 5);
        data [i].num_dims = sqlite3_column_int (stmt, 6);

//printf ("id: %d name: %s path: %s version: %d num_dims: %d ", id, data [i].name, data [i].path, data [i].version, data [i].num_dims);
        char v = 'x';
        while (j < data [i].num_dims)
        {
            data [i].dim [j].min = sqlite3_column_int (stmt, 7 + (j * 2));
            data [i].dim [j].max = sqlite3_column_int (stmt, 8 + (j * 2));
//printf ("%c: (%d/%d) ", v++, data [i].dim [j].min, data [i].dim [j].max);
            j++;
        }
//printf ("\n");

        rc = sqlite3_step (stmt);
        i++;
    }

    rc = sqlite3_finalize (stmt);

cleanup:
    rc = nssi_put_data (caller, data, size, data_addr, -1);
    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);
    free (data);

    return rc;
}

static int get_matching_chunk_count (const md_get_chunk_args * args, const struct md_dim_bounds * dims, uint32_t * count)
{
    int rc;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    const char * query = "select count (*) from var_data vd, global_catalog gc where "
"gc.name = ? and gc.path = ? and gc.version = ? "
"and (gc.txn_id = ? or gc.active = 1) "
"and gc.id = vd.global_id "
"and ? <= vd.d0_max and ? >= vd.d0_min "
"and ? <= vd.d1_max and ? >= vd.d1_min "
"and ? <= vd.d2_max and ? >= vd.d2_min ";

    rc = sqlite3_prepare_v2 (db, query, -1, &stmt, &tail); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 1, strdup (args->name), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 2, strdup (args->path), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 3, args->var_version); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 4, args->txid); assert (rc == SQLITE_OK);
//printf ("looking for name: %s path: %s version: %d ", args->name, args->path, args->var_version);
    for (int j = 0; j < args->num_dims; j++)
    {
        rc = sqlite3_bind_int (stmt, 5 + (j * 2), dims [j].min);
        rc = sqlite3_bind_int (stmt, 6 + (j * 2), dims [j].max);
//printf ("(%d:%d) ", dims [j].min, dims [j].max);
    }
//printf ("\n");
    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_ROW);
    *count = sqlite3_column_int (stmt, 0);
    rc = sqlite3_finalize (stmt); assert (rc == SQLITE_OK);
//printf ("matching chunk count: %d\n", *count);

    return rc;
}

int md_get_chunk_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_get_chunk_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    uint32_t count;
    int i = 0;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    const char * query = "select vd.id, vd.connection, vd.length, gc.num_dims, vd.d0_min, vd.d0_max, vd.d1_min, vd.d1_max, vd.d2_min, vd.d2_max from var_data vd, global_catalog gc where "
"gc.name = ? and gc.path = ? and gc.version = ? "
"and (gc.txn_id = ? or gc.active = 1) "
"and gc.id = vd.global_id "
"and ? <= vd.d0_max and ? >= vd.d0_min "
"and ? <= vd.d1_max and ? >= vd.d1_min "
"and ? <= vd.d2_max and ? >= vd.d2_min ";
    int global_id;
    struct md_chunk_entry * chunk_list = NULL;
    size_t size = 0;
    struct md_dim_bounds * dims;

//args->var_id is available.
    size = args->num_dims * sizeof (struct md_dim_bounds);
    dims = (struct md_dim_bounds *) malloc (size);
    rc = nssi_get_data(caller, dims, size, data_addr);
    assert (rc == NSSI_OK);

    rc = get_matching_chunk_count (args, dims, &count);

    rc = sqlite3_prepare_v2 (db, query, -1, &stmt, &tail); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 1, strdup (args->name), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_text (stmt, 2, strdup (args->path), -1, free); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 3, args->var_version); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 4, args->txid); assert (rc == SQLITE_OK);
    for (int j = 0; j < args->num_dims; j++)
    {
        rc = sqlite3_bind_int (stmt, 5 + (j * 2), dims [j].min);
        rc = sqlite3_bind_int (stmt, 6 + (j * 2), dims [j].max);
    }
    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_ROW);

    size = count * sizeof (struct md_chunk_entry);
    chunk_list = (struct md_chunk_entry *) malloc (size);

    i = 0;
    while (rc == SQLITE_ROW)
    {
//printf ("writing item: %d\n", i);
        int id;

        chunk_list [i].chunk_id = sqlite3_column_int (stmt, 0);
        strcpy (chunk_list [i].connection, (char *) sqlite3_column_text (stmt, 1));
        chunk_list [i].length_of_chunk = sqlite3_column_int (stmt, 2);
        chunk_list [i].num_dims = sqlite3_column_int (stmt, 3);

//printf ("GC id: %d name: %s path: %s version: %d ", id, args->name, args->path, args->var_version);
        int j = 0;
        char v = 'x';
        while (j < chunk_list [i].num_dims)
        {
            chunk_list [i].dim [j].min = sqlite3_column_int (stmt, 4 + (j * 2));
            chunk_list [i].dim [j].max = sqlite3_column_int (stmt, 5 + (j * 2));
//printf ("%c: (%d/%d) ", v++, chunk_list [i].dim [j].min, chunk_list [i].dim [j].max);
            j++;
        }
//printf ("\n");

        rc = sqlite3_step (stmt);
        i++;
    }

    rc = sqlite3_finalize (stmt);

cleanup:
    free (dims);

    rc = nssi_put_data (caller, chunk_list, size, data_addr, -1);

    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    free (chunk_list);

    return rc;
}

int md_get_chunk_count_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_get_chunk_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    uint32_t count;
    size_t size = 0;
    struct md_dim_bounds * dims;

    size = args->num_dims * sizeof (struct md_dim_bounds);
    dims = (struct md_dim_bounds *) malloc (size);
    rc = nssi_get_data(caller, dims, size, data_addr);

    rc = get_matching_chunk_count (args, dims, &count);

cleanup:
    free (dims);

    rc = nssi_put_data (caller, &count, sizeof (uint32_t), data_addr, -1);

    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);
}

int md_catalog_entry_count_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_catalog_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    uint32_t count;

    rc = sqlite3_prepare_v2 (db, "select count (*) from global_catalog where txn_id = ? or active = 1", -1, &stmt, &tail); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 1, args->txid); assert (rc == SQLITE_OK);
    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_ROW);
    count = sqlite3_column_int (stmt, 0);
    rc = sqlite3_finalize (stmt); assert (rc == SQLITE_OK);

cleanup:
    rc = nssi_put_data (caller, &count, sizeof (uint32_t), data_addr, -1);

    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);
}

int md_activate_var_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_activate_var_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    const char * query = "update global_catalog set active = 1 where txn_id = ?";

    rc = sqlite3_prepare_v2 (db, query, -1, &stmt, &tail); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 1, args->txid); assert (rc == SQLITE_OK);
    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_ROW || rc == SQLITE_DONE);
        if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW)
        {
            fprintf (stderr, "Line: %d SQL error (%d): %s\n", __LINE__, rc, sqlite3_errmsg (db));
            sqlite3_close (db);
            goto cleanup;
        }
    rc = sqlite3_finalize (stmt); assert (rc == SQLITE_OK);

cleanup:
    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    return rc;
}

int md_processing_var_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const md_processing_var_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    sqlite3_stmt * stmt = NULL;
    const char * tail = NULL;
    const char * query = "update global_catalog set active = 2 where txn_id = ?";

    rc = sqlite3_prepare_v2 (db, query, -1, &stmt, &tail); assert (rc == SQLITE_OK);
    rc = sqlite3_bind_int (stmt, 1, args->txid); assert (rc == SQLITE_OK);
    rc = sqlite3_step (stmt); assert (rc == SQLITE_OK || rc == SQLITE_ROW || rc == SQLITE_DONE);
    rc = sqlite3_finalize (stmt); assert (rc == SQLITE_OK);

    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    return rc;
}

//===========================================================================
static int metadata_server_init ()
{
    NSSI_REGISTER_SERVER_STUB(MD_CREATE_VAR_OP, md_create_var_stub, md_create_var_args, void);
    NSSI_REGISTER_SERVER_STUB(MD_INSERT_CHUNK_OP, md_insert_chunk_stub, md_insert_chunk_args, void);
    NSSI_REGISTER_SERVER_STUB(MD_DELETE_VAR_OP, md_delete_var_stub, md_delete_var_args, void);
    NSSI_REGISTER_SERVER_STUB(MD_GET_CHUNK_LIST_OP, md_get_chunk_list_stub, md_get_chunk_list_args, void);
    NSSI_REGISTER_SERVER_STUB(MD_GET_CHUNK_LIST_COUNT_OP, md_get_chunk_list_count_stub, md_get_chunk_list_args, void);
    NSSI_REGISTER_SERVER_STUB(MD_GET_CHUNK_OP, md_get_chunk_stub, md_get_chunk_args, void);
    NSSI_REGISTER_SERVER_STUB(MD_GET_CHUNK_COUNT_OP, md_get_chunk_count_stub, md_get_chunk_args, void);
    NSSI_REGISTER_SERVER_STUB(MD_CATALOG_OP, md_catalog_stub, void, void);
    NSSI_REGISTER_SERVER_STUB(MD_CATALOG_ENTRY_COUNT_OP, md_catalog_entry_count_stub, void, void);
    NSSI_REGISTER_SERVER_STUB(MD_ACTIVATE_VAR_OP, md_activate_var_stub, md_activate_var_args, void);
    NSSI_REGISTER_SERVER_STUB(MD_PROCESSING_VAR_OP, md_processing_var_stub, md_processing_var_args, void);

    // ======================================
    //setup the database
    int rc;
    char * ErrMsg = NULL;
    
    rc = sqlite3_open (":memory:", &db);
    //rc = sqlite3_open ("db", &db);
    
    if (rc)
    {        fprintf (stderr, "Can't open database: %s\n", sqlite3_errmsg (db));        sqlite3_close (db);
        goto cleanup;
    }
    rc = sqlite3_exec (db, "create table global_catalog (id integer primary key autoincrement not null, name varchar (50), path varchar (50), version int, active int, txn_id int, num_dims int, d0_min int, d0_max int, d1_min int, d1_max int, d2_min int, d2_max int)", callback, 0, &ErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, ErrMsg);
        sqlite3_free (ErrMsg);
        sqlite3_close (db);
        goto cleanup;
    }

    rc = sqlite3_exec (db, "create table var_data (global_id int not null, chunk_id int not null, connection varchar(128), length int, d0_min int not null, d0_max int not null, d1_min int not null, d1_max int not null, d2_min int not null, d2_max int not null, primary key (global_id, d0_min, d0_max, d1_min, d1_max, d2_min, d2_max))", callback, 0, &ErrMsg);
    if (rc != SQLITE_OK)    {
        fprintf (stderr, "Line: %d SQL error: %s\n", __LINE__, ErrMsg);        sqlite3_free (ErrMsg);
        sqlite3_close (db);
        goto cleanup;
    }

cleanup:
    return rc;
    // ======================================
}

static int metadata_server_finalize ()
{
    sqlite3_close (db);
}

// ============================================================================
typedef char NNTI_url [NNTI_URL_LEN];

static void generate_contact_info (const char * env_var, const char * myid)
{
    NNTI_url *all_urls=NULL;
    int rank, np;
    char contact_path[1024];
    //log_level debug_level = netcdf_debug_level;
    //debug_level = LOG_ALL;

    //log_debug(netcdf_debug_level, "enter");

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    //log_debug(debug_level, "rank (%d)", rank);

    if (rank==0) {
        MPI_Comm_size(MPI_COMM_WORLD, &np);
        all_urls=(NNTI_url *)malloc(np*sizeof(NNTI_url));
    }
    MPI_Gather((char *) myid, sizeof(NNTI_url), MPI_BYTE,
               all_urls, sizeof(NNTI_url), MPI_BYTE,
               0, MPI_COMM_WORLD);
    if (rank==0) {
        char *contact_file=getenv(env_var);
        if (contact_file==NULL) {
            //log_error(debug_level, "NETCDF_CONTACT_INFO env var is undefined.");
            free(all_urls);
            return;
        }
//        sprintf(contact_path, "%s.%04d", contact_file, rank);
        sprintf(contact_path, "%s.tmp", contact_file);
        //log_debug(debug_level, "creating contact file (%s)", contact_path);
        FILE *f=fopen(contact_path, "w");
        if (f==NULL) {
            perror("fopen");
        }
        for (int i=0;i<np;i++) {
            fprintf(f, "%s\n",
                    all_urls[i]);
        }
//        fprintf(f, "%u@%u@%s@%u\n",
//                myid->nid, myid->pid,
//                myid->hostname, (unsigned int)ntohs(myid->port));
        fclose(f);
        rename(contact_path, contact_file);
        free(all_urls);
    }
    //log_debug(netcdf_debug_level, "exit");
}

int main (int argc, char ** argv)
{
    char my_url [NSSI_URL_LEN];
    int rc;

    if (argc < 2)
    {
        fprintf (stderr, "Usage: %s <env var to write config data>\n", argv [0]);
        fprintf (stderr, "\tDefault env var: METADATA_CONFIG_FILE\n");

        return -1;
    }

    MPI_Init (&argc, &argv);

    logger_init ((log_level) atoi (getenv ("MD_SERVER_LOG_LEVEL")), NULL);

    nssi_rpc_init (NSSI_DEFAULT_TRANSPORT, NSSI_DEFAULT_ENCODE, NULL);

    nssi_get_url (NSSI_DEFAULT_TRANSPORT, my_url, NSSI_URL_LEN);
    generate_contact_info (argv [1], my_url);

    rc = nssi_service_init (NSSI_DEFAULT_TRANSPORT, NSSI_SHORT_REQUEST_SIZE, &service);
    if (rc != NSSI_OK) {
//        log_error(debug_level, "could not init xfer_svc: %s", nssi_err_str(rc));
        return -1;
    }

    rc = metadata_server_init ();

    /* start processing requests */
    service.max_reqs = -1;
    rc = nssi_service_start (&service);
    if (rc != NSSI_OK) {
        //log_info(netcdf_debug_level, "exited xfer_svc: %s", nssi_err_str(rc));
    }

    metadata_server_finalize ();

    /* shutdown the xfer_svc */
    //log_debug(debug_level, "shutting down service library");
    nssi_service_fini (&service);

    nssi_rpc_fini (NSSI_DEFAULT_TRANSPORT);

    MPI_Finalize ();

    return 1;
}
