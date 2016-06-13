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

#ifndef METADATA_CLIENT_H
#define METADATA_CLIENT_H

#ifdef __cplusplus
extern "C"
{
#endif

#define MD_MAX_STR_LEN 255
#define MD_MAX_DIMS 10

struct md_dim_bounds
{
    uint32_t min;
    uint32_t max;
};

struct md_catalog_entry
{
    uint64_t var_id;
    char name [MD_MAX_STR_LEN];
    char path [MD_MAX_STR_LEN];
    uint32_t version;
    char type;
    uint32_t active;
    uint64_t txn_id;
    uint32_t num_dims;
    struct md_dim_bounds dim [MD_MAX_DIMS];
};

struct md_chunk_entry
{
    uint64_t chunk_id;
    uint64_t length_of_chunk;
    char connection [MD_MAX_STR_LEN];
    uint32_t num_dims;
    struct md_dim_bounds dim [MD_MAX_DIMS];
};

struct metadata_server;

int metadata_init (const char * connection_string, struct metadata_server ** new_server);
int metadata_finalize (struct metadata_server * server, struct md_config * config, bool kill_service);
int metadata_get_config_from_env (const char * env_var, struct md_config * config);

// create a variable in an inactive state. Chunks will be added later. Once the
// variable is complete (the transaction is ready to commit), it can then be
// activated to make it visible to other processes.
// For a scalar, num_dims should be 0 causing the other parameters
// to be ignored.
int metadata_create_var (struct metadata_server * server, uint64_t txid, uint64_t * var_id, const struct md_catalog_entry * new_var);

// insert a chunk of an array
int metadata_insert_chunk (struct metadata_server * server, uint64_t var_id, struct md_chunk_entry * new_chunk);

// delete all chunks (or just the scalar) associated with the specified var
int metadata_delete_var (struct metadata_server * server, uint64_t var_id, const char * name, const char * path, uint32_t version);

// retrieve the list of chunks that comprise a var.
int metadata_get_chunk_list (struct metadata_server * server, uint64_t txid, const char * name, const char * path, uint32_t version, uint32_t * items, struct md_chunk_entry ** chunks);

// retrieve a list of chunks of a var that have data within a specified range.
int metadata_get_chunk (struct metadata_server * server, uint64_t txid, struct md_catalog_entry * desired_box, uint32_t * items, struct md_chunk_entry ** matching_chunks);

// return a catalog of items in the metadata service. For scalars, the num_dims
// will be 0
int metadata_catalog (struct metadata_server * server, uint64_t txid, uint32_t * items, struct md_catalog_entry ** entries);

// mark a variable in the global_catalog as active making it visible to other
// processes.
int metadata_activate_var (struct metadata_server * server, uint64_t txid, const char * name, const char * path, uint32_t version);

// mark a variable in the global_catalog as in process making it invisible to
// other processes.
int metadata_processing_var (struct metadata_server * server, uint64_t txid, const char * name, const char * path, uint32_t version);

#ifdef __cplusplus
}
#endif

#endif
