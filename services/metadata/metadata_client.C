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
#include <assert.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Trios_config.h>
#include <Trios_nssi_rpc.h>
#include <Trios_nssi_client.h>

#include "metadata_args.h"
#include "metadata_client.h"

#include "md_config.h"

extern "C"
struct metadata_server
{
    nssi_service * service;
    const char * service_connect_string;
};

extern "C"
int metadata_get_config_from_env (const char * env_var, struct md_config * config);

extern "C"
int metadata_init (const char * connect_str, struct metadata_server ** new_server)
{
    int rc;

    nssi_rpc_init (NSSI_DEFAULT_TRANSPORT, NSSI_DEFAULT_ENCODE, NULL);

    NSSI_REGISTER_CLIENT_STUB(MD_CREATE_VAR_OP, md_create_var_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_INSERT_CHUNK_OP, md_insert_chunk_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_DELETE_VAR_OP, md_delete_var_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_GET_CHUNK_LIST_OP, md_get_chunk_list_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_GET_CHUNK_LIST_COUNT_OP, md_get_chunk_list_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_GET_CHUNK_OP, md_get_chunk_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_GET_CHUNK_COUNT_OP, md_get_chunk_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_CATALOG_OP, void, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_CATALOG_ENTRY_COUNT_OP, void, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_ACTIVATE_VAR_OP, md_activate_var_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(MD_PROCESSING_VAR_OP, md_processing_var_args, void, int);

    *new_server = (struct metadata_server *) malloc (sizeof (struct metadata_server));
    (*new_server)->service = (nssi_service *) malloc (sizeof (nssi_service));
    (*new_server)->service_connect_string = strdup (connect_str);

    rc = nssi_get_service (NSSI_DEFAULT_TRANSPORT, (*new_server)->service_connect_string, -1, (*new_server)->service);
    if (rc != NSSI_OK)
    {
        printf ("error: %d\n", rc);
    }

    return RC_OK;
}

extern "C"
int metadata_finalize (struct metadata_server * server, struct md_config * config, bool kill_service)
{
    if (kill_service)
    {
        nssi_kill (server->service, 0, 5000);
    }
    nssi_free_service (NSSI_DEFAULT_TRANSPORT, server->service);
    free (server->service);
    free ((char *) server->service_connect_string);
    server->service = NULL;
    free (server);
    for (int i = 0; i < config->num_servers; i++)
    {
        free (config->server_urls [i]);
    }
    free (config->server_urls);

    return RC_OK;
}

// create a variable in an inactive state. Chunks will be added later. Once the
// variable is complete (the transaction is ready to commit), it can then be
// activated to make it visible to other processes.
// For a scalar, num_dims should be 0 causing the other parameters
// to be ignored.
extern "C"
int metadata_create_var (struct metadata_server * server, uint64_t txid
                        ,uint64_t * var_id
                        ,const struct md_catalog_entry * new_var
                        )
{
    int res;
    int rc;
    md_create_var_args args;
    struct md_dim_bounds * dims;
    size_t size;

    args.txid = txid;
    args.name = (char *) new_var->name;
    args.path = (char *) new_var->path;
    args.type = new_var->type;
    args.var_version = new_var->version;
    args.num_dims = new_var->num_dims;

    size = sizeof (struct md_dim_bounds) * new_var->num_dims;
    dims = (struct md_dim_bounds *) malloc (size);
    memcpy (dims, new_var->dim, size);

    rc = nssi_call_rpc_sync (server->service    // service
                            ,MD_CREATE_VAR_OP  // opcode
                            ,&args      // args struct
                            ,dims       // data pointer for bulk transfers
                            ,size       // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    *var_id = *(uint64_t *) dims; // this is the out; the other is the in
    free (dims);

    return RC_OK;
}

// insert a chunk of an array
extern "C"
int metadata_insert_chunk (struct metadata_server * server, uint64_t var_id, struct md_chunk_entry * new_chunk)
{
    int res;
    int rc;
    md_insert_chunk_args args;
    void * data = NULL;
    size_t size = 0;
    struct md_dim_bounds * dims;

    args.var_id = var_id;
    args.chunk_id = new_chunk->chunk_id;
    args.num_dims = new_chunk->num_dims;
    args.connection = (char *) new_chunk->connection;
    args.length_of_chunk = new_chunk->length_of_chunk;

    size = sizeof (struct md_dim_bounds) * new_chunk->num_dims;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,MD_INSERT_CHUNK_OP  // opcode
                            ,&args      // args struct
                            ,new_chunk->dim  // data pointer for bulk transfers
                            ,size       // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    return RC_OK;
}

// delete all chunks (or just the scalar) associated with the specified var
extern "C"
int metadata_delete_var (struct metadata_server * server, uint64_t var_id
                        ,const char * name
                        ,const char * path
                        ,uint32_t version
                        )
{
    int res;
    int rc;
    md_delete_var_args args;

    args.var_id = var_id;
    args.name = (char *) name;
    args.path = (char *) path;
    args.var_version = version;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,MD_DELETE_VAR_OP  // opcode
                            ,&args      // args struct
                            ,NULL       // data pointer for bulk transfers
                            ,0       // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    return rc;
}

// retrieve the list of chunks that comprise a var.
extern "C"
int metadata_get_chunk_list (struct metadata_server * server, uint64_t txid
                            ,const char * name
                            ,const char * path
                            ,uint32_t version
                            ,uint32_t * items
                            ,struct md_chunk_entry ** chunks
                            )
{
    int res;
    int rc;
    md_get_chunk_list_args args;
    size_t size = 0;

    args.txid = txid;
    args.name = (char *) name;
    args.path = (char *) path;
    args.var_version = version;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,MD_GET_CHUNK_LIST_COUNT_OP  // opcode
                            ,&args      // args struct
                            ,items       // data pointer for bulk transfers
                            ,sizeof (uint32_t) // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    if (*items > 0)
    {
        size = *items * sizeof (struct md_chunk_entry);
        *chunks = (struct md_chunk_entry *) malloc (size);
        rc = nssi_call_rpc_sync (server->service    // service
                                ,MD_GET_CHUNK_LIST_OP  // opcode
                                ,&args      // args struct
                                ,*chunks   // data pointer for bulk transfers
                                ,size       // length of bulk transfer buffer
                                ,&res       // where to put result
                                );
    }

    return RC_OK;
}

// retrieve a list of chunks of a var that have data within a specified range.
extern "C"
int metadata_get_chunk (struct metadata_server * server, uint64_t txid
                       ,struct md_catalog_entry * desired_box
                       ,uint32_t * items
                       ,struct md_chunk_entry ** matching_chunks
                       )
{
    int res;
    int rc;
    md_get_chunk_args args;
    struct md_dim_bounds * dims;
    size_t size = 0;

    args.txid = txid;
    args.var_id = desired_box->var_id;
    args.name = desired_box->name;
    args.path = desired_box->path;
    args.var_version = desired_box->version;
    args.num_dims = desired_box->num_dims;

    size = sizeof (struct md_dim_bounds) * desired_box->num_dims;
    dims = (struct md_dim_bounds *) malloc (size);
    for (int i = 0; i < desired_box->num_dims; i++)
    {
        dims [i].min = desired_box->dim [i].min;
        dims [i].max = desired_box->dim [i].max;
    }

    rc = nssi_call_rpc_sync (server->service    // service
                            ,MD_GET_CHUNK_COUNT_OP  // opcode
                            ,&args      // args struct
                            ,dims       // data pointer for bulk transfers
                            ,size       // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    *items = *(uint32_t *) dims;
    // refresh to make sure we are sending the right stuff again
    for (int i = 0; i < desired_box->num_dims; i++)
    {
        dims [i].min = desired_box->dim [i].min;
        dims [i].max = desired_box->dim [i].max;
    }

    if (*items > 0)
    {
        // need to offer the dims a second time, but still get back the
        // bulk data. So I just make a big enough buffer and reuse.
        size_t size2 = *items * sizeof (struct md_chunk_entry);
        size_t max_size = (size > size2 ? size : size2);
        *matching_chunks = (struct md_chunk_entry *) malloc (max_size);
        memcpy (*matching_chunks, dims, size);
        rc = nssi_call_rpc_sync (server->service    // service
                                ,MD_GET_CHUNK_OP  // opcode
                                ,&args      // args struct
                                ,*matching_chunks // data pointer for bulk transfers
                                ,max_size       // length of bulk transfer buffer
                                ,&res       // where to put result
                                );
    }

    free (dims);

    return RC_OK;
}

// return a catalog of items in the metadata service. For scalars, the num_dims
// will be 0
extern "C"
int metadata_catalog (struct metadata_server * server, uint64_t txn_id
                     ,uint32_t * items
                     ,struct md_catalog_entry ** entries
                     )
{
    int res;
    int rc;
    md_catalog_args args;
    size_t size = 0;
    *entries = NULL;

    args.txid = txn_id;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,MD_CATALOG_ENTRY_COUNT_OP  // opcode
                            ,&args      // args struct
                            ,items       // data pointer for bulk transfers
                            ,sizeof (uint32_t) // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    if (*items > 0)
    {
        size = sizeof (struct md_catalog_entry) * *items;
        *entries = (struct md_catalog_entry *) malloc (size);
        rc = nssi_call_rpc_sync (server->service    // service
                                ,MD_CATALOG_OP  // opcode
                                ,&args      // args struct
                                ,*entries  // data pointer for bulk transfers
                                ,size       // length of bulk transfer buffer
                                ,&res       // where to put result
                                );
    }

    return RC_OK;
}

// mark a variable in the global_catalog as active making it visible to other
// processes.
extern "C"
int metadata_activate_var (struct metadata_server * server, uint64_t txid
                          ,const char * name
                          ,const char * path
                          ,uint32_t version
                          )
{
    int res;
    int rc;
    md_activate_var_args args;
    void * data = NULL;
    size_t size = 0;

    args.txid = txid;
    args.name = (char *) name;
    args.path = (char *) path;
    args.var_version = version;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,MD_ACTIVATE_VAR_OP  // opcode
                            ,&args      // args struct
                            ,data       // data pointer for bulk transfers
                            ,size       // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    return RC_OK;
}

// mark a variable in the global_catalog as processing making it invisible to
// other processes.
extern "C"
int metadata_processing_var (struct metadata_server * server, uint64_t txid
                          ,const char * name
                          ,const char * path
                          ,uint32_t version
                          )
{
    int res;
    int rc;
    md_processing_var_args args;
    void * data = NULL;
    size_t size = 0;

    args.txid = txid;
    args.name = (char *) name;
    args.path = (char *) path;
    args.var_version = version;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,MD_PROCESSING_VAR_OP  // opcode
                            ,&args      // args struct
                            ,data       // data pointer for bulk transfers
                            ,size       // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    return RC_OK;
}

extern "C"
int metadata_get_config_from_env (const char * env_var, struct md_config * config)
{
    int rc = 0;
    char * env_contact_file = getenv (env_var);

    memset (config, 0, sizeof(struct md_config));

    if (env_contact_file != NULL)
    {
        int i;
        FILE *f=NULL;
        long  fsize=0, bytes_to_read=0, bytes_read=0;
        char *fbuf;
        int lcount;
        char *start, *end;

        struct stat sbuf;

        while (stat(env_contact_file, &sbuf))
            { perror(env_contact_file); sleep(1); }

        f=fopen(env_contact_file, "r");
        if (!f) {
            perror(env_contact_file);
        }
        else
        {
            fsize = sbuf.st_size;
            fbuf=(char *)calloc(fsize+1,1);

            bytes_to_read=fsize;

            while(bytes_to_read > 0) {
                bytes_read+=fread(fbuf+bytes_read, 1, bytes_to_read, f);
                if (bytes_read != fsize) {
                    //log_error(netcdf_debug_level, "bytes_read(%lld) != fsize(%lld)", (int64_t)bytes_read, (int64_t)fsize);
                }
                bytes_to_read=fsize-bytes_read;

                //log_debug(netcdf_debug_level, "fsize(%lld) bytes_read(%lld) bytes_to_read(%lld)", (int64_t)fsize, (int64_t)bytes_read, (int64_t)bytes_to_read);
            }

            fclose(f);

            lcount=0;
            start=end=fbuf;
            do {
                end=strchr(start, '\n');
                if (end!=NULL) {
                    lcount++;
                    start=end+1;
                }
            } while(end!=NULL);

            config->num_servers=lcount;

            //log_debug(netcdf_debug_level, "lcount(%lld)", (int64_t)lcount);

            config->server_urls = (char **)calloc(lcount, sizeof(char *));
            if (!config->server_urls) {
                //log_error(netcdf_debug_level, "could not allocate netcdf services");
                free (fbuf);
                return NSSI_ENOMEM;
            }
            for (i=0;i<lcount;i++) {
                config->server_urls[i] = (char *)calloc(NNTI_URL_LEN, sizeof(char));
                if (!config->server_urls[i]) {
                    //log_error(netcdf_debug_level, "could not allocate netcdf services");
                    free (fbuf);
                    return NSSI_ENOMEM;
                }
            }
            start=end=fbuf;
            for (i=0;i<lcount;i++) {
                end=strchr(start, '\n');
                if (end==NULL) {
                    //log_error(netcdf_debug_level, "URL #%d is missing");
                } else {
                    int url_len=end-start;
                    if (url_len==0) {
                        //log_error(netcdf_debug_level, "URL #%d is missing");
                    }
                    memcpy(config->server_urls[i], start, url_len);
                    config->server_urls[i][url_len]='\0';
                    //log_debug(netcdf_debug_level, "netcdf_server_url[%d]=%s", i, netcdf_cfg->netcdf_server_urls[i]);
                    start=end+1;
                }
            }
            free (fbuf);
        }
    }

    return(NSSI_OK);
}
