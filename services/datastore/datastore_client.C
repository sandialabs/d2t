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

#include "datastore_args.h"
#include "datastore_client.h"

#include "ds_config.h"

extern "C"
struct datastore_server
{
    nssi_service * service;
    const char * service_connect_string;
};

extern "C"
int datastore_get_config_from_env (const char * env_var, struct ds_config * config);

extern "C"
int datastore_put (struct datastore_server * server, uint64_t txid, uint64_t * id, size_t size, void * data)
{
    ds_put_res res;
    int rc;
    ds_put_args args;

    args.txid = txid;
    args.len = size;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,DS_PUT_OP  // opcode
                            ,&args      // args struct
                            ,data       // data pointer for bulk transfers
                            ,size       // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    *id = res.id;

    return RC_OK;
}

extern "C"
int datastore_get (struct datastore_server * server, uint64_t txid, uint64_t id, size_t * size, void ** data)
{
    int res;
    int rc;
    ds_get_args args;


//printf ("here\n");
    rc = datastore_obj_info (server, txid, id, size);
//printf ("got a size of %lld\n", *size);
    if (rc == NSSI_OK)
    {
//printf ("allocating buffer\n");
        args.txid = txid;
        args.id = id;
        *data = malloc (*size);
        rc = nssi_call_rpc_sync (server->service    // service
                                ,DS_GET_OP  // opcode
                                ,&args      // args struct
                                ,*data      // data pointer for bulk transfers
                                ,*size      // length of bulk transfer buffer
                                ,&res       // where to put result
                                );
//printf ("got my stuff\n");
    }
    else
    {
//printf ("got nothing\n");
        *size = 0;
        *data = NULL;
        rc = RC_ERR;
    }

    return rc;
}

extern "C"
int datastore_location_get (struct datastore_server * server, uint64_t txid, uint64_t id, const char * connection_str, size_t * size, void ** data)
{
    int res;
    int rc;
    nssi_service new_service;

    if (!strcmp (server->service_connect_string, connection_str))
    {
//printf ("Same service\n");
        return datastore_get (server, txid, id, size, data);
    }

    rc = nssi_get_service (NSSI_DEFAULT_TRANSPORT, connection_str, -1, &new_service);
    if (rc != NSSI_OK)
    {
        printf ("error: %d\n", rc);
    }

    ds_get_args get_args;
    ds_obj_info_args obj_info_args;

    obj_info_args.txid = txid;
    obj_info_args.id = id;

    rc = nssi_call_rpc_sync (&new_service    // service
                            ,DS_OBJ_INFO_OP  // opcode
                            ,&obj_info_args
                            ,size       // data pointer for bulk transfers
                            ,sizeof (size_t) // length of bulk transfer buffer
                            ,&res       // where to put result
                            );
//printf ("obj info size: %lld\n", *size);

    if (rc == NSSI_OK)
    {
//printf ("allocating buffer\n");
        get_args.txid = txid;
        get_args.id = id;
        *data = malloc (*size);
        rc = nssi_call_rpc_sync (&new_service    // service
                                ,DS_GET_OP  // opcode
                                ,&get_args      // args struct
                                ,*data      // data pointer for bulk transfers
                                ,*size      // length of bulk transfer buffer
                                ,&res       // where to put result
                                );
//printf ("got my stuff\n");
    }
    else
    {
//printf ("got nothing\n");
        *size = 0;
        *data = NULL;
        rc = RC_ERR;
    }

    // no need to kill the serivce since that will shut it down. Just the var
    // going out of scope is enough for the local cleanup
//    nssi_kill (new_service, 0, 5000);
    nssi_free_service (NSSI_DEFAULT_TRANSPORT, &new_service);

    return rc;
}

extern "C"
int datastore_remove (struct datastore_server * server, uint64_t txid, uint64_t id)
{
    int res;
    int rc;
    ds_remove_args args;

    args.txid = txid;
    args.id = id;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,DS_REMOVE_OP  // opcode
                            ,&args      // args struct
                            ,NULL       // data pointer for bulk transfers
                            ,0          // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    return rc;
}

extern "C"
int datastore_location_remove (struct datastore_server * server, const char * connection_str, uint64_t txid, uint64_t id)
{
    int res;
    int rc;
    nssi_service new_service;

//printf ("default: %s\n", server->service_connect_string);
//printf ("provided: %s\n", connection_str);
    if (!strcmp (server->service_connect_string, connection_str))
    {
//printf ("Same service\n");
        return datastore_remove (server, txid, id);
    }
//else { printf ("different service\n"); }

    rc = nssi_get_service (NSSI_DEFAULT_TRANSPORT, connection_str, -1, &new_service);
    if (rc != NSSI_OK)
    {
        printf ("error: %d\n", rc);
    }

    ds_remove_args args;

    args.txid = txid;
    args.id = id;

    rc = nssi_call_rpc_sync (&new_service    // service
                            ,DS_REMOVE_OP  // opcode
                            ,&args      // args struct
                            ,NULL       // data pointer for bulk transfers
                            ,0          // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    // no need to kill the serivce since that will shut it down. Just the var
    // going out of scope is enough for the local cleanup
//    nssi_kill (new_service, 0, 5000);
    nssi_free_service (NSSI_DEFAULT_TRANSPORT, &new_service);

    return rc;
}

extern "C"
int datastore_catalog (struct datastore_server * server, uint64_t txid, uint32_t * count, struct ds_catalog_item ** items)
{
    int res;
    int rc;
    int byte_size;
    ds_catalog_args args;

    datastore_count (server, txid, count);
//printf ("count of catalog items: %d\n", *count);

    args.txid = txid;
    args.count = *count;
    byte_size = sizeof (struct ds_catalog_item) * *count;
    *items = (struct ds_catalog_item *) malloc (byte_size);

    rc = nssi_call_rpc_sync (server->service    // service
                            ,DS_CATALOG_OP  // opcode
                            ,&args      // args struct
                            ,*items       // data pointer for bulk transfers
                            ,byte_size  // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    return rc;
}

extern "C"
int datastore_obj_info (struct datastore_server * server, uint64_t txid, uint64_t id, size_t * size)
{
    int res;
    int rc;

    ds_obj_info_args args;

    args.txid = txid;
    args.id = id;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,DS_OBJ_INFO_OP  // opcode
                            ,&args      // args struct
                            ,size       // data pointer for bulk transfers
                            ,sizeof (size_t) // length of bulk transfer buffer
                            ,&res       // where to put result
                            );
//printf ("obj info (%d) size: %ld\n", sizeof (size_t), *size);

    return rc;
}

extern "C"
int datastore_count (struct datastore_server * server, uint64_t txid, uint32_t * count)
{
    int res;
    int rc;
    struct ds_count_args args;

    args.txid = txid;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,DS_COUNT_OP  // opcode
                            ,&args      // args struct
                            ,count       // data pointer for bulk transfers
                            ,sizeof (uint32_t) // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    return RC_OK;
}

extern "C"
int datastore_activate (struct datastore_server * server, uint64_t txid, uint64_t id)
{
    int res;
    int rc;
    struct ds_activate_args args;

    args.txid = txid;
    args.id = id;

    rc = nssi_call_rpc_sync (server->service    // service
                            ,DS_ACTIVATE_OP  // opcode
                            ,&args      // args struct
                            ,NULL       // data pointer for bulk transfers
                            ,0          // length of bulk transfer buffer
                            ,&res       // where to put result
                            );

    return RC_OK;
}

extern "C"
int datastore_init (const char * connection_str, struct datastore_server ** new_server)
{
    ds_config config;
    int rc;

    nssi_rpc_init (NSSI_DEFAULT_TRANSPORT, NSSI_DEFAULT_ENCODE, NULL);

    NSSI_REGISTER_CLIENT_STUB(DS_PUT_OP, ds_put_args, void, ds_put_res);
    NSSI_REGISTER_CLIENT_STUB(DS_GET_OP, ds_get_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(DS_REMOVE_OP, ds_remove_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(DS_CATALOG_OP, ds_catalog_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(DS_OBJ_INFO_OP, ds_obj_info_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(DS_COUNT_OP, ds_count_args, void, int);
    NSSI_REGISTER_CLIENT_STUB(DS_ACTIVATE_OP, ds_activate_args, void, int);

    *new_server = (struct datastore_server *) malloc (sizeof (struct datastore_server));
    (*new_server)->service = (nssi_service *) malloc (sizeof (nssi_service));
    (*new_server)->service_connect_string = strdup (connection_str);

    rc = nssi_get_service (NSSI_DEFAULT_TRANSPORT, (*new_server)->service_connect_string, -1, (*new_server)->service);
    if (rc != NSSI_OK)
    {
        printf ("error: %d\n", rc);
    }

    return RC_OK;
}

extern "C"
int datastore_finalize (struct datastore_server * server, struct ds_config * config, bool kill_service)
{
    if (kill_service)
    {
        nssi_kill (server->service, 0, 5000);
    }
    nssi_free_service (NSSI_DEFAULT_TRANSPORT, server->service);
    free (server->service);
    free ((char *) server->service_connect_string);
    server->service = NULL;
    server->service_connect_string = NULL;
    free (server);
    for (int i = 0; i < config->num_servers; i++)
    {
        free (config->server_urls [i]);
    } 
    free (config->server_urls);

    return RC_OK;
}

extern "C"
int datastore_get_connect_string (struct datastore_server * server, int max_len, char * str)
{
    strncpy (str, server->service_connect_string, max_len);
    return RC_OK;
}

extern "C"
int datastore_get_config_from_env (const char * env_var, struct ds_config * config)
{
    int rc = 0;
    char * env_contact_file = getenv (env_var);

    memset (config, 0, sizeof(struct ds_config));

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
