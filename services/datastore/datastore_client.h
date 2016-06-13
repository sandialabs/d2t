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

#ifndef DATASTORE_CLIENT_H
#define DATASTORE_CLIENT_H

#ifdef __cplusplus
extern "C"
{
#endif

struct ds_catalog_item
{
    uint64_t id;
    size_t len;
};

struct datastore_server;

#define MAXSTRLEN 255

/////////////////////
int datastore_init (const char * connection_str, struct datastore_server ** new_server);

int datastore_finalize (struct datastore_server * server, struct ds_config * config, bool kill_service);

int datastore_get_config_from_env (const char * env_var, struct ds_config * config);

int datastore_get_connect_string (struct datastore_server * server, int max_len, char * str);

/////////////////////
int datastore_put (struct datastore_server * server, uint64_t txid, uint64_t * id, size_t size, void * data);

int datastore_get (struct datastore_server * server, uint64_t txid, uint64_t id, size_t * size, void ** data);

int datastore_location_get (struct datastore_server * server, uint64_t txid, uint64_t id, const char * connection_str, size_t * size, void ** data);

int datastore_remove (struct datastore_server * server, uint64_t txid, uint64_t id);

int datastore_location_remove (struct datastore_server * server, const char * connection_str, uint64_t txid, uint64_t id);

/////////////////////
int datastore_count (struct datastore_server * server, uint64_t txid, uint32_t * count);

int datastore_catalog (struct datastore_server * server, uint64_t txid, uint32_t * count, struct ds_catalog_item ** items);

int datastore_obj_info (struct datastore_server * server, uint64_t txid, uint64_t id, size_t * size);

/////////////////////
int datastore_activate (struct datastore_server * server, uint64_t txid, uint64_t id);

int datastore_location_activate_var (struct datastore_server * server, uint64_t txid, uint64_t id, const char * connection_str);

#ifdef __cplusplus
}
#endif

#endif
