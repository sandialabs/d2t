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

#include <iostream>
#include <stdlib.h>
#include <assert.h>

#include <mpi.h>

#include <Trios_config.h>
#include <Trios_nssi_rpc.h>
#include <Trios_nssi_client.h>

#include "datastore_client.h"

#include "ds_config.h"

int main (int argc, char ** argv)
{
    uint64_t txid = 9999;
    uint64_t id;
    size_t size;
    double * data;
    int count = 10;
    int rc;

    struct ds_config config;
    struct datastore_server * ds_server;

    datastore_get_config_from_env ("DATASTORE_CONFIG_FILE", &config);
    datastore_init (config.server_urls [0], &ds_server);

//    nssi_service nssi_service;
//    ds_config config;

//    datastore_get_config_from_env (&config);

//    rc = nssi_get_service (NSSI_DEFAULT_TRANSPORT, config.server_urls [0], -1, &nssi_service);
//    if (rc != RC_OK)
//    {
//        printf ("error: %d\n", rc);
//    }

    id = 5;
    size = sizeof (double) * count;
    data = (double *) malloc (size);
    for (int i = 0; i < count; i++)
        data [i] = i;

printf ("1\n");
    rc = datastore_put (ds_server, txid, &id, size, (void *) data);
    assert (rc == RC_OK);

    free (data); data = NULL;

printf ("2\n");
    rc = datastore_get (ds_server, txid, id, &size, (void **) &data);
    assert (size == count * sizeof (double));
    assert (data);
    for (int i = 0; i < size / sizeof (double); i++)
        std::cout << "item " << i << " " << data [i] << std::endl;
    assert (rc == RC_OK);
    free (data);
printf ("3\n");
    rc = datastore_get (ds_server, txid, id + 1, &size, (void **) &data);
    assert (rc == RC_ERR);
printf ("4\n");

    struct ds_catalog_item * items;
    uint32_t item_count;
    rc = datastore_catalog (ds_server, txid, &item_count, &items);
printf ("5\n");

    for (int i = 0; i < item_count; i++)
    {
        std::cout << "ID: " << items [i].id << " " << "Len: " << items [i].len << std::endl;
    }

printf ("6\n");
    datastore_finalize (ds_server, &config, true);
printf ("7\n");

    return 1;
}
