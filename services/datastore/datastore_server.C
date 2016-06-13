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
#define __STDC_LIMIT_MACROS
#include <stdint.h>
#include <assert.h>
#include <time.h>
#include <iostream>
#include <string>
#include <map>

#include <mpi.h>

#include <Trios_config.h>
#include <Trios_nssi_server.h>
#include <Trios_logger.h>

#include "datastore_args.h"
#include "datastore_client.h"
#include "ds_config.h"

static void generate_contact_info (const char * env_var, const char * myid);
static nssi_service service;
static int datastore_server_init ();

// active values
// 0 = inactive
// 1 = active
// 2 = in process (hidden from outside transaction)
struct ds_item
{
    uint64_t txid;
    int active;
    size_t len;
    void * data;

    ds_item (uint64_t tx_id, size_t s, void * d) : txid (tx_id), len (s), data (d) {active = 0;}
    ds_item (ds_item & d) : txid (d.txid), active (d.active), len (d.len), data (d.data) {}
};

class DataStore
{
    public:
        DataStore () { srand (time (NULL));}
        ~DataStore ();

        // data pointer ownership is taken by this method
        int Put (uint64_t txid, uint64_t * id, size_t size, void * data);

        // data pointer is the value passed into the put but ownership retained
        int Get (uint64_t txid, uint64_t id, size_t * size, void ** data) const;

        // returns the size of the object
        int ObjInfo (uint64_t txid, uint64_t id, size_t * size) const;

        // delete is called on the data pointer
        int Remove (uint64_t txid, uint64_t id);

        // count of items in the store
        uint32_t GetCount (uint64_t txid) const;

        // returns a catalog of items in the catalog
        void Catalog (uint64_t txid, ds_catalog_item ** items) const;

        // prints to stdout the IDs in the catalog
        void PrintCatalog (uint64_t txid) const;

        // activate an object for a given transaction
        int Activate (uint64_t txid, uint64_t id);

    private:
        std::map<uint64_t, ds_item *> data;
} datastore;

int DataStore::Put (uint64_t txid, uint64_t * id, size_t size, void * obj)
{
    *id = rand () % UINT64_MAX;
    ds_item * item = new ds_item (txid, size, obj);

    data [*id] = item;

    return 1;
}

int DataStore::Get (uint64_t txid, uint64_t id, size_t * size, void ** obj) const
{
    std::map<uint64_t, ds_item *>::const_iterator it;

    it = data.find (id);
    if (it != data.end ())
    {
        struct ds_item * i = (struct ds_item *) it->second;
        if (i->txid == txid || i->active == 1)
        {
            *size = it->second->len;
            *obj = it->second->data;
            return RC_OK;
        }
    }

    *size = 0;
    *obj = 0;
    return RC_ERR;
}

int DataStore::ObjInfo (uint64_t txid, uint64_t id, size_t * s) const
{
    std::map<uint64_t, ds_item *>::const_iterator it;

    it = data.find (id);
    if (it != data.end ())
    {
        struct ds_item * i = (struct ds_item *) it->second;
        if (i->txid == txid || i->active == 1)
        {
            *s = it->second->len;
            return RC_OK;
        }
    }

    *s = 0;
    return RC_ERR;
}

int DataStore::Remove (uint64_t txid, uint64_t id)
{
    struct ds_item * d = (struct ds_item *) data [id];

fprintf (stderr, "%s %d Need to filter based on txid\n", __FILE__, __LINE__);
    if (d)
    {
        delete d;
    }

    data.erase (id);

    return 1;
}

uint32_t DataStore::GetCount (uint64_t txid) const
{
fprintf (stderr, "%s %d Need to filter based on txid\n", __FILE__, __LINE__);
    return data.size ();
}

void DataStore::Catalog (uint64_t txid, ds_catalog_item ** items) const
{
    int i = 0;
    uint32_t count = GetCount (txid);
    *items = new ds_catalog_item [count];
    std::map<uint64_t, ds_item *>::const_iterator it;

    for (i = 0, it = data.begin (); it != data.end (); it++, i++)
    {
fprintf (stderr, "%s %d Need to filter based on txid\n", __FILE__, __LINE__);
        (*items) [i].id = (*it).first;
        (*items) [i].len = (*it).second->len;
    }
}

void DataStore::PrintCatalog (uint64_t txid) const
{
    std::map<uint64_t, ds_item *>::const_iterator i;

    for (i = data.begin (); i  != data.end (); i++)
    {
        std::cout << "ID:" << (*i).first << " " << (*i).second->len << std::endl;
    }
}

int DataStore::Activate (uint64_t txid, uint64_t id)
{
    struct ds_item * d = (struct ds_item *) data [id];

    if (d)
    {
        if (d->txid == txid)
        {
            d->active = 1;
            return 1;
        }
        else
            return 0;
    }
    else
        return 0;
}

DataStore::~DataStore ()
{
    std::map<uint64_t, ds_item *>::const_iterator i;

    for (i = data.begin (); i  != data.end (); i++)
    {
        delete ((char *) (*i).second->data);
        delete (*i).second;
    }

    data.clear ();
}

// ============================================================================
int main (int argc, char ** argv)
{
    char my_url [NSSI_URL_LEN];
    int rc;

    if (argc < 2)
    {
        fprintf (stderr, "Usage: %s <env var name for contact info file>\n", argv [0]);
        fprintf (stderr, "\tDefault env var name: DATASTORE_CONFIG_FILE\n");

        return -1;
    }

    MPI_Init (&argc, &argv);

    logger_init ((log_level) atoi (getenv ("DS_SERVER_LOG_LEVEL")), NULL);

    nssi_rpc_init (NSSI_DEFAULT_TRANSPORT, NSSI_DEFAULT_ENCODE, NULL);

    nssi_get_url (NSSI_DEFAULT_TRANSPORT, my_url, NSSI_URL_LEN);
    generate_contact_info (argv [1], my_url);

    rc = nssi_service_init(NSSI_DEFAULT_TRANSPORT, NSSI_SHORT_REQUEST_SIZE, &service);
    if (rc != NSSI_OK) {
//        log_error(debug_level, "could not init xfer_svc: %s", nssi_err_str(rc));
        return -1;
    }

    rc = datastore_server_init ();

    /* start processing requests */
    service.max_reqs = -1;
    rc = nssi_service_start (&service);
    if (rc != NSSI_OK) {
        //log_info(netcdf_debug_level, "exited xfer_svc: %s", nssi_err_str(rc));
    }

    /* shutdown the xfer_svc */
    //log_debug(debug_level, "shutting down service library");
    nssi_service_fini (&service);

    nssi_rpc_fini (NSSI_DEFAULT_TRANSPORT);

    MPI_Finalize ();

    return 1;
}

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

//===========================================================================
int ds_put_stub (const unsigned long request_id
                ,const NNTI_peer_t *caller
                ,const ds_put_args *args
                ,const NNTI_buffer_t *data_addr
                ,const NNTI_buffer_t *res_addr
                )
{
    int rc = NSSI_OK;
    char * buf = NULL;
    size_t len = args->len; 
    ds_put_res res;

    buf = new char [len];

    /* Fetch the data from the client */
    rc = nssi_get_data(caller, buf, len, data_addr);
    if (rc != NSSI_OK) {
//        log_error(debug_level, "Could not fetch var data from client");
        goto cleanup;
    }

    datastore.Put (args->txid, &res.id, len, buf);

cleanup:
    /* we always send a result */
    rc = nssi_send_result(caller, request_id, rc, &res, res_addr);

    return rc;
}

int ds_get_stub (const unsigned long request_id
                ,const NNTI_peer_t *caller
                ,const ds_get_args *args
                ,const NNTI_buffer_t *data_addr
                ,const NNTI_buffer_t *res_addr
                )
{
    int rc = NSSI_OK;
    void * buf;
    size_t size;

    buf = 0;
    size = 0;
    rc = datastore.Get (args->txid, args->id, &size, &buf);

    if (rc == RC_OK)
    {
        rc = nssi_put_data (caller, buf, size, data_addr, -1);
        if (rc != NSSI_OK) {
//        log_error(debug_level, "Could not put var data on client");
            goto cleanup; 
	}
    }

cleanup:
    /* we always send a result */
    rc = nssi_send_result(caller, request_id, rc, NULL, res_addr);

    return rc;
}

int ds_remove_stub (const unsigned long request_id
                   ,const NNTI_peer_t *caller
                   ,const ds_remove_args *args
                   ,const NNTI_buffer_t *data_addr
                   ,const NNTI_buffer_t *res_addr
                   )
{
    int rc = NSSI_OK;

    datastore.Remove (args->txid, args->id);

//cleanup:
    /* we always send a result */
    rc = nssi_send_result(caller, request_id, rc, NULL, res_addr);

    return rc;
}

int ds_count_stub (const unsigned long request_id
                  ,const NNTI_peer_t *caller
                  ,const ds_count_args *args
                  ,const NNTI_buffer_t *data_addr
                  ,const NNTI_buffer_t *res_addr
                  )
{
    int rc;
    uint32_t count;

    count = datastore.GetCount (args->txid);

    rc = nssi_put_data (caller, &count, sizeof (uint32_t), data_addr, -1);

    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    return rc;
}

int ds_activate_stub (const unsigned long request_id
                     ,const NNTI_peer_t *caller
                     ,const ds_activate_args *args
                     ,const NNTI_buffer_t *data_addr
                     ,const NNTI_buffer_t *res_addr
                     )
{
    int rc = NSSI_OK;

    datastore.Activate (args->txid, args->id);

    rc = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    return rc;
}

int ds_catalog_stub (const unsigned long request_id
                    ,const NNTI_peer_t *caller
                    ,const ds_catalog_args *args
                    ,const NNTI_buffer_t *data_addr
                    ,const NNTI_buffer_t *res_addr
                    )
{
    int rc;
    uint32_t count;
    ds_catalog_item * items;

    count = datastore.GetCount (args->txid);
    datastore.Catalog (args->txid, &items);

    rc = nssi_put_data (caller, items, count * sizeof (ds_catalog_item), data_addr, -1);

//cleanup:
    /* we always send a result */
    rc = nssi_send_result(caller, request_id, rc, NULL, res_addr);

    delete items;

    return rc;
}

int ds_obj_info_stub (const unsigned long request_id
                     ,const NNTI_peer_t *caller
                     ,const ds_obj_info_args *args
                     ,const NNTI_buffer_t *data_addr
                     ,const NNTI_buffer_t *res_addr
                     )
{
    int rc = NSSI_OK;
    int rc2 = NSSI_OK;
    size_t s = 0;

    rc = datastore.ObjInfo (args->txid, args->id, &s);

    rc2 = nssi_put_data (caller, &s, sizeof (size_t), data_addr, -1);
    rc2 = nssi_send_result (caller, request_id, rc, NULL, res_addr);

    if (rc != NSSI_OK || rc2 != NSSI_OK)
    {
        rc = NSSI_ENOENT;
    }

    return rc;
}

//===========================================================================
static int datastore_server_init ()
{
    NSSI_REGISTER_SERVER_STUB(DS_PUT_OP, ds_put_stub, ds_put_args, ds_put_res);
    NSSI_REGISTER_SERVER_STUB(DS_GET_OP, ds_get_stub, ds_get_args, void);
    NSSI_REGISTER_SERVER_STUB(DS_REMOVE_OP, ds_remove_stub, ds_remove_args, void);
    NSSI_REGISTER_SERVER_STUB(DS_CATALOG_OP, ds_catalog_stub, ds_catalog_args, void);
    NSSI_REGISTER_SERVER_STUB(DS_OBJ_INFO_OP, ds_obj_info_stub, ds_obj_info_args, void);
    NSSI_REGISTER_SERVER_STUB(DS_COUNT_OP, ds_count_stub, void, void);
    NSSI_REGISTER_SERVER_STUB(DS_ACTIVATE_OP, ds_activate_stub, ds_activate_args, void);

    return 1;
}
