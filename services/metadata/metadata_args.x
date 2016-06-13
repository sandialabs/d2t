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

enum md_opcode
{
     MD_CREATE_VAR_OP = 100
    ,MD_INSERT_CHUNK_OP
    ,MD_DELETE_VAR_OP
    ,MD_GET_CHUNK_LIST_OP
    ,MD_GET_CHUNK_LIST_COUNT_OP
    ,MD_GET_CHUNK_OP
    ,MD_GET_CHUNK_COUNT_OP
    ,MD_CATALOG_OP
    ,MD_CATALOG_ENTRY_COUNT_OP
    ,MD_ACTIVATE_VAR_OP
    ,MD_PROCESSING_VAR_OP
/*    ,MD__OP */
};

const MAXSTRLEN = 255;

struct md_create_var_args
{
    uint64_t txid;
    /* uint64_t var_id is returned */
    string name<MAXSTRLEN>;
    string path<MAXSTRLEN>;
    char type;
    uint32_t var_version;
    uint32_t num_dims;
    /* struct {int32_t min, int32_t max} * dims is bulk data */
};

struct md_insert_chunk_args
{
    uint64_t var_id;
    uint64_t chunk_id;
    uint32_t num_dims;
    /* struct {int32_t min, int32_t max} * dims is bulk data */
    string connection<MAXSTRLEN>;
    uint64_t length_of_chunk;  /* use for retrieval of chunk from datastore */
};

struct md_delete_var_args
{
    uint64_t var_id;
    string name<MAXSTRLEN>;
    string path<MAXSTRLEN>;
    uint32_t var_version;
};

struct md_catalog_args
{
    uint64_t txid;
};

struct md_get_chunk_list_args
{
    uint64_t txid; /* if an in-process txn, must provide */
    string name<MAXSTRLEN>;
    string path<MAXSTRLEN>;
    uint32_t var_version;
    uint32_t num_dims;
    /* struct (int32_t min, int32_t max} * box dims is bulk data */
    /* catalog struct is return bulk data of the relevant chunks */
};

struct md_get_chunk_args
{
    uint64_t txid; /* if an in-process txn, must provide */
    uint64_t var_id;
    string name<MAXSTRLEN>;
    string path<MAXSTRLEN>;
    uint32_t var_version;
    uint32_t num_dims;
    /* struct (int32_t min, int32_t max} * box dims is bulk data */
    /* catalog struct is return bulk data of the relevant chunks */
};

struct md_activate_var_args
{
    uint64_t txid;
    string name<MAXSTRLEN>;
    string path<MAXSTRLEN>;
    uint32_t var_version;
};

struct md_processing_var_args
{
    uint64_t txid;
    string name<MAXSTRLEN>;
    string path<MAXSTRLEN>;
    uint32_t var_version;
};
