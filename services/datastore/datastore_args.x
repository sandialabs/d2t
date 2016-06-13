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

enum ds_opcode
{
     DS_PUT_OP = 1
    ,DS_GET_OP
    ,DS_REMOVE_OP
    ,DS_CATALOG_OP
    ,DS_COUNT_OP
    ,DS_OBJ_INFO_OP = 10
    ,DS_ACTIVATE_OP
    ,DS_INIT_OP
};

struct ds_put_args
{
    uint64_t txid;
    uint64_t len;  /* obj length. *//* Data sent in bulk parameters */
};

struct ds_put_res
{
    uint64_t id; /* generated id */
};

struct ds_get_args
{
    uint64_t txid;
    uint64_t id;  /* obj id. Data pulled in bulk parameters */
};

struct ds_remove_args
{
    uint64_t txid;
    uint64_t id; /* obj id */
};

struct ds_catalog_entry
{
    uint64_t txid;
    uint64_t id;   /* obj id */
    uint64_t len;  /* length of the object */
};

struct ds_catalog_args
{
    uint64_t txid;
    uint32_t count; /* count of entries. Entries list sent in bulk parameters */
};

struct ds_obj_info_args
{
    uint64_t txid;
    uint64_t id;  /* obj id */
};

struct ds_count_args
{
    uint64_t txid;
};

struct ds_activate_args
{
    uint64_t txid;
    uint64_t id;
};
