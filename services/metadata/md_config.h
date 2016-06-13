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

#ifndef MD_CONFIG_H
#define MD_CONFIG_H

// Database schema:
//
// global_catalog
//    id integer primary key  -- links to var_data.global_id
//    name varchar (50)
//    path varchar (50)
//    version int
//    x_min int  -- these are the global dimensions
//    x_max int
//    y_min int
//    y_max int
//    z_min int
//    z_max int
//    active int  -- 0 or 1 for active or not; 2 means hide, being processed
//    txn_id int  -- txn part of to override the active flag hiding the var
//
// var_data
//    id integer primary key  -- links to chunk_index.id
//    global_id int           -- links to global_catalog.id
//    connection varchar (128)
//    length int              -- length of the assoc. blob for easier retrieval
//
// chunk_index (rtree_i32)
//    id     -- links to var_data.id
//    x_min  -- these are within the global dimensions, but the local extents
//    x_max
//    y_min
//    y_max
//    z_min
//    z_max

#ifdef __cplusplus
extern "C"
{
#endif

#ifndef RC_ENUM
#define RC_ENUM
enum RC
{
    RC_OK = NSSI_OK,
    RC_ERR
};
#endif

struct md_config
{
    int num_servers;
    char ** server_urls;
    unsigned int num_participants;
};

#ifdef __cplusplus
}
#endif

#endif
