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

#ifndef _TXN_TRANSACTION_H
#define _TXN_TRANSACTION_H

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "txn_globals.h"

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Represents a timestamp for a transaction.  This is used for basic
 * logging and during timeout detection.
 */
struct txn_timestamp
{
    time_t init_time;
    time_t last_update_time;
    time_t finalize_time;
};

/*
 * Represents a sub_transaction.
 */
struct sub_transaction
{
    char name[TRANSACTION_NAME_SIZE];
    txn_state state;
    txn_vote vote;            // each local proc votes. Then collected up.
    txn_timestamp timestamp;
    unsigned char all_procs;
    uint32_t proc_id;
};

struct transaction_comm;

/*
 * Encapsulates the state for a given transaction.  Contains any
 * number of sub_transactions. 
 */
struct transaction
{
    int tx_id;
    txn_status status;
    txn_state state;
    txn_timestamp timestamp;
    int num_sub_transactions;
    int num_committed_sub_tx;
    sub_transaction * sub_tx_list[MAX_SUB_TRANSACTIONS];
    transaction_comm * comm;
};

#ifdef __cplusplus
}
#endif

#endif
