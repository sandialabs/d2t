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

#ifndef _TXN_CLIENT_H
#define _TXN_CLIENT_H

#include <stdint.h>

#include "txn_globals.h"
#include <mpi.h>

#ifdef __cplusplus
extern "C"
{
#endif

struct transaction;

struct sub_transaction;

struct transaction_comm;

//=============================
// global init/cleanup fns
//=============================

// initialize transactions for a particular communicator
// return: id to use when creating transactions
struct transaction_comm * 
txn_init(MPI_Comm, uint32_t);

// cleanup anything that the txn_init calls setup and potentially kill any
// transaction service we started
void
txn_finalize (struct transaction_comm * comm, bool shutdown_service);

//=============================
// transaction functions
//=============================

// create a new transaction using the initialized comm and using a particular id
struct transaction *
txn_create_transaction (struct transaction_comm * comm, uint64_t txid);

// start a transaction
// Any sub transactions that have been added will be synchronized immediately
// any added later will be done one at a time as each call is made.
uint32_t 
txn_begin_transaction(transaction * trans);

// create a single process transaction (no need to coordinate)
struct sub_transaction *
txn_create_sub_transaction (transaction * trans, const char * name, uint32_t proc_id);

// create a subtransaction where all processes participate (must coordinate)
struct sub_transaction *
txn_create_sub_transaction_all (transaction * trans, const char * name, uint32_t proc_id);

// vote on the overall transaction
// causes all associated sub-transactions to be voted on
uint32_t 
txn_vote_transaction(transaction * trans, uint32_t rank, txn_vote vote);

// commit the transaction (and sub-transactions)
uint32_t 
txn_commit_transaction(transaction * trans);

// abort the transaction (and sub-transactions)
uint32_t 
txn_abort_transaction(transaction * trans);

// cleanup anything allocated as part of this transaction
uint32_t 
txn_finalize_transaction(transaction * trans);

void txn_print_sub_transactions (transaction * trans, int rank);

//=============================
// sub transaction functions
//=============================

// vote by participants. This is local only.
txn_vote 
txn_vote_sub_transaction(transaction * trans, sub_transaction * sub_trans, txn_vote vote);

// commit this sub-transaction only. This is local only
txn_vote
txn_commit_sub_transaction(transaction * trans, sub_transaction * sub_trans);

// abort this sub-transaction only. This is local only
txn_vote
txn_abort_sub_transaction(transaction * trans, sub_transaction * sub_trans);

// cleanup anything related to this sub_trans. For example, if there is
// something that needs to happen during the final commit of the txn, you
// handle it here. This needs some connection to the user. Not sure it is
// necessary
txn_res 
txn_finalize_sub_transaction(transaction * trans, sub_transaction * sub_trans);

// true if content is the same otherwise false
bool
txn_compare_sub_transactions (const sub_transaction * sub1, const sub_transaction * sub2);

void txn_print_failed_ranks (struct transaction_comm * comm);

#ifdef __cplusplus
}
#endif

//TODO:
// 1. consider adding a txn_add_sub_transaction_some (or new comm) so that
//    you can use something other than 1 or all procs in a comm for a sub_txn
#endif
