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

#ifndef _TXN_GLOBALS_H
#define _TXN_GLOBALS_H

#define TRANSACTION_NAME_SIZE 32
#define MAX_SUB_TRANSACTIONS 128
#define NUM_TRANSPORTS 4

enum _txn_res
{
     TXN_SUCCESS = 1
    ,TXN_COORDINATOR_FAILURE
    ,TXN_SUB_COORDINATOR_FAILURE
    ,TXN_SUBORDINATE_FAILURE
    ,TXN_PARTICIPANT_FAILURE
    ,TXN_ERROR
    ,TXN_INCOMPLETE_VOTING
};
typedef enum _txn_res txn_res;

enum _txn_role
{
    TXN_ROLE_COORDINATOR = 10,
    TXN_ROLE_SUB_COORDINATOR,
    TXN_ROLE_SUBORDINATE
};
typedef enum _txn_role txn_role;

enum _txn_vote
{
    TXN_VOTE_COMMIT = 20,
    TXN_VOTE_ABORT,
    TXN_VOTE_UNKNOWN
};
typedef enum _txn_vote txn_vote;

/*
 * Represents the current status of the transaction, pertaining to 
 * the transactions completion or not. 
 * NOTES:
 * 1. single process sub transactions can only be done when status is NEW
 * 2. when creating a sub transaction_all, status changes. Once it is done,
 *    it switches back to IN_PROGRESS.
 */
enum _txn_status
{
    TXN_STATUS_NEW = 30,
    TXN_STATUS_PARTICIPANT_FAILURE,
    TXN_STATUS_SUCCESS,
    TXN_STATUS_IN_PROGRESS,
    TXN_STATUS_CREATING_SUB_TXN_ALL,
    TXN_STATUS_VOTING,
    TXN_STATUS_VOTED,
    TXN_STATUS_COMMITTING,
    TXN_STATUS_COMMITTED,
    TXN_STATUS_ABORTING,
    TXN_STATUS_ABORTED
};
typedef enum _txn_status txn_status;


/*
 * Representing the state of the current transaction or sub transaction.
 * This shows if it is being executed, valid to commit/abort, or done.
 */
enum _txn_state
{
    TXN_STATE_IN_PROGRESS = 40,
    TXN_STATE_COMMIT_READY,
    TXN_STATE_ABORT_READY,
    TXN_STATE_COMMIT_DONE,
    TXN_STATE_ABORT_DONE
};
typedef enum _txn_state txn_state;

/*
 * Enum representing the transport method for the sub_transaction.  
 * Putting TXN_ in front of them because stupid MPI is a C++ namespace and
 * to make sure we are scoped to just the TXN protocol.
enum _txn_transport
{
    TXN_MPI = 50,
    TXN_CMRPC,
    TXN_NSSI,
    TXN_TCP
};
typedef enum _txn_transport txn_transport;
 */

#endif

