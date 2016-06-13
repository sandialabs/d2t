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

#include "txn_transaction.h"
#include "txn_client_mpi.h"
#include <vector>
#include <set>
#include <list>
#include <algorithm>
#include <assert.h>

#define WAIT_TIME 5.0
#define COORD_WAIT_TIME (WAIT_TIME * 2)
#define ERROR(a,b) ((a)>(b))?(a):(b)

#define COORDINATOR_SLOT 0

struct request_item
{
    int dest_rank;
    MPI_Request request;
}; 

typedef std::list<struct request_item *> request_list;

// this is from txn_client.h/implemented in txn_client.cpp
// true if content is the same otherwise false
extern "C"
bool
txn_compare_sub_transactions (const sub_transaction * sub1, const sub_transaction * sub2);

// this holds all of the comm info used by the other calls to avoid
// a local only copy that prevents multiple uses and makes threading difficult
struct transaction_comm
{
    int my_role; // what is the highest role I am playing?
    int my_rank; // what is my rank in the associated COMM?
    MPI_Comm comm; // the comm context
    int * sub_coord_rank; // list of sub_coord rank IDs (by group)
    int num_sub_coords; // how many sub_coords (in array)
    std::vector<int> failed_rank; // set of the globally failed ranks
    request_list failure_propagation;
    std::list<int *> failure_propagation_buffer;
    // can calculate the following:
    // my_group = calc_groups (my_group)
    // first rank in the group = calc_groups (sub_coord)
    // ranks in the group (no failures) = calc_groups (my_group_size)
    // my_sub_coord = sub_coord_ranks [my_group]
    // coord = sub_coord_ranks [0]
};

/*
 * Asychronous MPI broadcast.  Loops through the ranks
 * associated with specific MPI Communicator.
 */
static request_list * mpi_ibcast(struct transaction_comm * comms, char*, int, int, int);

/*
 * Helper function to loop through MPI requests and
 * see if they're completed or not.
 */
static int mpi_check_request_status(request_list * reqs);

/*
 * Helper function to loop through MPI requests, as
 * above, except repeats until all have completed
 * within a timeout period, or else indicate failure
 */
static int mpi_check_request_status_timed (struct transaction_comm * comms, request_list * reqs, double, int * coord_failed);

/*
 * TODO: Need to generalize with mpi_reduce_transactions.
 */
static int mpi_reduce_responses(mpi_tx_response*, mpi_tx_response*, int);

/*
 * Helper function to simply gather the responses from below you
 * and tell you whether or not you got them all and if they
 * all agree on the state
 */
static int mpi_reduce_transactions(struct transaction_comm * comms, mpi_tx*, int, int, MSG_TAG, int*);

/* Calculate which group any given rank belongs to and what the coords are
 */
static
void calc_groups (int rank, int size, int groups
                 ,int * group, int * group_size, int * sub_coord_rank
                 ,int * coord_rank
                 );

// check for failed rank messages and reconfigure the sub-coords/coord if
// necessary. Also sends to subordinates if proper.
static
int check_for_failed_ranks (struct transaction_comm * comm, int * coord_failed);

// this will both add the failed rank to the list, but also handle sending
// out 
static
void add_failed_rank (struct transaction_comm * comm, int rank);

static
const char * tag_to_string (int tag)
{
    switch (tag)
    {
        case TAG_BEGIN_TX: return "TAG_BEGIN_TX";
        case TAG_BCAST_SUB_TX_SIZE: return "TAG_BCAST_SUB_TX_SIZE";
        case TAG_BCAST_SUB_TX: return "TAG_BCAST_SUB_TX";
        case TAG_NEW_SUB_TX: return "TAG_NEW_SUB_TX";
        case TAG_BEGIN_SUB_TX: return "TAG_BEGIN_SUB_TX";
        case TAG_BEGIN_WRITE: return "TAG_BEGIN_WRITE";
        case TAG_REQ_VOTE_VAR: return "TAG_REQ_VOTE_VAR";
        case TAG_VOTE_VAR: return "TAG_VOTE_VAR";
        case TAG_PRE_COMMIT: return "TAG_PRE_COMMIT";
        case TAG_PRE_COMMIT_RESP: return "TAG_PRE_COMMIT_RESP";
        case TAG_FINALIZE_TX: return "TAG_FINALIZE_TX";
        case TAG_COMMIT: return "TAG_COMMIT";
        case TAG_ABORT: return "TAG_ABORT";
        case TAG_REQ_VOTE_TX: return "TAG_REQ_VOTE_TX";
        case TAG_VOTE_TX: return "TAG_VOTE_TX";
        case TAG_VOTE_TX_RESP: return "TAG_VOTE_TX_RESP";
        case TAG_FAILED_RANK: return "TAG_FAILED_RANK";
        case TAG_PROCESS_FAILURE: return "TAG_PROCESS_FAILURE";
        case TAG_NEW_SUB_COORDINATOR: return "TAG_NEW_SUB_COORDINATOR";
        case TAG_NEW_COORDINATOR: return "TAG_NEW_COORDINATOR";
        default: return "<invalid>";
    }
}

struct transaction_comm *
txn_mpi_init(MPI_Comm comm, int num_groups)
{
    struct transaction_comm * new_comm = new transaction_comm ();

    /////////////////////////////////////////////////
    MPI_Comm_dup (comm, &new_comm->comm);
    MPI_Comm_rank (new_comm->comm, &new_comm->my_rank);
    new_comm->num_sub_coords = num_groups;
    new_comm->sub_coord_rank = new int [num_groups];

    int size;
    MPI_Comm_size (new_comm->comm, &size);

    int my_group;
    int group_size;
    int sub_coord_rank;
    int coord_rank;

    int current_rank = 0;
    int i = 0;
    while (current_rank < size)
    {
        calc_groups (current_rank, size, num_groups
                    ,&my_group, &group_size, &new_comm->sub_coord_rank [i]
                    ,&coord_rank
                    );
        i++;
        current_rank += group_size;
    }
    calc_groups (new_comm->my_rank, size, num_groups
                ,&my_group, &group_size, &sub_coord_rank, &coord_rank
                );
    if (new_comm->my_rank == coord_rank)
    {
        new_comm->my_role = TXN_ROLE_COORDINATOR;
    }
    else
    {
        if (new_comm->my_rank == sub_coord_rank)
        {
            new_comm->my_role = TXN_ROLE_SUB_COORDINATOR;
        }
        else
        {
            new_comm->my_role = TXN_ROLE_SUBORDINATE;
        }
    }

    return new_comm;
}

void txn_mpi_finalize (struct transaction_comm * comm)
{
    MPI_Comm_free (&comm->comm);
    delete comm->sub_coord_rank;

    delete comm;
}

int mpi_begin_transaction(struct transaction * txn, mpi_tx * tx)
{
    tx->state = TXN_STATE_IN_PROGRESS;
    int retval = TXN_SUCCESS;
    int success1, success2;
    mpi_tx_response * response = new mpi_tx_response ();
    response->tx.tx_id = tx->tx_id;
    response->tx.status = TXN_STATUS_SUCCESS;
    response->tx.state = tx->state;
    response->num_sub_transactions = txn->num_sub_transactions;

    for (int i = 0; i < txn->num_sub_transactions; i++)
    {
        memcpy (&response->sub_tx [i], txn->sub_tx_list [i], sizeof (struct sub_transaction));
    }

    // This takes the known sub-transactions and pushes them up the tree
    success2 = mpi_gather_response(txn->comm, response, TAG_BEGIN_TX);
    assert (success2 == TXN_SUCCESS);

    for(int i = 0; i<response->num_sub_transactions; i++)
    {
        bool found = false;
        sub_transaction * temp = new sub_transaction ();
        memcpy(temp,
               &response->sub_tx[i],
               sizeof (struct sub_transaction));
         // search to see if this sub_txn is in the list already. If so,
         // skip, if not add to the list and increase the
         // tx->num_sub_transactions by 1
        for (int j = 0; j < txn->num_sub_transactions && !found; j++)
        {
            if (txn_compare_sub_transactions (txn->sub_tx_list [j], temp) == true)
            {
                found = true;
                delete temp;
            }
        }
        if (!found)
        {
            txn->sub_tx_list [txn->num_sub_transactions++] = temp;
        }
    }

    // then push down the merged list
    success1 = mpi_bcast_sub_transactions (txn);    
    if(success1 != TXN_SUCCESS) response->tx.status = TXN_STATUS_ABORTED;
    assert (success1 == TXN_SUCCESS);

    retval = ERROR(success1, success2);

    delete response;

    return retval;
}

/*
 * Processes know here whether or not all other processes vote to commit
 * or abort the transaction.  They can proceed to mark "commit" their operations
 * now.  If something fails, they can alert everyone in the finalize stage.
 * If there's an error, during the last round of messages, the coordinator
 * can send this out during the finalize phase.. i.e., if the root coordinator
 * gets an error from the last gather (something failed), the finalize
 * will be an abort.
 */
int mpi_vote_transaction(struct transaction_comm * comms, mpi_tx * tx, txn_vote MY_VOTE)
{
    int success2, success3, success4;
    
    /*
     * gather everyone's response. 
     */
    tx->vote = MY_VOTE;
    success2 = mpi_gather_transaction(comms, tx, TAG_REQ_VOTE_TX);

    if (comms->my_rank == comms->sub_coord_rank [COORDINATOR_SLOT])
    {
	// did everyone agree to commit?  If not, abort.
	if((success2 == TXN_SUCCESS) && (tx->vote == TXN_VOTE_COMMIT))		
        {
	    tx->vote = TXN_VOTE_COMMIT;
            tx->status = TXN_STATUS_SUCCESS;
        }
	else
        {
	    tx->vote = TXN_VOTE_ABORT;	      	
            tx->status = TXN_STATUS_ABORTING;
        }
    }
    // root sends final verdict to everyone.
    success3 = mpi_bcast_transaction(comms, tx, TAG_VOTE_TX);

    // root makes sure everyone responded (especially for commit).
    if(success3 != TXN_SUCCESS)    
	tx->vote = TXN_VOTE_ABORT;
    
    success4 = mpi_gather_transaction(comms, tx, TAG_VOTE_TX_RESP);
//printf ("gather vote rank: %d vote: %d\n", comms->my_rank, tx->vote); fflush (stdout);
    if(success4 != TXN_SUCCESS)
	tx->vote = TXN_VOTE_ABORT;
//printf ("vote %d 2: %d 3: %d 4: %d\n", comms->my_rank, success2, success3, success4); fflush (stdout);
    return ERROR(success2,ERROR(success3, success4));
}

int
mpi_commit_transaction(struct transaction_comm * comms, mpi_tx * tx)
{
    int retval = TXN_SUCCESS;
    int success1, success2, success3;
    success1 = mpi_bcast_transaction(comms, tx, TAG_PRE_COMMIT);
    if(success1 == TXN_SUCCESS)
	tx->vote = TXN_VOTE_COMMIT;
    else{
	tx->vote = TXN_VOTE_ABORT;
	retval = TXN_ERROR;
    }

    success2 = mpi_gather_transaction(comms, tx, TAG_PRE_COMMIT_RESP);   

    if (comms->my_rank == comms->sub_coord_rank [COORDINATOR_SLOT])
    {
	if((tx->vote != TXN_VOTE_COMMIT) && (success2 != TXN_SUCCESS))
	{
	    tx->vote = TXN_VOTE_ABORT;
	    retval = TXN_ERROR;
	}	
    }
    
    success3 = mpi_bcast_transaction(comms, tx, TAG_COMMIT);
    if(success3 != TXN_SUCCESS)
	retval = TXN_ERROR;
    return retval;
}

int mpi_abort_transaction(struct transaction_comm * comms, mpi_tx * tx)
{
    return mpi_bcast_transaction(comms, tx, TAG_ABORT);    
}

/*
 * Tell everyone to either commit or abort the transaction. 
 *
 */
int mpi_finalize_transaction(struct transaction_comm * comms, mpi_tx * tx, txn_vote THE_VOTE)
{
    tx->vote = THE_VOTE;
    // root disperses it's commit/abort to all of the subordinates.
    return mpi_bcast_transaction(comms, tx, TAG_FINALIZE_TX);   
}

static const char * res_to_string (int res)
{
    switch (res)
    {
        case TXN_SUCCESS: return "TXN_SUCCESS";
        case TXN_COORDINATOR_FAILURE: return "TXN_COORDINATOR_FAILURE";
        case TXN_SUB_COORDINATOR_FAILURE: return "TXN_SUB_COORDINATOR_FAILURE";
        case TXN_SUBORDINATE_FAILURE: return "TXN_SUBORDINATE_FAILURE";
        case TXN_PARTICIPANT_FAILURE: return "TXN_PARTICIPANT_FAILURE";
        case TXN_ERROR: return "TXN_ERROR";
        case TXN_INCOMPLETE_VOTING: return "TXN_INCOMPLETE_VOTING";
        default: return "<invalid>";
    }
}

static const char * status_to_string (int status)
{
    switch (status)
    {
        case TXN_STATUS_NEW: return "TXN_STATUS_NEW";
        case TXN_STATUS_PARTICIPANT_FAILURE: return "TXN_STATUS_PARTICIPANT_FAILURE";
        case TXN_STATUS_SUCCESS: return "TXN_STATUS_SUCCESS";
        case TXN_STATUS_IN_PROGRESS: return "TXN_STATUS_IN_PROGRESS";
        case TXN_STATUS_CREATING_SUB_TXN_ALL: return "TXN_STATUS_CREATING_SUB_TXN_ALL";
        case TXN_STATUS_VOTING: return "TXN_STATUS_VOTING";
        case TXN_STATUS_VOTED: return "TXN_STATUS_VOTED";
        case TXN_STATUS_COMMITTING: return "TXN_STATUS_COMMITTING";
        case TXN_STATUS_COMMITTED: return "TXN_STATUS_COMMITTED";
        case TXN_STATUS_ABORTING: return "TXN_STATUS_ABORTING";
        case TXN_STATUS_ABORTED: return "TXN_STATUS_ABORTED";
        default: return "<invalid>";
    }
}

static
void delete_request_item (request_item * i)
{
    MPI_Request_free (&i->request);
    delete i;
}

/*
 * TODO: Can be simplified.  COORDINATOR's role at SUB_COMM2 should
 * be SUB_COORDINATOR.  Reduces repeated code between COORDINATOR
 * and SUB_COORDINATOR's roles.
 */
int mpi_bcast_transaction(struct transaction_comm * comms, mpi_tx * tx, MSG_TAG tag)
{
    int retval = TXN_SUCCESS;
    int num_rec = 0;
    int offset = 0;
    request_list * requests = 0;
    int coord_failed;

    // from root to sub_coordinators
    if (comms->my_rank == comms->sub_coord_rank [COORDINATOR_SLOT])
    {
//printf ("rank: %d send to sub_coordinators %s\n", comms->my_rank, tag_to_string (tag)); fflush (stdout);
	// sending to sub_coordinators
	requests = mpi_ibcast(comms, (char*)tx, 
			      sizeof(mpi_tx), 
			      (int)tag, 
			      SUB_COMM1);
	if(!mpi_check_request_status_timed(comms, requests, WAIT_TIME, &coord_failed))
	{
//printf ("rank: %d bcast failed for %s\n", comms->my_rank, tag_to_string (tag)); fflush (stdout);
	    retval = TXN_SUB_COORDINATOR_FAILURE;
            //std::for_each (requests->begin (), requests->end (), delete_request_item);
            //delete requests;

            // cannot bail here because the subordinates are waiting and will
            // assume I am dead if I do not send them something
            //return retval;
	}
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;

	// now since the coordinator also has subordinates of its own..
	tx->from_app_rank = comms->my_rank;
	requests = mpi_ibcast(comms, (char*)tx, 
			      sizeof(mpi_tx), 
			      (int)tag, 
			      SUB_COMM2);
	if(!mpi_check_request_status_timed(comms, requests, WAIT_TIME, &coord_failed))
	{
//printf ("rank: %d bcast to subordinates failed for %s\n", comms->my_rank, tag_to_string (tag)); fflush (stdout);
	    retval = TXN_SUB_COORDINATOR_FAILURE;
//printf ("rank: %d ret: %s\n", comms->my_rank, res_to_string (retval)); fflush (stdout);
	}
        else
        {
            retval = (retval != TXN_SUCCESS ? retval : (tx->status == TXN_STATUS_SUCCESS ? TXN_SUCCESS : TXN_SUB_COORDINATOR_FAILURE));
//printf ("rank: %d ret: %s\n", comms->my_rank, res_to_string (retval)); fflush (stdout);
        }
//printf ("rank: %d mpi_bcast tx->status %s retval: %s\n", comms->my_rank, status_to_string (tx->status), res_to_string (retval)); fflush (stdout);
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;
    }
    // Get roots message, send to subordinates
    if (comms->my_role == TXN_ROLE_SUB_COORDINATOR)
    {
	// get message from the coordinator
	num_rec = mpi_get_all_tag_timed(comms, (char*)tx, sizeof(mpi_tx),
					sizeof(mpi_tx), 1, tag,
					COORD_WAIT_TIME, &offset, SUB_COMM1);
//printf ("rank: %d num_rec: %d tag: %s\n", comms->my_rank, num_rec, tag_to_string (tag)); fflush (stdout);
	// if it didn't find a bcast within the timout period, then
	// coordinator has failed.
	if(num_rec != 1)
	{
//printf ("rank: %d sub coord receive failed for %s\n", comms->my_rank, tag_to_string (tag)); fflush (stdout);
	    tx->tx_id = -1;
	    retval = TXN_COORDINATOR_FAILURE;
//printf ("rank: %d ret: %s\n", comms->my_rank, res_to_string (retval)); fflush (stdout);

            return retval;
	}
        else
        {
            retval = (tx->status == TXN_STATUS_SUCCESS ? TXN_SUCCESS : TXN_SUBORDINATE_FAILURE);
//printf ("rank: %d ret: %s\n", comms->my_rank, res_to_string (retval)); fflush (stdout);
        }

	tx->from_app_rank = comms->my_rank;
	// send message to the subordinates
	requests = mpi_ibcast(comms, (char*)tx, 
			      sizeof(mpi_tx), 
			      (int)tag, 
			      SUB_COMM2);
	if(!mpi_check_request_status_timed(comms, requests, WAIT_TIME, &coord_failed))
	{
//printf ("rank: %d bcast to subordinates failed for %s\n", comms->my_rank, tag_to_string (tag)); fflush (stdout);
	    retval = (retval != TXN_SUCCESS ? retval : TXN_SUBORDINATE_FAILURE);
	}
        else
        {
            retval = (retval != TXN_SUCCESS ? retval : (tx->status == TXN_STATUS_SUCCESS ? TXN_SUCCESS : TXN_SUBORDINATE_FAILURE));
        }
//printf ("rank: %d mpi_bcast tx->status %s retval: %s\n", comms->my_rank, status_to_string (tx->status), res_to_string (retval)); fflush (stdout);
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;
    }
    if (comms->my_role == TXN_ROLE_SUBORDINATE)
    {
	// get message from sub_coordinator
	num_rec = mpi_get_all_tag_timed(comms, (char*)tx, 
					sizeof(mpi_tx), 
					sizeof(mpi_tx), 
					1, 
					tag, 
					COORD_WAIT_TIME, 
					&offset, 
					SUB_COMM2);
//printf ("rank: %d num_rec: %d tag: %s\n", comms->my_rank, num_rec, tag_to_string (tag)); fflush (stdout);
	if(num_rec != 1)
	{
//printf ("rank: %d subordinate receive failed for %s\n", comms->my_rank, tag_to_string (tag)); fflush (stdout);
	    tx->tx_id = -1;
	    retval = TXN_SUB_COORDINATOR_FAILURE;
	}
        else
        {
            retval = (tx->status == TXN_STATUS_SUCCESS ? TXN_SUCCESS : TXN_SUB_COORDINATOR_FAILURE);
//printf ("rank: %d ret: %s\n", comms->my_rank, res_to_string (retval)); fflush (stdout);
        }
//printf ("rank: %d mpi_bcast tx->status %s retval: %s\n", comms->my_rank, status_to_string (tx->status), res_to_string (retval)); fflush (stdout);
    }
    return retval;
}

/*
 * TODO: Can be simplified.  COORDINATOR's role at SUB_COMM2 should
 * be SUB_COORDINATOR.  Reduces repeated code between COORDINATOR
 * and SUB_COORDINATOR's roles.
 */
int mpi_bcast_sub_transactions (struct transaction * txn)
{
    int retval = TXN_SUCCESS;
    int num_rec = 0;
    int offset = 0;
    request_list * requests = 0;
    mpi_tx_response response;
    int coord_failed;

    // from root to sub_coordinators
    if (txn->comm->my_rank == txn->comm->sub_coord_rank [COORDINATOR_SLOT])
    {
        response.num_sub_transactions = txn->num_sub_transactions;

	// sending to sub_coordinators
	requests = mpi_ibcast(txn->comm, (char*)&response.num_sub_transactions,
			      sizeof(int), 
			      (int)TAG_BCAST_SUB_TX_SIZE, 
			      SUB_COMM1);
	if(!mpi_check_request_status_timed(txn->comm, requests, COORD_WAIT_TIME, &coord_failed))
	{
	    retval = TXN_SUB_COORDINATOR_FAILURE;
	}
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;

	// now since the coordinator also has subordinates of its own..
	requests = mpi_ibcast(txn->comm, (char *)&response.num_sub_transactions,
			      sizeof(int), 
			      (int)TAG_BCAST_SUB_TX_SIZE,
			      SUB_COMM2);
	if(!mpi_check_request_status_timed(txn->comm, requests, COORD_WAIT_TIME, &coord_failed))
	{
	    retval = TXN_SUB_COORDINATOR_FAILURE;
	}
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;
    }
    // Get roots message, send to subordinates
    if (txn->comm->my_role == TXN_ROLE_SUB_COORDINATOR)
    {
	// get message from the coordinator
	num_rec = mpi_get_all_tag_timed(txn->comm, (char *)&response.num_sub_transactions, sizeof(int),
					sizeof(int), 1, TAG_BCAST_SUB_TX_SIZE,
					WAIT_TIME, &offset, SUB_COMM1);
	// if it didn't find a bcast within the timout period, then
	// coordinator has failed.
	if(num_rec != 1)
	{
	    txn->tx_id = -1;
	    retval = TXN_COORDINATOR_FAILURE;
	}

	// send message to the subordinates
	requests = mpi_ibcast(txn->comm, (char *)&response.num_sub_transactions,
			      sizeof(int),
			      (int)TAG_BCAST_SUB_TX_SIZE,
			      SUB_COMM2);
	if(!mpi_check_request_status_timed(txn->comm, requests, WAIT_TIME, &coord_failed))
	{
	    retval = TXN_SUBORDINATE_FAILURE;
	}
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;
    }
    if (txn->comm->my_role == TXN_ROLE_SUBORDINATE)
    {
	// get message from sub_coordinator
	num_rec = mpi_get_all_tag_timed(txn->comm, (char*)&response.num_sub_transactions,
					sizeof(int), 
					sizeof(int), 
					1, 
					TAG_BCAST_SUB_TX_SIZE,
					WAIT_TIME,
					&offset, 
					SUB_COMM2);
	if(num_rec != 1)
	{
	    txn->tx_id = -1;
	    retval = TXN_SUB_COORDINATOR_FAILURE;
	}
    }
// need to always send the count or we don't know if a payload is coming
if (response.num_sub_transactions != 0)
{
    offset = 0;
    // from root to sub_coordinators
    if (txn->comm->my_rank == txn->comm->sub_coord_rank [COORDINATOR_SLOT])
    {
        for (int i = 0; i < response.num_sub_transactions; i++)
        {
            memcpy (&response.sub_tx [i], txn->sub_tx_list [i], sizeof (struct sub_transaction));
        }
	// sending to sub_coordinators
	requests = mpi_ibcast(txn->comm, (char*) response.sub_tx,
			      response.num_sub_transactions * sizeof (struct sub_transaction),
			      (int)TAG_BCAST_SUB_TX, 
			      SUB_COMM1);
	if(!mpi_check_request_status_timed(txn->comm, requests, COORD_WAIT_TIME, &coord_failed))
	{
	    retval = TXN_SUB_COORDINATOR_FAILURE;
	}
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;

	// now since the coordinator also has subordinates of its own..
	requests = mpi_ibcast(txn->comm, (char *)response.sub_tx,
			      response.num_sub_transactions * sizeof(struct sub_transaction), 
			      (int)TAG_BCAST_SUB_TX, 
			      SUB_COMM2);
	if(!mpi_check_request_status_timed(txn->comm, requests, COORD_WAIT_TIME, &coord_failed))
	{
	    retval = TXN_SUB_COORDINATOR_FAILURE;
	}
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;
    }
    // Get roots message, send to subordinates
    if (txn->comm->my_role == TXN_ROLE_SUB_COORDINATOR)
    {
	// get message from the coordinator
	num_rec = mpi_get_all_tag_timed(txn->comm, (char*)response.sub_tx, sizeof(struct sub_transaction) * response.num_sub_transactions,
					sizeof(struct sub_transaction) * response.num_sub_transactions, 1, TAG_BCAST_SUB_TX,
					WAIT_TIME, &offset, SUB_COMM1);
	if(num_rec != 1)
	{
	    txn->tx_id = -1;
	    retval = TXN_COORDINATOR_FAILURE;
	}

	// send message to the subordinates
	requests = mpi_ibcast(txn->comm, (char*)response.sub_tx,
                              sizeof (struct sub_transaction) * response.num_sub_transactions,
			      (int)TAG_BCAST_SUB_TX, 
			      SUB_COMM2);
	if(!mpi_check_request_status_timed(txn->comm, requests, WAIT_TIME, &coord_failed))
	{
	    retval = TXN_SUBORDINATE_FAILURE;
	}
        std::for_each (requests->begin (), requests->end (), delete_request_item);
	delete requests;
        requests = 0;
    }
    if (txn->comm->my_role == TXN_ROLE_SUBORDINATE)
    {
	// get message from sub_coordinator
	num_rec = mpi_get_all_tag_timed(txn->comm, (char*)response.sub_tx,
					sizeof(struct sub_transaction) * response.num_sub_transactions, 
					sizeof(struct sub_transaction) * response.num_sub_transactions, 
					1, 
					TAG_BCAST_SUB_TX,
					WAIT_TIME,
					&offset, 
					SUB_COMM2);
	if(num_rec != 1)
	{
	    txn->tx_id = -1;
	    retval = TXN_SUB_COORDINATOR_FAILURE;
	}
    }

    // merge distributed sub_txns into the local set
    if (txn->comm->my_rank != txn->comm->sub_coord_rank [COORDINATOR_SLOT])
    {
        for (int i = 0; i < response.num_sub_transactions; i++)
        {
            bool found = false;
            for (int j = 0; j < txn->num_sub_transactions && !found; j++)
            {
                if (txn_compare_sub_transactions (&response.sub_tx [i], txn->sub_tx_list [j]) == true)
                    found = true; // break out
            }
            if (!found)
            {
                struct sub_transaction * temp = new sub_transaction ();
                memcpy (temp, &response.sub_tx [i], sizeof (struct sub_transaction));
                txn->sub_tx_list [txn->num_sub_transactions] = temp;
                txn->num_sub_transactions++;
            }
        }
    }
}
    return retval;
}

/*
 * Used primarily during the init phase, where the sub_coordinators and coordinators
 * aggregate all of the sub_transaction information from below.
 * TODO: Reduce repeated code. For SUB_COMM2, coordinator is also SUB_COORDINATOR.
 * Can use an additional check in SUB_COORDINATOR's section (if highest_role ==
 * SUB_COORDINATOR) then send upwards, else, don't. 
 */
int mpi_gather_response(struct transaction_comm * comms, mpi_tx_response * tx, MSG_TAG tag)
{
    int size;
    MPI_Comm_size (comms->comm, &size);
    int my_group;
    int group_size;
    int my_sub_coord;
    int coord_rank;
    calc_groups (comms->my_rank, size, comms->num_sub_coords
                ,&my_group, &group_size, &my_sub_coord
                ,&coord_rank
                );
    int retval = TXN_SUCCESS;
    int coord_failed;

    check_for_failed_ranks (comms, &coord_failed);

    // send my tx response upwards to my sub_coordinator
    if (comms->my_role == TXN_ROLE_SUBORDINATE)
    {
	request_list request;
        struct request_item * item = new request_item ();
        item->dest_rank = comms->sub_coord_rank [my_group];
	int err = MPI_Issend(tx, 
			    sizeof(mpi_tx_response), 
			    MPI_BYTE, 
			    comms->sub_coord_rank [my_group],
			    tag,
                            comms->comm, 
			    &item->request);
        request.push_front (item);
	if(err != MPI_SUCCESS)
	{
//printf ("mpi_gather_response rank: %d MPI failure\n", comms->my_rank); fflush (stdout);
if (item->dest_rank == 0) {printf ("here 1\n"); fflush (stdout);}
            add_failed_rank (comms, item->dest_rank);
	    return TXN_SUB_COORDINATOR_FAILURE;
	}
	// if request fails
	if(!mpi_check_request_status_timed(comms, &request, WAIT_TIME, &coord_failed))
	{
//printf ("mpi_gather_response rank: %d timeout\n", comms->my_rank); fflush (stdout);
//if (item->dest_rank == 0) {printf ("here 2\n"); fflush (stdout);}
            add_failed_rank (comms, item->dest_rank);
            // TODO: need to send to the new sub so that we can tell coord

            // if we have been promoted to sub_coordinator, we need to tell
            // the coordinator so it will not timeout. If that fails, we should
            // bail because things are in bad, bad shape.
            if (comms->my_role == TXN_ROLE_SUB_COORDINATOR)
            {
                tx->tx.status = TXN_STATUS_PARTICIPANT_FAILURE;
                item = new request_item ();
                item->dest_rank = comms->sub_coord_rank [0];
                int err = MPI_Issend(tx, 
                                     sizeof(mpi_tx_response), 
                                     MPI_BYTE, 
                                     comms->sub_coord_rank [0],
                                     tag,
                                     comms->comm, 
                                     &item->request);
                request.push_front (item);
                if(err != MPI_SUCCESS)
                {
//printf ("mpi_gather_response rank: %d MPI failure\n", comms->my_rank); fflush (stdout);
if (item->dest_rank == 0) {printf ("here 1\n"); fflush (stdout);}
                    add_failed_rank (comms, item->dest_rank);
                    return TXN_COORDINATOR_FAILURE;
	        }
                // if request fails
                if(!mpi_check_request_status_timed(comms, &request, WAIT_TIME, &coord_failed))
                {
                    add_failed_rank (comms, comms->sub_coord_rank [0]);
                    return TXN_COORDINATOR_FAILURE;
                }
            }
	    return TXN_SUB_COORDINATOR_FAILURE;
	}
	return TXN_SUCCESS;
    }

    int failed_ranks_my_group = 0;

    if (!comms->failed_rank.empty ())
    {
        for (int i = my_sub_coord; i < my_sub_coord + group_size; i++)
        {
            if (std::binary_search (comms->failed_rank.begin (), comms->failed_rank.end (), i))
                failed_ranks_my_group++;
        }
    }

    // gather responses, aggregate them, and send them to root
    if (comms->my_role == TXN_ROLE_SUB_COORDINATOR)
    {
        // gathering from subordinates
	int expected_msgs = group_size - 1 - failed_ranks_my_group;
	int array_size = expected_msgs * sizeof(mpi_tx_response);
	mpi_tx_response * array = new mpi_tx_response [expected_msgs];
	int offset = 0;
	int num_msgs = mpi_get_all_tag_timed(comms, (char*)array, array_size,
					     sizeof(mpi_tx_response),
					     expected_msgs, tag, WAIT_TIME,
					     &offset, SUB_COMM2);
        if (num_msgs != expected_msgs)
        {
            tx->tx.status = TXN_STATUS_PARTICIPANT_FAILURE;
            retval = TXN_SUBORDINATE_FAILURE;
        }
	int pass = mpi_reduce_responses(array, tx, num_msgs);
        delete array;
        array = 0;
	if(pass != TXN_SUCCESS)
	    retval = TXN_SUBORDINATE_FAILURE;
	tx->tx.from_app_rank = comms->my_rank;

	// Now, send it to the root, using sub_comm1
	request_list request;
        struct request_item * item = new request_item ();

        item->dest_rank = comms->sub_coord_rank [COORDINATOR_SLOT];
	tx->tx.from_app_rank = comms->my_rank;
	int err = MPI_Issend(tx, 
			    sizeof(mpi_tx_response), 
			    MPI_BYTE, 
			    item->dest_rank,
			    (int)tag, 
			    comms->comm, 
			    &item->request);
        request.push_front (item);
	if (err != MPI_SUCCESS)
	{
//printf ("mpi_gather_response rank: %d MPI failure\n", comms->my_rank); fflush (stdout);
	    return TXN_COORDINATOR_FAILURE;
	}
//printf ("sub_coord wait for ssend to coord\n"); fflush (stdout);
	if (!(mpi_check_request_status_timed(comms, &request, COORD_WAIT_TIME, &coord_failed)))
        {
//printf ("mpi_gather_response rank: %d timeout\n", comms->my_rank); fflush (stdout);
	    return TXN_COORDINATOR_FAILURE;
        }
//printf ("sub_coord done wait for ssend to coord\n"); fflush (stdout);
    }

    // role is coordinator, gather responses from both sub_coordinators
    // and your subordinates.
    if (comms->my_role == TXN_ROLE_COORDINATOR)
    {
	// gathering from subordinates
	int expected_msgs = group_size - 1 - failed_ranks_my_group;
	int array_size = expected_msgs * sizeof(mpi_tx_response);
	mpi_tx_response * array = new mpi_tx_response [expected_msgs];
	int offset = 0;
	
//printf ("mpi_gather_response coord start recv subordinates\n"); fflush (stdout);
	int num_msgs = mpi_get_all_tag_timed(comms, (char*)array, 
					 array_size,
					 sizeof(mpi_tx_response),
					 expected_msgs, 
					 tag, 
					 WAIT_TIME,
					 &offset, 
					 SUB_COMM2);
        if (num_msgs != expected_msgs)
        {
//printf ("mpi_gather_response coord end recv subordinates (failed)\n"); fflush (stdout);
            tx->tx.status = TXN_STATUS_PARTICIPANT_FAILURE;
            retval = TXN_SUBORDINATE_FAILURE;
        }
else
{
//printf ("mpi_gather_response coord end recv subordinates (success)\n"); fflush (stdout);
}

	int pass = mpi_reduce_responses(array, tx, num_msgs);

	if(pass != TXN_SUCCESS)
        {
            retval = TXN_SUBORDINATE_FAILURE;
        }
	delete array;
        array = 0;

	// gathering from subcoordinators
	expected_msgs = comms->num_sub_coords - 1 - 0; // no failed sub_coords
	array_size = expected_msgs * sizeof(mpi_tx_response);
	array = new mpi_tx_response [expected_msgs];
	offset = 0;
	num_msgs = mpi_get_all_tag_timed(comms, (char*)array, 
					     array_size,
					     sizeof(mpi_tx_response),
					     expected_msgs, 
					     tag, 
					     COORD_WAIT_TIME,
					     &offset, 
					     SUB_COMM1);       
					     	
        if (num_msgs != expected_msgs)
        {
            tx->tx.status = TXN_STATUS_PARTICIPANT_FAILURE;
            retval = TXN_SUB_COORDINATOR_FAILURE;
        }
	pass = mpi_reduce_responses(array, tx, num_msgs);	
	if(pass != TXN_SUCCESS)
        {
	    retval = TXN_SUB_COORDINATOR_FAILURE;
        }
	delete array;
        array = 0;
    }
    return retval;
}

/*
 * Can reduce repeated code between SUB_COORDINATOR and COORDINATOR.
 * Just check the highest_role var, and do accordingly.
 *
 * Needs to be modified now so that based on the state, it will aggregate correctly.
 * For example, if we're voting, and some child votes to abort, we have to set parent's
 * vote to ABORT.  Maybe we have to maintain who actually has done the aborting here?
 *
 */ 
int mpi_gather_transaction(struct transaction_comm * comms, mpi_tx * tx, MSG_TAG tag)
{
    int size;
    MPI_Comm_size (comms->comm, &size);
    int my_group;
    int group_size;
    int my_sub_coord;
    int coord_rank;
    calc_groups (comms->my_rank, size, comms->num_sub_coords
                ,&my_group, &group_size, &my_sub_coord
                ,&coord_rank
                );

    int failed_ranks_my_group = 0;
    int coord_failed;

    if (!comms->failed_rank.empty ())
    {
        for (int i = my_sub_coord; i < my_sub_coord + group_size; i++)
        {
            if (std::binary_search (comms->failed_rank.begin (), comms->failed_rank.end (), i))
                failed_ranks_my_group++;
        }
    }

    int retval = TXN_SUCCESS;

    // send my tx response upwards to my sub_coordinator
    if (comms->my_role == TXN_ROLE_SUBORDINATE)
    {
	request_list request;
        struct request_item * item = new request_item ();
        item->dest_rank = comms->sub_coord_rank [my_group];
//printf ("rank: %d telling my vote to %d\n", comms->my_rank, item->dest_rank); fflush (stdout);
	int err = MPI_Issend(tx, 
			    sizeof(mpi_tx), 
			    MPI_BYTE, 
			    item->dest_rank,
			    tag, 
			    comms->comm,
			    &item->request);
        request.push_front (item);
	if(err != MPI_SUCCESS)
	{
//printf ("rank: %d MPI failure\n", comms->my_rank); fflush (stdout);
	    return TXN_SUB_COORDINATOR_FAILURE;
	}
	// if request fails
	if(!mpi_check_request_status_timed(comms, &request, COORD_WAIT_TIME, &coord_failed))
	{
//printf ("rank: %d timeout\n", comms->my_rank); fflush (stdout);
	    return TXN_SUB_COORDINATOR_FAILURE;
	}
	return TXN_SUCCESS;
    }

    // gather responses, aggregate them, and send them to root
    if (comms->my_role == TXN_ROLE_SUB_COORDINATOR)
    {
	int array_size = (group_size - 1 - failed_ranks_my_group) * sizeof(mpi_tx);
	mpi_tx * array = new mpi_tx [group_size - 1 - failed_ranks_my_group];
	int num_msgs;

        // does a mpi_get_all_tag_timed
	int pass1 = mpi_reduce_transactions(comms, array, 
					    array_size, 
					    SUB_COMM2, 
					    tag, 
					    &num_msgs);	

	if(pass1 != TXN_SUCCESS)
	{
	    retval = TXN_SUBORDINATE_FAILURE;
	}
	// make this into a function!
	if((   (tx->state == TXN_STATE_COMMIT_DONE)
            || (tx->state == TXN_STATE_ABORT_DONE)
            || (tx->status == TXN_STATUS_VOTING)
           ) && (pass1 == TXN_SUCCESS)
          )
	{	    
//printf ("checking the votes\n"); fflush (stdout);
	    int i;
	    for(i=0; i<num_msgs; i++)
	    {
		if(array[i].vote != TXN_VOTE_COMMIT)
		{
		    tx->vote = TXN_VOTE_ABORT;
		    break;
		}
	    }
	}
	tx->from_app_rank = comms->my_rank;
	
	delete array;
        array = 0;

	// Now, send it to the root, using sub_comm1
	request_list requests;
        struct request_item * item = new request_item ();
        item->dest_rank = comms->sub_coord_rank [COORDINATOR_SLOT];
	tx->from_app_rank = comms->my_rank;
	
	int err = MPI_Issend(tx, 
			    sizeof(mpi_tx), 
			    MPI_BYTE, 
			    item->dest_rank,
			    (int)tag, 
			    comms->comm,
			    &item->request);
        requests.push_front (item);
assert (err == MPI_SUCCESS);
	if((err != MPI_SUCCESS) || 
	   !(mpi_check_request_status_timed(comms, &requests, COORD_WAIT_TIME, &coord_failed)))
	{
//printf ("can't send to the coordinator. Timed out\n"); fflush (stdout);
	    retval = TXN_COORDINATOR_FAILURE;
	}
        else
        {
//printf ("can send to the coordinator\n"); fflush (stdout);
        }
    }

    // role is coordinator, gather responses from both sub_coordinators
    // and your subordinates.
    if (comms->my_role == TXN_ROLE_COORDINATOR)
    {
        // gathering from subordinates
        int expected_messages = group_size - 1 - failed_ranks_my_group;
        int array_size = expected_messages * sizeof(mpi_tx);
        mpi_tx * array = new mpi_tx [expected_messages];
        int num_msgs = 0;

        // does a mpi_get_all_tag_timed
	int pass2 = mpi_reduce_transactions(comms, array, 
						array_size, 
						SUB_COMM2, 
						tag, 
						&num_msgs);

        delete array;
        array = 0;

	// gathering from subcoordinators
        expected_messages = comms->num_sub_coords - 1;
	array_size = expected_messages * sizeof(mpi_tx);
	array = new mpi_tx [expected_messages];

        // does a mpi_get_all_tag_timed
	int pass1 = mpi_reduce_transactions(comms, array, 
					    array_size,
					    SUB_COMM1, 
					    tag, 
					    &num_msgs);
	int is_abort = 0;
	if(pass1 != TXN_SUCCESS || pass2 != TXN_SUCCESS)
	    is_abort = 1;
	if((   (tx->state == TXN_STATE_COMMIT_DONE)
            || (tx->state == TXN_STATE_ABORT_DONE)
            || (tx->status == TXN_STATUS_VOTING)
           ) && (pass1 == TXN_SUCCESS)
          )
	{
	    {	    
		int i;
		for(i=0; i<num_msgs; i++)
		{
		    if(array[i].vote != TXN_VOTE_COMMIT)
		    {
			is_abort = 1;
			tx->vote = TXN_VOTE_ABORT;
			break;
		    }
		}
	    
	    }
	    delete array;
            array = 0;
	}
	if(is_abort == 1)
	    tx->vote = TXN_VOTE_ABORT;	              
    }
    return retval;
}

request_list * mpi_ibcast(struct transaction_comm * comms, char * buf,
                          int element_size, int tag, int index)
{
    request_list * requests = new request_list ();
    struct request_item * item = 0;
    int i;

    int size;
    MPI_Comm_size (comms->comm, &size);
    int my_group;
    int group_size;
    int my_sub_coord;
    int coord_rank;
    calc_groups (comms->my_rank, size, comms->num_sub_coords
                ,&my_group, &group_size, &my_sub_coord
                ,&coord_rank
                );

    int failed_ranks_my_group = 0;
    int coord_failed;

    // get any pending failed ranks messages and process them first
    check_for_failed_ranks (comms, &coord_failed);

    if (!comms->failed_rank.empty ())
    {
        for (int i = my_sub_coord; i < my_sub_coord + group_size; i++)
        {
            if (std::binary_search (comms->failed_rank.begin (), comms->failed_rank.end (), i))
                failed_ranks_my_group++;
        }
    }

switch (index)
{
    case ROOT_COMM1:
    case ROOT_COMM2:
        assert (false);
        break;

    case SUB_COMM1:
        for (i = 1; i < comms->num_sub_coords; i++)
        {
            item = new request_item ();
            item->dest_rank = comms->sub_coord_rank [i];
            requests->push_back (item);
            MPI_Issend(buf, element_size, MPI_BYTE, item->dest_rank, tag,
                      comms->comm, &(item->request));
        }
        break;

    case SUB_COMM2:
    for(i = 0; i < group_size; i++)
    {
        if (   i + my_sub_coord !=  comms->sub_coord_rank [my_group]
            && !(std::binary_search (comms->failed_rank.begin (), comms->failed_rank.end (), (i + my_sub_coord)))
           )
        {
            item = new request_item ();
            item->dest_rank = i + my_sub_coord;
            requests->push_back (item);
            MPI_Issend(buf, element_size, MPI_BYTE, item->dest_rank, tag, comms->comm,
                      &(item->request));
        }
    }
}

    return requests;
}

int mpi_reduce_responses(mpi_tx_response * array, mpi_tx_response * tx, int num_msgs)
{
    int retval = TXN_SUCCESS;
    std::vector<sub_transaction*> buckets;
    int i,j;	
    for(i = 0; i<num_msgs; i++)
    {
        if (array [i].tx.status != TXN_STATUS_SUCCESS)
            retval = TXN_STATUS_PARTICIPANT_FAILURE;
	for(j = 0; j<array[i].num_sub_transactions; j++)
	{
            bool found = false;
            for (std::vector<sub_transaction*>::iterator it = buckets.begin ()
                ;it != buckets.end () && !found
                ;it++
                )
            {
                if (txn_compare_sub_transactions (&array [i].sub_tx [j], *it))
                {
                    found = true;
                }
            }
	    if (!found)
	    {
		buckets.push_back (&array [i].sub_tx [j]);
	    }
	}
    }
    //add your existing sub_transactions to the map
    for(std::vector<sub_transaction*>::iterator it = buckets.begin(); it != buckets.end(); it++)
    {
        bool found = false;
        for (i = 0; i < tx->num_sub_transactions && !found; i++)
        {
            if (txn_compare_sub_transactions (&tx->sub_tx [i], *it) == true)
                found = true; // break out
        }
        if (!found)
        {
	    memcpy (&tx->sub_tx[tx->num_sub_transactions], *it, sizeof (struct sub_transaction));
            tx->num_sub_transactions++;
        }
    }

    return retval;
}

int mpi_reduce_transactions(struct transaction_comm * comms, mpi_tx * array, int array_size, 
			    int comm_index, MSG_TAG tag, 
			    int * num_msgs)
{
    int size;
    MPI_Comm_size (comms->comm, &size);
    int my_group;
    int group_size;
    int my_sub_coord;
    int coord_rank;
    calc_groups (comms->my_rank, size, comms->num_sub_coords
                ,&my_group, &group_size, &my_sub_coord
                ,&coord_rank
                );

    int failed_ranks_my_group = 0;

    if (!comms->failed_rank.empty ())
    {
        for (int i = my_sub_coord; i < my_sub_coord + group_size; i++)
        {
            if (!(std::binary_search (comms->failed_rank.begin (), comms->failed_rank.end (), i)))
                failed_ranks_my_group++;
        }
    }

    int ret_failure;
    int offset = 0;
    int expected_msgs;
switch (comm_index)
{
    case ROOT_COMM1:
    case ROOT_COMM2:
        assert (false);
        break;

    case SUB_COMM1:
        expected_msgs = comms->num_sub_coords - 1;
        *num_msgs = mpi_get_all_tag_timed(comms, (char*)array, 
				      array_size, 
				      sizeof(mpi_tx),
				      expected_msgs, 
				      tag, 
				      WAIT_TIME,
				      &offset, 
				      comm_index);

        break;
    case SUB_COMM2:
        expected_msgs = group_size - 1 - failed_ranks_my_group;
        *num_msgs = mpi_get_all_tag_timed(comms, (char*)array, 
				      array_size, 
				      sizeof(mpi_tx),
				      expected_msgs, 
				      tag, 
				      WAIT_TIME,
				      &offset, 
				      comm_index);
        break;
}
    if(comms->my_role == TXN_ROLE_COORDINATOR)
	ret_failure = TXN_SUB_COORDINATOR_FAILURE;
    if(comms->my_role == TXN_ROLE_SUB_COORDINATOR)
	ret_failure = TXN_SUBORDINATE_FAILURE;

    if(*num_msgs != expected_msgs)
	return ret_failure;
    return TXN_SUCCESS;
}

int mpi_get_all_tag(struct transaction_comm * comms, char * buf, int buf_size,
		    int element_size, int offset,
		    MSG_TAG tag, int comm_index, std::vector<int> * alive,
                    int * failed_found)
{
    int flag = 1;
    int msg_count = 0;
    int i;

    // need to iprobe for each rank at the specificed tab.
    for(i = offset; i<buf_size && flag;)
    {
        MPI_Status status;
        int coord_failed;

        // get all the failed rank messages first
        *failed_found = check_for_failed_ranks (comms, &coord_failed);

        // then look for the messages we are explicitly wanting to see
	MPI_Iprobe(MPI_ANY_SOURCE, (int)tag, comms->comm,
                         &flag, &status);
        if(flag)
        {
            MPI_Status temp;
            int source = status.MPI_SOURCE;
            MPI_Recv(&(buf[i]), element_size, MPI_BYTE, source,
                     (int)tag, comms->comm, &temp);
            msg_count++;
            alive->push_back (source);
            i += element_size;
        }
    }
    return msg_count;
}

static bool compare_ints (int i, int j)
{
    return i < j;
}

static void print_rank (int i)
{
    printf ("rank: %d\n", i); fflush (stdout);
}

int mpi_get_all_tag_timed(struct transaction_comm * comms, char * buf, int buf_size, int element_size,
			  int exp_messages, MSG_TAG tag, double wait_time,
			  int * offset, int comm_index)
{
    int num_msgs = 0;
    std::vector<int> alive;
    double start = MPI_Wtime();
    double end = MPI_Wtime();
    int failed_found;

    int size;
    MPI_Comm_size (comms->comm, &size);
    int my_group;
    int group_size;
    int my_sub_coord;
    int coord_rank;
    calc_groups (comms->my_rank, size, comms->num_sub_coords
                ,&my_group, &group_size, &my_sub_coord
                ,&coord_rank
                );

    int failed_ranks_my_group = 0;

    if (!comms->failed_rank.empty ())
    {
        for (int i = my_sub_coord; i < my_sub_coord + group_size; i++)
        {
            if (std::binary_search (comms->failed_rank.begin (), comms->failed_rank.end (), i))
                failed_ranks_my_group++;
        }
    }

    switch (comm_index)
    {
        case ROOT_COMM1:
        case ROOT_COMM2:
            assert (false);
            break;

        case SUB_COMM1:
            switch (comms->my_role)
            {
                case TXN_ROLE_SUBORDINATE:
                    assert (false);
                    break;

                case TXN_ROLE_SUB_COORDINATOR:
                    exp_messages = 1;
                    wait_time = COORD_WAIT_TIME;
                    break;

                case TXN_ROLE_COORDINATOR:
                    exp_messages = comms->num_sub_coords - 1;
                    wait_time = COORD_WAIT_TIME;
                    break;
            }
            break;

        case SUB_COMM2:
            switch (comms->my_role)
            {
                case TXN_ROLE_SUBORDINATE:
                    exp_messages = 1;
                    wait_time = COORD_WAIT_TIME;
                    break;

                case TXN_ROLE_SUB_COORDINATOR:
                    exp_messages = group_size - 1 - failed_ranks_my_group;
                    wait_time = WAIT_TIME;
                    break;

                case TXN_ROLE_COORDINATOR:
                    exp_messages = group_size - 1 - failed_ranks_my_group;
                    wait_time = WAIT_TIME;
                    break;
            }
            break;
    }

    while(num_msgs < exp_messages && (end-start) < wait_time)
    {
        int msgs = mpi_get_all_tag(comms, buf, buf_size,
                                   element_size, *offset,
				   tag, comm_index, &alive, &failed_found);
        num_msgs += msgs;
        (*offset)+=(msgs * element_size);
        end = MPI_Wtime();
        if (failed_found)
        {
//if (failed_found) {printf ("rank %d got a failure notice\n", comms->my_rank); fflush (stdout); }
            failed_found = 0;
            start = end; // reset the clock. Might need to just make
                         // original wait time + COORD_WAIT_TIME
            start += COORD_WAIT_TIME;
        }
    }

    if (num_msgs != exp_messages)
    {
        // first figure out who did not respond
        std::sort (alive.begin (), alive.end (), compare_ints);
        std::sort (comms->failed_rank.begin ()
                  ,comms->failed_rank.end ()
                  ,compare_ints
                  );
        int group, group_size, sub_coord_rank, coord_rank;
        int size;
        MPI_Comm_size (comms->comm, &size);
        calc_groups (comms->my_rank, size, comms->num_sub_coords
                    ,&group, &group_size, &sub_coord_rank, &coord_rank);
        // we know that the number of responses is < (size - 1). We just need
        // to figure out who did not respond (excluding already known failed
        // ranks).
#if 0
if (comms->my_rank == 0)
{
        printf ("alive: %d\n", alive.size ()); fflush (stdout);
        std::for_each (alive.begin (), alive.end (), print_rank);
        printf ("failed: %d\n", comms->failed_rank.size ()); fflush (stdout);
        std::for_each (comms->failed_rank.begin ()
                      ,comms->failed_rank.end ()
                      ,print_rank);
}
#endif
        std::vector<int> failures;
        switch (comm_index)
        {
            case ROOT_COMM1:
            case ROOT_COMM2:
                assert (false);
                break;

            case SUB_COMM1:
                for (int i = 0; i < comms->num_sub_coords; i++)
                {
                    if (   !(std::binary_search (alive.begin ()
                                                ,alive.end ()
                                                ,comms->sub_coord_rank [i]
                                                )
                            )
                        && !(std::binary_search (comms->failed_rank.begin ()
                                                ,comms->failed_rank.end ()
                                                ,comms->sub_coord_rank [i]
                                                )
                            )
                        && comms->sub_coord_rank [i] != comms->my_rank
                       )
                    {
//printf ("SUB_COMM1 rank %d failed\n", comms->sub_coord_rank [i]); fflush (stdout);
//assert (0);
if (comms->sub_coord_rank [i] == 0) {printf ("here 3\n"); fflush (stdout);}
                        add_failed_rank (comms, comms->sub_coord_rank [i]);
                        failures.push_back (comms->sub_coord_rank [i]);
                    }
                }
                break;

            case SUB_COMM2:
            {
//printf ("failure in SUB_COMM2 rank: %d num_msgs: %d exp_messages: %d tag: %s\n", comms->my_rank, num_msgs, exp_messages, tag_to_string (tag)); fflush (stdout);
                for (int i = sub_coord_rank
                    ;i < sub_coord_rank + group_size
                    ;i++
                    )
                {
                    if (   !(std::binary_search (alive.begin ()
                                                ,alive.end ()
                                                ,i
                                                )
                            )
                        && !(std::binary_search (comms->failed_rank.begin ()
                                                ,comms->failed_rank.end ()
                                                ,i
                                                )
                            )
                        && i != comms->my_rank
                       )
                    {
//printf ("SUB_COMM2 rank %d failed detected by %d\n", i, comms->my_rank); fflush (stdout);
if (i == 0) {printf ("here 4\n"); fflush (stdout);}
                        add_failed_rank (comms, i);
                        failures.push_back (i);
                    }
                }
                break;
            }
        }

//num_msgs = exp_messages;
    }
#if 0
if (comms->my_rank == 0)
{
    printf ("Post failure\n"); fflush (stdout);
//    printf ("alive: %d\n", alive.size ()); fflush (stdout);
//    std::for_each (alive.begin (), alive.end (), print_rank);
    printf ("rank: %d failed count: %d\n", comms->my_rank, comms->failed_rank.size ()); fflush (stdout);
    std::for_each (comms->failed_rank.begin (), comms->failed_rank.end (), print_rank);
}
#endif

    return(num_msgs);
}

int mpi_check_request_status(request_list * requests)

{
    int flag = 0;
    request_list::iterator it = requests->begin ();
    while (it != requests->end ())
    {
	MPI_Status status;
	MPI_Request_get_status((*it)->request, &flag, &status);

        if (flag)
        {
            // theoretically does an implicit MPI_Request_free
            MPI_Test (&(*it)->request, &flag, &status);
            delete *it;
            it = requests->erase (it);
        }
        else
            it++;
    }

    return requests->empty () ? 1 : 0;  // if empty, finished
}

static
int mpi_check_request_status_timed (struct transaction_comm * comms, request_list * requests, double wait_time, int * coord_failed)
{
    double start = MPI_Wtime();
    double end = MPI_Wtime();
    int failed_found = 0;

    while((end-start) < wait_time)
    {
        // get all the failed rank messages first
        failed_found = check_for_failed_ranks (comms, coord_failed);
        if (failed_found && !(*coord_failed))
        {
            failed_found = 0;
            start = end; // may need to make this old time + COORD_WAIT_TIME
            start += COORD_WAIT_TIME;
        }

	if(mpi_check_request_status(requests))
        {
	    return 1;
        }
	else
	    end = MPI_Wtime();
    }
    return 0;
}

static
void calc_groups (int rank, int size, int groups
                 ,int * group, int * group_size, int * sub_coord_rank
                 ,int * coord_rank
                 )
{
    *group_size = size / groups;
    int larger_groups = size % groups;
    if (rank < larger_groups * (*group_size + 1) || !larger_groups)
    {
        if (larger_groups)
            (*group_size)++;
        *group = rank / (*group_size);
        *sub_coord_rank =   *group_size * (*group);
    }
    else
    {
        *group =     (larger_groups)
                   + (rank - (larger_groups * (*group_size + 1)))
                 / *group_size;
        *sub_coord_rank =   (larger_groups * (*group_size + 1))
                          + (   (*group - larger_groups)
                              * *group_size
                             );
    }
    *coord_rank = 0;
}

void txn_mpi_print_failed_ranks (struct transaction_comm * comm)
{
    for (std::vector<int>::iterator i = comm->failed_rank.begin ()
        ;i != comm->failed_rank.end ()
        ;++i
        )
        printf (" {%d} failed rank: %d\n", comm->my_rank, *i); fflush (stdout);
}

// probe for failed rank messages and reconfigure the sub-coords/coord if
// necessary. Also sends to subordinates if proper.
//
// every function goes through either mpi_ibcast or mpi_get_all_tag_timed.
// This is called by both before their processing so that we can gather any
// outstanding messages and process them before we do any communicating.
static
int check_for_failed_ranks (struct transaction_comm * comm, int * coord_failed)
{
    MPI_Status status;
    int flag;
    int failed_found = 0;
    *coord_failed = 0;

    do
    {
        MPI_Iprobe (MPI_ANY_SOURCE, TAG_FAILED_RANK, comm->comm, &flag
                   ,&status);
        if (flag)
        {
            int rank;
            MPI_Status temp;
    
            MPI_Recv (&rank, sizeof (int), MPI_BYTE, status.MPI_SOURCE
                     ,TAG_FAILED_RANK, comm->comm, &temp);
            if (rank == comm->sub_coord_rank [COORDINATOR_SLOT])
                *coord_failed = 1;
            add_failed_rank (comm, rank);
            failed_found = 1;
        }
    } while (flag);

    return failed_found;
}

// this will both add the failed rank to the list, but also handle sending
// out 
static
void add_failed_rank (struct transaction_comm * comms, int rank)
{
    int size;
    MPI_Comm_size (comms->comm, &size);
    int my_group;
    int group_size;
    int sub_coord_rank;
    int coord_rank;
    calc_groups (comms->my_rank, size, comms->num_sub_coords
                ,&my_group, &group_size, &sub_coord_rank
                ,&coord_rank
                );

    int * buf = new int (rank);

    comms->failed_rank.push_back (rank);
    std::sort (comms->failed_rank.begin ()
              ,comms->failed_rank.end ()
              ,compare_ints
              );
//printf ("add_failed_rank1: my rank: %d failed rank: %d\n", comms->my_rank, rank); fflush (stdout);

    // tell subordinates if I am not just a subordinate
    if (comms->my_role != TXN_ROLE_SUBORDINATE)
    {
        for (int i = sub_coord_rank
            ;i < sub_coord_rank + group_size
            ;i++
            )
        {
            if (!(std::binary_search (comms->failed_rank.begin ()
                                     ,comms->failed_rank.end ()
                                     ,i
                                     )
                 )
                && i != comms->my_rank
               )
            {
//printf ("sending FAILED_RANK (%d) to %d from %d\n", rank, i, comms->my_rank);
                request_item * item = new request_item ();
                item->dest_rank = i;
                MPI_Issend (buf, sizeof (int), MPI_BYTE
                           ,item->dest_rank, TAG_FAILED_RANK
                           ,comms->comm, &(item->request)
                           );
//printf ("rank %d tell rank %d that rank %d failed\n", comms->my_rank, item->dest_rank, *buf); fflush (stdout);
                comms->failure_propagation.push_back (item);
            }
        }
    }

    // if the failed rank was a sub_coord, I need to update my list so
    // that the next message to the sub_coords can be successful
    // purposely redefining vars in the inner scope for easier reading
    {
        int failed_group;
        int group_size;
        int sub_coord_rank;
        int coord_rank;
        calc_groups (rank, size, comms->num_sub_coords
                    ,&failed_group, &group_size, &sub_coord_rank
                    ,&coord_rank
                    );
        if (comms->sub_coord_rank [failed_group] == rank)
        {
            for (int i = rank + 1; i < sub_coord_rank + group_size; i++)
            {
                if (std::find (comms->failed_rank.begin ()
                              ,comms->failed_rank.end ()
                              ,i
                              ) == comms->failed_rank.end ()
                   )
                {
                    comms->sub_coord_rank [failed_group] = i;
//printf ("add_failed_rank2: my rank: %d new_sub_coord: %d\n", comms->my_rank, i); fflush (stdout);
                    if (i == comms->my_rank)
                    {
                        if (my_group == 0)
                        {
                            comms->my_role = TXN_ROLE_COORDINATOR;
//printf ("add_failed_rank3: my rank: %d new role: %d\n", comms->my_rank, comms->my_role); fflush (stdout);
                        }
                        else
                        {
                            comms->my_role = TXN_ROLE_SUB_COORDINATOR;
//printf ("add_failed_rank4: my rank: %d new role: %d\n", comms->my_rank, comms->my_role); fflush (stdout);
                        }
                    }
                    break;
                }
            }
        }
    }

    // if I am the sub_coord (or new sub_coord)
    // and the failure was in my group, tell the other sub_coords
    if (   comms->sub_coord_rank [my_group] == comms->my_rank
        && (sub_coord_rank <= rank && rank <= (sub_coord_rank + group_size - 1))
       )
    {
        // tell sub_coords (includes coord)
        for (int i = 0
            ;i < comms->num_sub_coords
            ;i++
            )
        {
            if (!(std::binary_search (comms->failed_rank.begin ()
                                     ,comms->failed_rank.end ()
                                     ,comms->sub_coord_rank [i]
                                     )
                 )
                && comms->sub_coord_rank [i] != comms->my_rank
               )
            {
//printf ("sending FAILED_RANK (%d) to sub_coord %d from %d\n", rank, i, comms->my_rank);
                request_item * item = new request_item ();
                item->dest_rank = comms->sub_coord_rank [i];
                MPI_Issend (buf, sizeof (int), MPI_BYTE
                           ,item->dest_rank, TAG_FAILED_RANK
                           ,comms->comm, &(item->request)
                           );
//printf ("rank %d tell sub_coord rank %d that rank %d failed\n", comms->my_rank, item->dest_rank, *buf); fflush (stdout);
                comms->failure_propagation.push_back (item);
            }
        }
        comms->failure_propagation_buffer.push_back (buf);
    }

//printf ("my rank: %d sub_coord [0]: %d sub_coord [1]: %d\n", comms->my_rank, comms->sub_coord_rank [0],comms->sub_coord_rank [1]); fflush (stdout);
}
