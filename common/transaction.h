/*
 * ===============================================================
 *    Description:  Data structures for unprocessed transactions.
 *
 *        Created:  08/31/2013 03:17:04 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_transaction_h_
#define weaver_common_transaction_h_

#include <vector>
#include <unordered_set>

#include "common/vclock.h"

namespace transaction
{
    enum update_type
    {
        NODE_CREATE_REQ,
        EDGE_CREATE_REQ,
        NODE_DELETE_REQ,
        EDGE_DELETE_REQ,
        NODE_SET_PROPERTY,
        EDGE_SET_PROPERTY
    };

    // store state for update received from client but not yet completed
    struct pending_update
    {
        update_type type;
        vc::qtimestamp_t qts; // queue timestamp
        std::string handle, handle1, handle2;
        uint64_t loc1, loc2, sender;
        std::unique_ptr<std::string> key, value;
    };

    typedef std::vector<std::shared_ptr<pending_update>> tx_list_t;

    struct pending_tx
    {
        uint64_t id // unique tx id
            , client_id; // client to which we need to reply
        std::string client_tx_id; // unique tx id from client
        tx_list_t writes;
        vc::vclock timestamp; // vector timestamp
        std::unordered_set<std::string> del_elems;
        std::vector<std::string> busy_elems;
    };
}

#endif
