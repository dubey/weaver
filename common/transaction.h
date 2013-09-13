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

#ifndef __COORD_TX__
#define __COORD_TX__

#include <vector>

#include "common/vclock.h"

namespace transaction
{
    enum update_type
    {
        NODE_CREATE_REQ,
        EDGE_CREATE_REQ,
        NODE_DELETE_REQ,
        EDGE_DELETE_REQ
    };

    // store state for update received from client but not yet completed
    struct pending_update
    {
        update_type type;
        vc::qtimestamp_t qts; // queue timestamp
        uint64_t handle, elem1, elem2, loc1, loc2, sender;
        //uint32_t key;
        //uint64_t value;
    };

    typedef std::vector<std::shared_ptr<pending_update>> tx_list_t;

    struct pending_tx
    {
        uint64_t id // unique tx id
            , client_id; // client to which we need to reply
        tx_list_t writes;
        vc::vclock timestamp; // vector timestamp
    };
}

#endif
