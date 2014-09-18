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
#include "node_prog/node_prog_type.h"

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
        std::string handle, handle1, handle2;
        uint64_t loc1, loc2, sender;
        std::unique_ptr<std::string> key, value;
    };

    using done_req_t = std::vector<std::pair<uint64_t, node_prog::prog_type>>;

    struct nop_data
    {
        uint64_t max_done_id;
        vc::vclock_t max_done_clk;
        uint64_t outstanding_progs;
        std::unordered_map<uint64_t, done_req_t> done_reqs;
        std::vector<uint64_t> shard_node_count;
    };

    using tx_list_t = std::vector<pending_update*>;

    enum tx_type
    {
        // internal
        FAIL,
        EPOCH_CHANGE,
        // to shards
        NOP,
        UPDATE
    };

    struct pending_tx
    {
        tx_type type;
        uint64_t id; // unique tx id, assigned by client
        vc::vclock timestamp; // vector timestamp
        uint64_t qts; // queue timestamp
        std::vector<bool> shard_write; // which shards are involved in the write

        tx_list_t writes; // if this is a write tx
        uint64_t sender; // client to which we need to reply for write tx

        nop_data *nop; // if this is a nop

        uint64_t new_epoch; // if this is an epoch change

        pending_tx(tx_type t) : type(t), nop(nullptr) { }

        ~pending_tx()
        {
            if (type == NOP) {
                delete nop;
            } else if (type == UPDATE) {
                for (pending_update *upd: writes) {
                    delete upd;
                }
                writes.clear();
            }
        }

        pending_tx* copy_fail_transaction()
        {
            pending_tx *fail_tx = new pending_tx(FAIL);
            fail_tx->id = id;
            fail_tx->timestamp = timestamp;
            return fail_tx;
        }
    };

}

#endif
