/*
 * ===============================================================
 *    Description:  Hyperdex client stub for shard state.
 *
 *        Created:  2014-02-02 16:54:42
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_hyper_stub_h_
#define weaver_db_hyper_stub_h_

#include <po6/threads/mutex.h>

#include "common/hyper_stub_base.h"
#include "common/vclock.h"
#include "db/node.h"
#include "db/edge.h"

namespace db
{
    enum persist_node_state
    {
        STABLE = 0,
        MOVING
    };

    enum persist_migr_token
    {
        INACTIVE = 0, // this shard does not have the token
        ACTIVE // this shard does have the token
    };

    class hyper_stub : private hyper_stub_base
    {
        private:
            const uint64_t shard_id;

        public:
            hyper_stub(uint64_t sid);
            void restore_backup(bool &migr_token,
                std::unordered_map<node_handle_t, element::node*> *nodes,
                po6::threads::mutex *shard_mutexes);
            // bulk loading
            void bulk_load(std::unordered_map<node_handle_t, element::node*> *nodes);
            bool put_mappings(std::unordered_map<node_handle_t, uint64_t> &map);
    };
}

#endif
