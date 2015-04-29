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
#include "db/types.h"
#include "db/node.h"
#include "db/edge.h"
#include "db/node_entry.h"

namespace db
{
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
            void restore_backup(db::data_map<db::node_entry> *nodes,
                /*XXX std::unordered_map<node_handle_t, std::unordered_set<node_version_t, node_version_hash>> &edge_map,*/
                po6::threads::mutex *shard_mutexes);
            // bulk loading
            void bulk_load(int tid, std::unordered_map<node_handle_t, std::vector<node*>> *nodes);
            void memory_efficient_bulk_load(int tid, db::data_map<db::node_entry> *nodes);
            // migration
            bool update_mapping(const node_handle_t &handle, uint64_t loc);
            bool recover_node(db::node &n);
    };
}

#endif
