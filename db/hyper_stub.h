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

    using apn_ptr_t = std::shared_ptr<async_put_node>;
    using ape_ptr_t = std::shared_ptr<async_put_edge>;
    using aai_ptr_t = std::shared_ptr<async_add_index>;

    class hyper_stub : private hyper_stub_base
    {
        private:
            const uint64_t shard_id;

        public:
            hyper_stub(uint64_t sid);
            void restore_backup(db::data_map<std::shared_ptr<db::node_entry>> *nodes,
                /*XXX std::unordered_map<node_handle_t, std::unordered_set<node_version_t, node_version_hash>> &edge_map,*/
                po6::threads::mutex *shard_mutexes);
            // bulk loading
            void bulk_load(int tid, std::unordered_map<node_handle_t, std::vector<node*>> *nodes);
            void memory_efficient_bulk_load(int tid, db::data_map<std::shared_ptr<db::node_entry>> *nodes);
            void memory_efficient_bulk_load(db::data_map<std::shared_ptr<db::node_entry>> &nodes);
            bool put_node_no_loop(db::node *n);
            bool put_edge_no_loop(const node_handle_t &node_handle, db::edge *e, const std::string &alias, bool del_after_call);
            bool add_index_no_loop(const node_handle_t &node_handle, const std::string &alias);
            bool flush_put_edge(uint32_t evict_idx);
            bool flush_all_put_edge();
            bool loop_async(uint64_t loops);
            bool loop_async_calls(bool flush);
            std::vector<apn_ptr_t> apn_pool;
            std::vector<ape_ptr_t> ape_pool;
            std::vector<aai_ptr_t> aai_pool;
            uint32_t apn_pool_sz, ape_pool_sz, aai_pool_sz;
            apn_ptr_t acquire_apn_ptr();
            void release_apn_ptr(apn_ptr_t);
            ape_ptr_t acquire_ape_ptr();
            void release_ape_ptr(ape_ptr_t);
            aai_ptr_t acquire_aai_ptr();
            void release_aai_ptr(aai_ptr_t);
            std::unordered_map<int64_t, apn_ptr_t> async_put_node_calls;
            std::unordered_map<int64_t, ape_ptr_t> async_put_edge_calls;
            std::vector<ape_ptr_t> put_edge_batch;
            std::unordered_map<int64_t, aai_ptr_t> async_add_index_calls;
            uint32_t put_edge_batch_clkhand;
            uint64_t apn_count, ape_count, aai_count;
            std::unique_ptr<e::buffer> restore_clk_buf;
            std::unique_ptr<e::buffer> last_clk_buf;
            // migration
            bool update_mapping(const node_handle_t &handle, uint64_t loc);
            bool recover_node(db::node &n);
        private:
            void put_node_loop(db::data_map<std::shared_ptr<db::node_entry>> &nodes,
                std::unordered_map<node_handle_t, node*> &node_map,
                int &progress,
                std::shared_ptr<vc::vclock> last_upd_clk,
                std::shared_ptr<vc::vclock_t> restore_clk);
            void put_index_loop(db::data_map<std::shared_ptr<db::node_entry>> &nodes,
                std::unordered_map<std::string, node*> &idx_add_if_not_exist,
                std::unordered_map<std::string, node*> &idx_add,
                int &ine_progress,
                int &progress);
    };
}

#endif
