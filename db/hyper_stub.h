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

#include <random>
#include <po6/threads/mutex.h>

#include "common/hyper_stub_base.h"
#include "common/vclock.h"
#include "db/types.h"
#include "db/node.h"
#include "db/edge.h"
#include "db/node_entry.h"

// debug
#include <list>
#include "common/clock.h"

namespace db
{
    enum persist_migr_token
    {
        INACTIVE = 0, // this shard does not have the token
        ACTIVE // this shard does have the token
    };

    using async_call_ptr_t = std::shared_ptr<async_call>;
    using apn_ptr_t = std::shared_ptr<async_put_node>;
    using apes_ptr_t = std::shared_ptr<async_put_edge_set>;
    using ape_ptr_t = std::shared_ptr<async_put_edge>;
    using aai_ptr_t = std::shared_ptr<async_add_index>;

    template <typename T>
    class hyper_stub_pool
    {
        private:
            std::vector<async_call_ptr_t> pool;
            uint64_t sz;

        public:
            hyper_stub_pool();
            uint64_t size();
            std::shared_ptr<T> acquire();
            void release(std::shared_ptr<T>);
            void clear() { pool.clear(); }
    };

    class hyper_stub : private hyper_stub_base
    {
        private:
            const uint64_t shard_id;
            const int thread_id;

        public:
            hyper_stub(uint64_t sid, int tid);
            void restore_backup(db::data_map<std::shared_ptr<db::node_entry>> *nodes,
                /*XXX std::unordered_map<node_handle_t, std::unordered_set<node_version_t, node_version_hash>> &edge_map,*/
                po6::threads::mutex *shard_mutexes);
            // bulk loading
            bool put_node_no_loop(db::node *n);
            bool put_edge_no_loop(const node_handle_t &node_handle,
                                  db::edge *e,
                                  const std::string &alias,
                                  bool del_after_call);
            bool add_edge_to_node_set(const node_handle_t&,
                                      uint64_t edge_id);
            bool add_index_no_loop(const node_handle_t &node_handle,
                                   const std::string &alias);
            bool flush_or_defer_put_edge_set(apes_ptr_t ape, bool &defer);
            bool flush_put_edge_set(apes_ptr_t);
            bool flush_all_put_edge();
            bool loop_async_and_flush(uint64_t loops, uint64_t &timeouts);
            bool loop_async_calls(bool flush);
            void possibly_flush();
            void abort_bulk_load();
            void done_bulk_load();
            hyper_stub_pool<async_put_node> apn_pool;
            hyper_stub_pool<async_put_edge_set> apes_pool;
            hyper_stub_pool<async_put_edge> ape_pool;
            hyper_stub_pool<async_add_index> aai_pool;
            std::unordered_map<std::string, apes_ptr_t> put_edge_batch;
            uint64_t put_edge_batch_clock;
            std::unordered_map<std::string, std::vector<apes_ptr_t>> outstanding_node_puts;
            std::vector<apes_ptr_t> flushable_apes;
            std::unordered_map<int64_t, async_call_ptr_t> async_calls;
            std::unordered_map<node_handle_t, uint64_t> node_max_edge_id;
            std::unique_ptr<e::buffer> restore_clk_buf;
            std::unique_ptr<e::buffer> last_clk_buf;
            // migration
            bool update_mapping(const node_handle_t &handle, uint64_t loc);
            bool recover_node(db::node &n);

            // debug
            void done_op_stat(uint64_t time, size_t op_sz);
            std::list<std::pair<uint64_t, size_t>> done_op_stats;
            wclock::weaver_timer timer;
            uint64_t print_op_stats_counter;
        private:
            // pseudo random number gen for bulk loading, seeded by /dev/urandom
            uint64_t gen_seed;
            std::mt19937_64 mt64_gen;
            std::uniform_int_distribution<uint64_t> uint64max_dist;
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
            bool done_op(async_call_ptr_t, int64_t op_id);
    };
}

#endif
