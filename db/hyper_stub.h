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

    class hyper_stub : private hyper_stub_base
    {
        public:

            template <typename T>
            class hyper_stub_pool
            {
                private:
                    std::vector<async_call_ptr_t> pool;
                    uint64_t sz;
                    async_call_type type;

                public:
                    hyper_stub_pool(async_call_type type);
                    uint64_t size();
                    std::shared_ptr<T> acquire();
                    void release(std::shared_ptr<T>);
                    void clear() { pool.clear(); }
            };

            hyper_stub(uint64_t sid, int tid);
            int fd();
            void restore_backup(db::data_map<std::shared_ptr<db::node_entry>> *nodes,
                /*XXX std::unordered_map<node_handle_t, std::unordered_set<node_version_t, node_version_hash>> &edge_map,*/
                po6::threads::mutex *shard_mutexes);
            void print_errors();
            // bulk loading
            void mark_done_chunk(uint64_t chunk);
            void check_loaded_chunk(uint64_t chunk, uint32_t chunk_elem, bool &lchunk, bool &lelem);
            void wait_done_chunk(uint64_t chunk);
            bool new_node(const node_handle_t &handle, uint64_t start_edge_idx);
            bool put_node_no_loop(db::node *n);
            uint64_t new_edge(const node_handle_t &node_handle);
            bool put_edge_no_loop(const node_handle_t &node_handle,
                                  db::edge *e,
                                  uint64_t edge_id,
                                  bool del_after_call);
            bool put_edge_id_no_loop(uint64_t edge_id,
                                     const edge_handle_t &edge_handle);
            bool put_node_edge_id_set_no_loop(const node_handle_t &node_handle,
                                              int64_t start_id,
                                              int64_t end_id);
            bool add_index_no_loop(const node_handle_t &node_handle,
                                   const std::string &alias);
            bool flush_all_edge_ids();
            bool loop_async_and_flush(uint64_t loops, uint64_t &timeouts);
            bool loop_async_calls(bool flush);
            void possibly_flush();
            void abort_bulk_load();
            void done_bulk_load();
            // migration
            bool update_mapping(const node_handle_t &handle, uint64_t loc);
            // node swapping
            bool recover_node(db::node &n);
            bool get_node_no_loop(db::node *n);
            bool loop_get_node(db::node **n);

        private:
            bool done_op(async_call_ptr_t, int64_t op_id);
            bool done_get_op(async_call_ptr_t, int64_t op_id, db::node **n);
            // debug
            void done_op_stat(uint64_t time, size_t op_sz);

            // member vars

            const uint64_t m_shard_id;
            const int m_thread_id;
            std::unordered_map<int64_t, async_call_ptr_t> m_async_calls;

            // bulk load
            hyper_stub_pool<async_put_node> m_apn_pool;
            hyper_stub_pool<async_put_edge_set> m_apes_pool;
            hyper_stub_pool<async_put_edge> m_ape_pool;
            hyper_stub_pool<async_put_edge_id> m_apei_pool;
            hyper_stub_pool<async_add_index> m_aai_pool;
            std::unique_ptr<e::buffer> m_restore_clk_buf;
            std::unique_ptr<e::buffer> m_last_clk_buf;
            // bulk load ds may be accessed by other threads outside member functions
            // concurrent access protected by mutex
            uint64_t g_load_chunk;
            std::unordered_set<uint32_t> g_loaded_elems;
            std::unordered_map<node_handle_t, std::pair<uint64_t, uint64_t>> g_node_edge_id; // node handle -> (start edge id, edge count)
            po6::threads::mutex g_bulk_load_mtx;
            po6::threads::cond g_chunk_cond;

            // shard funcs
            po6::threads::mutex m_shard_mtx;

            // debug
            std::list<std::pair<uint64_t, size_t>> m_done_op_stats;
            wclock::weaver_timer m_timer;
            uint64_t m_print_op_stats_counter;
    };
}

#endif
