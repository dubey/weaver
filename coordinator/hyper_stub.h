/*
 * ===============================================================
 *    Description:  Hyperdex client stub for timestamper state.
 *
 *        Created:  2014-02-26 13:37:34
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_coordinator_hyper_stub_h_
#define weaver_coordinator_hyper_stub_h_

#include "common/event_order.h"
#include "common/hyper_stub_base.h"

namespace coordinator
{
    class hyper_stub : private hyper_stub_base
    {
        private:
            uint64_t vt_id;
            po6::threads::mutex dummy_mtx;
            vclock_ptr_t dummy_clk;

        public:
            hyper_stub();
            void init(uint64_t vt_id);
            std::unordered_map<node_handle_t, uint64_t> get_mappings(std::unordered_set<node_handle_t> &get_set);
            bool get_idx(std::unordered_map<std::string, std::pair<std::string, uint64_t>>&);
            /*
            void do_tx(std::unordered_set<node_handle_t> &get_nodes,
                       std::unordered_set<edge_handle_t> &get_edges,
                       std::unordered_map<node_handle_t, std::vector<uint64_t>> &create_nodes,
                       std::unordered_set<edge_handle_t> &create_edges,
                       std::shared_ptr<transaction::pending_tx> tx,
                       bool &ready,
                       bool &error,
                       order::oracle *time_oracle);
            */
            void do_tx(std::shared_ptr<transaction::pending_tx> tx,
                       bool &ready,
                       bool &error,
                       order::oracle *time_oracle);
            void clean_tx(uint64_t tx_id);
            void restore_backup(std::vector<std::shared_ptr<transaction::pending_tx>> &txs);

        private:
            void clean_node(db::node*);
            void clean_up(std::unordered_map<node_handle_t, db::node*> &nodes);
            void clean_up(std::unordered_map<edge_handle_t, db::edge*> &edges);
            bool check_lastupd_clk(vc::vclock &before,
                                   vc::vclock &tx_clk,
                                   order::oracle*);
            bool del_key(const char *key, size_t key_sz,
                         const char *space,
                         std::unordered_map<int64_t, async_call_ptr_t> &async_calls);
            bool loop_async_calls(std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                                  std::unordered_map<int64_t, async_call_ptr_t> &done_calls);
            bool check_calls_status(std::unordered_map<int64_t, async_call_ptr_t> &async_calls);
            void recreate_tx(const hyperdex_client_attribute *attr, transaction::pending_tx &tx);
    };
}

#endif
