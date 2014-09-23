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

#include <google/dense_hash_map>

#include "common/event_order.h"
#include "common/hyper_stub_base.h"
#include "coordinator/current_tx.h"

namespace coordinator
{
    struct eqstr
    {
        bool operator() (const std::string &s1, const std::string &s2) const
        {
            return s1 == s2;
        }
    };

    class hyper_stub : private hyper_stub_base
    {
        private:
            uint64_t vt_id;
            po6::threads::mutex dummy_mtx;
            vc::vclock dummy_clk;
            //std::unordered_map<node_handle_t, uint64_t> cached_nmap;
            google::dense_hash_map<std::string, uint64_t, std::hash<std::string>, eqstr> cached_nmap;
            po6::threads::mutex cached_nmap_mutex;

        public:
            std::unordered_map<node_handle_t, uint64_t> get_mappings(std::unordered_set<node_handle_t> &get_set);
            void do_tx(std::unordered_set<node_handle_t> &get_set,
                std::unordered_set<node_handle_t> &del_set,
                std::unordered_map<node_handle_t, uint64_t> &loc_map,
                transaction::pending_tx *tx,
                bool &ready,
                bool &error,
                order::oracle *time_oracle);
            void clean_tx(uint64_t tx_id);
            hyper_stub(uint64_t vt_id);

        private:
            void clean_up(std::unordered_map<node_handle_t, db::element::node*> &nodes);
            void cache_nmap(const node_handle_t&, uint64_t);
            void cache_nmap(const std::unordered_map<node_handle_t, uint64_t>&);
    };
}

#endif
