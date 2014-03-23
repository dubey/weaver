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

#include "common/weaver_constants.h"
#include "common/hyper_stub_base.h"
#include "common/vclock.h"
#include "db/element/node.h"
#include "db/element/edge.h"

namespace db
{
    class hyper_stub : private hyper_stub_base
    {
        private:
            const uint64_t shard_id;
            const char *graph_space = "weaver_graph_data";
            const char *graph_attrs[5];
            const enum hyperdatatype graph_dtypes[5];
            const char *shard_space = "weaver_shard_data";
            const char *shard_attrs[2];
            const enum hyperdatatype shard_dtypes[2];
            typedef int64_t (hyperdex::Client::*hyper_func)(const char*,
                const char*,
                size_t,
                const struct hyperdex_client_attribute*,
                size_t,
                hyperdex_client_returncode*);
            typedef int64_t (hyperdex::Client::*hyper_map_func)(const char*,
                const char*,
                size_t,
                const struct hyperdex_client_map_attribute*,
                size_t,
                hyperdex_client_returncode*);

        public:
            hyper_stub(uint64_t sid);
            void init();
            void restore_backup(std::unordered_map<uint64_t, uint64_t> &qts_map,
                std::unordered_map<uint64_t, vc::vclock_t> &last_clocks);
            // graph updates
            void put_node(element::node &n, std::unordered_set<uint64_t> &nbr_map);
            element::node* get_node(uint64_t node);
            void update_creat_time(element::node &n);
            void update_del_time(element::node &n);
            void update_properties(element::node &n);
            void add_out_edge(element::node &n, element::edge *e);
            void remove_out_edge(element::node &n, element::edge *e);
            void add_in_nbr(uint64_t n_hndl, uint64_t nbr);
            void remove_in_nbr(uint64_t n_hndl, uint64_t nbr);
            // bulk loading
            void bulk_load(std::unordered_map<uint64_t, element::node*> nodes, std::unordered_map<uint64_t, std::unordered_set<uint64_t>> edge_map);
            // shard updates
            void increment_qts(uint64_t vt_id, uint64_t incr);
            void update_last_clocks(uint64_t vt_id, vc::vclock_t &vclk);
    };
}

#endif
