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
#define NUM_GRAPH_ATTRS 7
#define NUM_SHARD_ATTRS 3

#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
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
            const char *graph_space = "weaver_graph_data";
            const char *graph_attrs[NUM_GRAPH_ATTRS];
            const enum hyperdatatype graph_dtypes[NUM_GRAPH_ATTRS];
            const char *shard_space = "weaver_shard_data";
            const char *shard_attrs[NUM_SHARD_ATTRS];
            const enum hyperdatatype shard_dtypes[NUM_SHARD_ATTRS];
            const char *nmap_space = "weaver_loc_mapping";
            const char *nmap_attr = "shard";
            const enum hyperdatatype nmap_dtype = HYPERDATATYPE_INT64;
            void recreate_node(const hyperdex_client_attribute *cl_attr, element::node &n, std::unordered_set<uint64_t> &nbr_map);

        public:
            hyper_stub(uint64_t sid);
            void init();
            void restore_backup(std::unordered_map<uint64_t, uint64_t> &qts_map,
                std::unordered_map<uint64_t, vc::vclock_t> &last_clocks,
                bool &migr_token,
                std::unordered_map<uint64_t, element::node*> &nodes,
                std::unordered_map<uint64_t, std::unordered_set<uint64_t>> &edge_map,
                po6::threads::mutex *shard_mutex);
            // graph updates
            void put_node(element::node &n, std::unordered_set<uint64_t> &nbr_map);
            void update_creat_time(element::node &n);
            void update_del_time(element::node &n);
            void update_properties(element::node &n);
            void add_out_edge(element::node &n, element::edge *e);
            void remove_out_edge(element::node &n, element::edge *e);
            void add_in_nbr(uint64_t n_hndl, uint64_t nbr);
            void remove_in_nbr(uint64_t n_hndl, uint64_t nbr);
            void update_tx_queue(element::node &n);
            void update_migr_status(uint64_t n_hndl, enum persist_node_state status);
            // bulk loading
            void bulk_load(std::unordered_map<uint64_t, element::node*> nodes, std::unordered_map<uint64_t, std::unordered_set<uint64_t>> edge_map);
            // shard updates
            void increment_qts(uint64_t vt_id, uint64_t incr);
            void update_last_clocks(uint64_t vt_id, vc::vclock_t &vclk);
            void update_migr_token(enum persist_migr_token token);
    };
}

#endif
