/*
 * ===============================================================
 *    Description:  db::node implementation
 *
 *        Created:  2014-06-02 11:59:22
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <memory>
#include "common/message.h"
#include "common/cache_constants.h"
#include "common/config_constants.h"
#include "db/node.h"

using db::element::remote_node;
using db::element::edge;
using db::element::node;

node :: node(uint64_t id, vc::vclock &vclk, po6::threads::mutex *mtx)
    : base(id, vclk)
    , state(mode::NASCENT)
    , cv(mtx)
    , migr_cv(mtx)
    , in_use(true)
    , waiters(0)
    , permanently_deleted(false)
    , last_perm_deletion(nullptr)
    , new_loc(UINT64_MAX)
    , update_count(1)
    , migr_score(NUM_SHARDS, 0)
    , updated(true)
    , already_migr(false)
    , dependent_del(0)
    , cache(MAX_CACHE_ENTRIES)
{
    int num_prog_types = node_prog::END;
    prog_states.resize(num_prog_types);
}

void
node :: add_edge(edge *e)
{
#ifdef weaver_debug_
    edge_handles.emplace(e->get_id());
#endif
    out_edges.emplace(e->get_id(), e);
}

node_prog::edge_list
node :: get_edges()
{
    assert(base.view_time != NULL);
    return node_prog::edge_list(out_edges, base.view_time);
};

node_prog::prop_list
node :: get_properties()
{
    assert(base.view_time != NULL);
    return node_prog::prop_list(base.properties, *base.view_time);
};

void
node :: add_cache_value(std::shared_ptr<vc::vclock> vc,
    std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
    std::shared_ptr<std::vector<remote_node>> watch_set,
    uint64_t key)
{
#if MAX_CACHE_ENTRIES
    // clear oldest entry if cache is full
    if (MAX_CACHE_ENTRIES > 0 && cache.size() >= MAX_CACHE_ENTRIES) {
        vc::vclock &oldest = *vc;
        uint64_t key_to_del = key;
        for (auto& kvpair : cache) {
            vc::vclock &to_cmp = *kvpair.second.clk;
            // don't talk to kronos just pick one to delete
            if (order::compare_two_clocks(to_cmp.clock, oldest.clock) <= 0) {
                key_to_del = kvpair.first;
                oldest = to_cmp;
            }
        }
        cache.erase(key_to_del);
    }

    if (cache.size() < MAX_CACHE_ENTRIES) {
        cache_entry new_entry(cache_value, vc, watch_set);
        cache.emplace(key, new_entry);
    }
#else
    UNUSED(vc);
    UNUSED(cache_value);
    UNUSED(watch_set);
    UNUSED(key);
#endif
}

