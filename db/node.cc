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

#define weaver_debug_
#include <memory>
#include "common/message.h"
#include "common/cache_constants.h"
#include "common/config_constants.h"
#include "common/event_order.h"
#include "db/node.h"

using db::element::remote_node;
using db::element::edge;
using db::element::node;

node :: node(const node_handle_t &_handle, uint64_t shrd, vc::vclock &vclk, po6::threads::mutex *mtx)
    : base(_handle, vclk)
    , shard(shrd)
    , state(mode::NASCENT)
    , cv(mtx)
    , migr_cv(mtx)
    , in_use(true)
    , waiters(0)
    , permanently_deleted(false)
    , last_perm_deletion(nullptr)
    , new_loc(UINT64_MAX)
    , update_count(1)
    , migr_score(get_num_shards(), 0)
    , updated(true)
    , already_migr(false)
    , dependent_del(0)
    , cache(MaxCacheEntries)
{
    int num_prog_types = node_prog::END;
    prog_states.resize(num_prog_types);
}

node :: ~node()
{
    assert(out_edges.empty());
}

void
node :: add_edge(edge *e)
{
    out_edges.emplace(e->get_handle(), e);
}

node_prog::edge_list
node :: get_edges()
{
    assert(base.view_time != nullptr);
    assert(base.time_oracle != nullptr);
    return node_prog::edge_list(out_edges, base.view_time, base.time_oracle);
};

node_prog::prop_list
node :: get_properties()
{
    assert(base.view_time != nullptr);
    assert(base.time_oracle != nullptr);
    return node_prog::prop_list(base.properties, *base.view_time, base.time_oracle);
};

bool
node :: has_property(std::pair<std::string, std::string> &p)
{
    assert(base.view_time != nullptr);
    assert(base.time_oracle != nullptr);
    return base.has_property(p);
}

bool
node :: has_all_properties(std::vector<std::pair<std::string, std::string>> &props)
{
    assert(base.view_time != nullptr);
    assert(base.time_oracle != nullptr);
    return base.has_all_properties(props);
}

void
node :: add_alias(const node_handle_t &alias)
{
    aliases.emplace(alias);
}

bool
node :: del_alias(const node_handle_t &alias)
{
    return (aliases.erase(alias) != 0);
}

bool
node :: is_alias(const node_handle_t &alias)
{
    return (aliases.find(alias) != aliases.end());
}

void
node :: add_cache_value(std::shared_ptr<vc::vclock> vc,
    std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
    std::shared_ptr<std::vector<remote_node>> watch_set,
    cache_key_t key)
{
    if (MaxCacheEntries) {
        // clear oldest entry if cache is full
        if (cache.size() >= MaxCacheEntries) {
            std::vector<vc::vclock_t*> oldest(1, &vc->clock);
            cache_key_t key_to_del = key;
            for (const auto &kvpair : cache) {
                vc::vclock &to_cmp = *kvpair.second.clk;
                // don't talk to kronos just pick one to delete
                if (order::oracle::happens_before_no_kronos(to_cmp.clock, oldest)) {
                    key_to_del = kvpair.first;
                    oldest[0] = &to_cmp.clock;
                }
            }
            cache.erase(key_to_del);
        }

        if (cache.size() < MaxCacheEntries) {
            cache_entry new_entry(cache_value, vc, watch_set);
            cache.emplace(key, new_entry);
        }
    }
}

