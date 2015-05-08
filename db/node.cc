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

using db::remote_node;
using db::edge;
using db::node;
using db::migr_data;

migr_data :: migr_data()
    : new_loc(UINT64_MAX)
    , migr_score(get_num_shards(), 0)
    , already_migr(false)
{ }

node :: node(const node_handle_t &_handle, uint64_t shrd, vclock_ptr_t &vclk, po6::threads::mutex *mtx)
    : base(_handle, vclk)
    , shard(shrd)
    , state(mode::NASCENT)
    , cv(mtx)
    , migr_cv(mtx)
    , in_use(true)
    , waiters(0)
    , permanently_deleted(false)
    , evicted(false)
    , to_evict(false)
    , last_perm_deletion(nullptr)
{
    std::string empty("");
    out_edges.set_deleted_key(empty);
    aliases.set_deleted_key(empty);
}

node :: ~node()
{
    assert(out_edges.empty());
}

// true if prog states is empty
bool
node :: empty_evicted_node_state()
{
    for (const auto &p: prog_states) {
        if (!p.second.empty()) {
            return false;
        }
    }

    if (!tx_queue.empty() || last_perm_deletion) {
        return false;
    }

    return true;
}

void
node :: add_edge_unique(edge *e)
{
    out_edges[e->get_handle()] = std::vector<edge*>(1,e);
}

void
node :: add_edge(edge *e)
{
    auto iter = out_edges.find(e->get_handle());
    if (iter == out_edges.end()) {
        out_edges[e->get_handle()] = std::vector<edge*>(1, e);
    } else {
        if (iter->second.back()->base.get_del_time()) {
            // if not deleted, then this node was swapped in from HyperDex
            // in that case we don't need to perform the write
            iter->second.emplace_back(e);
        }
    }
}

bool
node :: edge_exists(const edge_handle_t &handle)
{
    assert(base.view_time != nullptr);
    assert(base.time_oracle != nullptr);
    auto find_iter = out_edges.find(handle);
    if (find_iter != out_edges.end()) {
        for (edge *e: find_iter->second) {
            if (base.time_oracle->clock_creat_before_del_after(*base.view_time, e->base.get_creat_time(), e->base.get_del_time())) {
                return true;
            }
        }
        return false;
    } else {
        return false;
    }
}

db::edge&
node :: get_edge(const edge_handle_t &handle)
{
    assert(base.view_time != nullptr);
    assert(base.time_oracle != nullptr);
    auto find_iter = out_edges.find(handle);
    if (find_iter != out_edges.end()) {
        for (edge *e: find_iter->second) {
            if (base.time_oracle->clock_creat_before_del_after(*base.view_time, e->base.get_creat_time(), e->base.get_del_time())) {
                e->base.view_time = base.view_time;
                e->base.time_oracle = base.time_oracle;
                return *e;
            }
        }
    }

    return edge::empty_edge;
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

bool
node :: has_all_predicates(std::vector<predicate::prop_predicate> &preds)
{
    assert(base.view_time != nullptr);
    assert(base.time_oracle != nullptr);
    return base.has_all_predicates(preds);
}

void
node :: add_alias(const node_handle_t &alias)
{
    aliases.insert(alias);
}

bool
node :: del_alias(const node_handle_t &alias)
{
    return (aliases.erase(alias) != 0);
}

bool
node :: is_alias(const node_handle_t &alias) const
{
    return (aliases.find(alias) != aliases.end());
}

void
node :: add_cache_value(vclock_ptr_t vc,
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

void
node :: get_client_node(cl::node &n, bool get_p, bool get_e, bool get_a)
{
    n.handle = base.get_handle();
    n.properties.clear();
    n.out_edges.clear();
    n.aliases.clear();

    if (get_p) {
        node_prog::prop_list plist = get_properties();
        for (std::vector<std::shared_ptr<node_prog::property>> pvec: plist) {
            n.properties.insert(n.properties.end(), pvec.begin(), pvec.end());
        }
    }

    if (get_e) {
        node_prog::edge_list elist = get_edges();
        for (node_prog::edge &e: elist) {
            std::string edge_handle = e.get_handle();
            e.get_client_edge(n.handle, n.out_edges[e.get_handle()]);
        }
    }

    if (get_a) {
        for (const node_handle_t &h: aliases) {
            n.aliases.emplace(h);
        }
    }
}
