/*
 * ===============================================================
 *    Description:  Shard hyperdex stub implementation.
 *
 *        Created:  2014-02-18 15:32:42
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "db/shard_constants.h"
#include "db/hyper_stub.h"

#define PUT_EDGE_BATCH_SZ 1000
#define FLUSH_CALL_SZ 10000

using db::hyper_stub;
using db::apn_ptr_t;
using db::ape_ptr_t;
using db::aai_ptr_t;

hyper_stub :: hyper_stub(uint64_t sid)
    : shard_id(sid)
    , put_edge_batch_clkhand(0)
    , apn_count(0)
    , ape_count(0)
    , aai_count(0)
{
    apn_pool.reserve(FLUSH_CALL_SZ*10);
    ape_pool.reserve(FLUSH_CALL_SZ*10);
    aai_pool.reserve(FLUSH_CALL_SZ*10);
    for (uint32_t i = 0; i < FLUSH_CALL_SZ*10; i++) {
        apn_ptr_t apn = std::make_shared<async_put_node>();
        apn_pool.emplace_back(apn);
        ape_ptr_t ape = std::make_shared<async_put_edge>();
        ape_pool.emplace_back(ape);
        aai_ptr_t aai = std::make_shared<async_add_index>();
        aai_pool.emplace_back(aai);
    }
    apn_pool_sz = FLUSH_CALL_SZ*10;
    ape_pool_sz = FLUSH_CALL_SZ*10;
    aai_pool_sz = FLUSH_CALL_SZ*10;

    for (uint32_t i = 0; i < PUT_EDGE_BATCH_SZ; i++) {
        put_edge_batch.emplace_back(acquire_ape_ptr());
    }
}

void
hyper_stub :: restore_backup(db::data_map<std::shared_ptr<db::node_entry>> *nodes,
    /*XXX std::unordered_map<node_handle_t, std::unordered_set<node_version_t, node_version_hash>> &edge_map,*/
    po6::threads::mutex *shard_mutexes)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;

    // node list
    const hyperdex_client_attribute_check attr_check = {graph_attrs[0], (const char*)&shard_id, sizeof(int64_t), graph_dtypes[0], HYPERPREDICATE_EQUALS};
    enum hyperdex_client_returncode search_status, loop_status;

    int64_t call_id = hyperdex_client_search(cl, graph_space, &attr_check, 1, &search_status, &cl_attr, &num_attrs);
    if (call_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << call_id
               << ", status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl;
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl;
        return;
    }

    std::vector<node_handle_t> node_list;
    int loop_id;
    bool loop_done = false;
    char *handle_str = (char*)malloc(sizeof(char)*128);
    size_t handle_sz = 128;
    size_t cur_sz;
    node_handle_t node_handle;
    vc::vclock_ptr_t dummy_clock;
    node *n;
    uint64_t map_idx;

    while (!loop_done) {
        // loop until search done
        loop_id = hyperdex_client_loop(cl, -1, &loop_status);
        if (loop_id != call_id
         || loop_status != HYPERDEX_CLIENT_SUCCESS
         || (search_status != HYPERDEX_CLIENT_SUCCESS && search_status != HYPERDEX_CLIENT_SEARCHDONE)) {
            WDEBUG << "Hyperdex function failed, call id = " << call_id
                   << ", loop_id = " << loop_id
                   << ", loop status = " << hyperdex_client_returncode_to_string(loop_status)
                   << ", search status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
            WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl;
            WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl;
            return;
        }

        if (search_status == HYPERDEX_CLIENT_SEARCHDONE) {
            loop_done = true;
        } else if (search_status == HYPERDEX_CLIENT_SUCCESS) {
            assert(num_attrs == NUM_GRAPH_ATTRS+1); // node handle + graph attrs

            uint64_t key_idx = UINT64_MAX;
            hyperdex_client_attribute node_attrs[NUM_GRAPH_ATTRS];
            for (uint64_t i = 0, j = 0; i < num_attrs; i++) {
                if (strncmp(cl_attr[i].attr, graph_key, 4) == 0) {
                    key_idx = i;
                } else {
                    node_attrs[j++] = cl_attr[i];
                }
            }
            assert(key_idx != UINT64_MAX);

            // null terminated copy of the obtained handle
            cur_sz = cl_attr[key_idx].value_sz + 1;
            if (handle_sz < cur_sz) {
                while(handle_sz < cur_sz) {
                    handle_sz *= 2;
                }
                handle_str = (char*)realloc(handle_str, handle_sz);
            }
            memcpy(handle_str, cl_attr[key_idx].value, cur_sz-1);
            handle_str[cur_sz-1] = '\0';
            node_handle = node_handle_t(handle_str);

            // recreate node
            map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;
            n = new node(node_handle, UINT64_MAX, dummy_clock, shard_mutexes+map_idx);
            recreate_node(node_attrs, *n);

            //XXX edge map
            //for (const auto &p: n->out_edges) {
            //    assert(p.second.size() == 1);
            //    edge *e = p.second.front();
            //    edge_map[e->nbr.handle].emplace(std::make_pair(node_handle, e->base.get_creat_time()));
            //}

            // node map
            auto &node_map = nodes[map_idx];
            assert(node_map.find(node_handle) == node_map.end());
            node_map[node_handle] = std::make_shared<db::node_entry>(n);

            hyperdex_client_destroy_attrs(cl_attr, num_attrs);
        } else {
            WDEBUG << "unexpected search status " << search_status << std::endl;
        }
    }

    free(handle_str);
}

void
hyper_stub :: put_node_loop(db::data_map<std::shared_ptr<db::node_entry>> &nodes,
    std::unordered_map<node_handle_t, node*> &node_map,
    int &progress,
    std::shared_ptr<vc::vclock> last_upd_clk,
    std::shared_ptr<vc::vclock_t> restore_clk)
{
    for (const auto &p: nodes) {
        auto &nodes_vec = p.second->nodes;
        assert(nodes_vec.size() <= 1);
        if (!nodes_vec.empty()) {
            node_map.emplace(p.first, nodes_vec.front());
            if (node_map.size() >= 1000) {
                put_nodes_bulk(node_map, last_upd_clk, restore_clk);
                node_map.clear();
                WDEBUG << "bulk load hyperdex progress " << ++progress << std::endl;
            }
        }
    }
}

void
hyper_stub :: put_index_loop(db::data_map<std::shared_ptr<db::node_entry>> &nodes,
    std::unordered_map<std::string, node*> &idx_add_if_not_exist,
    std::unordered_map<std::string, node*> &idx_add,
    int &ine_progress,
    int &progress)
{
    for (const auto &p: nodes) {
        auto &nodes_vec = p.second->nodes;
        if (!nodes_vec.empty()) {
            node *n = nodes_vec.front();
            idx_add_if_not_exist.emplace(p.first, n);
            for (const node_handle_t &alias: n->aliases) {
                assert(idx_add_if_not_exist.find(alias) == idx_add_if_not_exist.end());
                idx_add_if_not_exist.emplace(alias, n);
            }

            for (auto &x: n->out_edges) {
                assert(x.second.size() == 1);
                assert(idx_add_if_not_exist.find(x.first) == idx_add_if_not_exist.end());
                idx_add_if_not_exist.emplace(x.first, n);
            }

            //if (n->temp_aliases != nullptr) {
            //    for (const std::string &s: *n->temp_aliases) {
            //        idx_add.emplace(s, n);
            //    }
            //    n->done_temp_index();
            //}

            if (idx_add_if_not_exist.size() >= 10000) {
                add_indices(idx_add_if_not_exist, false, true);
                idx_add_if_not_exist.clear();
                WDEBUG << "aux index if not exist progress " << ++ine_progress << std::endl;
            }
            if (idx_add.size() >= 10000) {
                add_indices(idx_add, false, false);
                idx_add.clear();
                WDEBUG << "aux index progress " << ++progress << std::endl;
            }
        }
    }
}

void
hyper_stub :: memory_efficient_bulk_load(int thread_id, db::data_map<std::shared_ptr<db::node_entry>> *nodes_arr)
{
    WDEBUG << "starting HyperDex bulk load." << std::endl;
    std::shared_ptr<vc::vclock> last_upd_clk(new vc::vclock(0,0));
    std::shared_ptr<vc::vclock_t> restore_clk(new vc::vclock_t(last_upd_clk->clock));

    std::unordered_map<node_handle_t, node*> node_map;
    int progress = 0;
    for (int tid = thread_id; tid < NUM_NODE_MAPS; tid += NUM_SHARD_THREADS) {
        put_node_loop(nodes_arr[tid], node_map, progress, last_upd_clk, restore_clk);
    }

    put_nodes_bulk(node_map, last_upd_clk, restore_clk);

    progress = 0;
    int ine_progress = 0;
    if (AuxIndex) {
        std::unordered_map<std::string, node*> idx_add_if_not_exist;
        std::unordered_map<std::string, node*> idx_add;

        for (int tid = thread_id; tid < NUM_NODE_MAPS; tid += NUM_SHARD_THREADS) {
            put_index_loop(nodes_arr[tid], idx_add_if_not_exist, idx_add, ine_progress, progress);
        }

        add_indices(idx_add_if_not_exist, false, true);
        add_indices(idx_add, false, false);
    }

    WDEBUG << "bulk load done." << std::endl;
}

void
hyper_stub :: memory_efficient_bulk_load(db::data_map<std::shared_ptr<db::node_entry>> &nodes)
{
    std::shared_ptr<vc::vclock> last_upd_clk(new vc::vclock(0,0));
    std::shared_ptr<vc::vclock_t> restore_clk(new vc::vclock_t(last_upd_clk->clock));

    std::unordered_map<node_handle_t, node*> node_map;
    int progress = 0;
    put_node_loop(nodes, node_map, progress, last_upd_clk, restore_clk);
    put_nodes_bulk(node_map, last_upd_clk, restore_clk);

    progress = 0;
    int ine_progress = 0;
    if (AuxIndex) {
        std::unordered_map<std::string, node*> idx_add_if_not_exist;
        std::unordered_map<std::string, node*> idx_add;

        put_index_loop(nodes, idx_add_if_not_exist, idx_add, ine_progress, progress);

        add_indices(idx_add_if_not_exist, false, true);
        add_indices(idx_add, false, false);
    }
}

void
hyper_stub :: bulk_load(int thread_id, std::unordered_map<node_handle_t, std::vector<node*>> *nodes_arr)
{
    assert(NUM_NODE_MAPS % NUM_SHARD_THREADS == 0);
    std::shared_ptr<vc::vclock> last_upd_clk(new vc::vclock(0,0));
    std::shared_ptr<vc::vclock_t> restore_clk(new vc::vclock_t(last_upd_clk->clock));

    std::unordered_map<node_handle_t, node*> node_map;
    for (int tid = thread_id; tid < NUM_NODE_MAPS; tid += NUM_SHARD_THREADS) {
        for (const auto &p: nodes_arr[tid]) {
            assert(p.second.size() == 1);
            node_map.emplace(p.first, p.second.front());
        }
    }

    put_nodes_bulk(node_map, last_upd_clk, restore_clk);

    if (AuxIndex) {
        std::unordered_map<std::string, node*> idx_add = node_map;
        for (auto &p: node_map) {
            for (const node_handle_t &alias: p.second->aliases) {
                assert(idx_add.find(alias) == idx_add.end());
                idx_add.emplace(alias, p.second);
            }

            for (auto &x: p.second->out_edges) {
                assert(x.second.size() == 1);
                assert(idx_add.find(x.first) == idx_add.end());
                idx_add.emplace(x.first, p.second);
            }
        }

        add_indices(idx_add, false, true);
    }
}

bool
hyper_stub :: update_mapping(const node_handle_t &handle, uint64_t loc)
{
    return update_nmap(handle, loc);
}

bool
hyper_stub :: recover_node(db::node &n)
{
    std::unordered_map<node_handle_t, db::node*> nodes;
    nodes.emplace(n.get_handle(), &n);
    return get_nodes(nodes, false);
}

apn_ptr_t
hyper_stub :: acquire_apn_ptr()
{
    apn_ptr_t ret;
    if (apn_pool.empty()) {
        for (uint32_t i = 0; i < FLUSH_CALL_SZ*5; i++) {
            apn_ptr_t new_ptr= std::make_shared<async_put_node>();
            apn_pool.emplace_back(new_ptr);
        }
        apn_pool_sz += FLUSH_CALL_SZ*5;
        WDEBUG << "apn pool sz=" << apn_pool_sz << std::endl;
    }
    ret = apn_pool.back();
    apn_pool.pop_back();
    return ret;
}

void
hyper_stub :: release_apn_ptr(apn_ptr_t ret)
{
    apn_pool.emplace_back(ret);
}

ape_ptr_t
hyper_stub :: acquire_ape_ptr()
{
    ape_ptr_t ret;
    if (ape_pool.empty()) {
        for (uint32_t i = 0; i < FLUSH_CALL_SZ*5; i++) {
            ape_ptr_t new_ptr= std::make_shared<async_put_edge>();
            ape_pool.emplace_back(new_ptr);
        }
        ape_pool_sz += FLUSH_CALL_SZ*5;
        WDEBUG << "ape pool sz=" << ape_pool_sz << std::endl;
    }
    ret = ape_pool.back();
    ape_pool.pop_back();
    ret->reset();
    return ret;
}

void
hyper_stub :: release_ape_ptr(ape_ptr_t ret)
{
    ret->batched.clear();
    free(ret->attr);
    ape_pool.emplace_back(ret);
}

aai_ptr_t
hyper_stub :: acquire_aai_ptr()
{
    aai_ptr_t ret;
    if (aai_pool.empty()) {
        for (uint32_t i = 0; i < FLUSH_CALL_SZ*5; i++) {
            aai_ptr_t new_ptr= std::make_shared<async_add_index>();
            aai_pool.emplace_back(new_ptr);
        }
        aai_pool_sz += FLUSH_CALL_SZ*5;
        WDEBUG << "aai pool sz=" << aai_pool_sz << std::endl;
    }
    ret = aai_pool.back();
    aai_pool.pop_back();
    return ret;
}

void
hyper_stub :: release_aai_ptr(aai_ptr_t ret)
{
    aai_pool.emplace_back(ret);
}

bool
hyper_stub :: put_node_no_loop(db::node *n)
{
    if (last_clk_buf == nullptr) {
        std::shared_ptr<vc::vclock> last_upd_clk(new vc::vclock(0,0));
        std::shared_ptr<vc::vclock_t> restore_clk(new vc::vclock_t(last_upd_clk->clock));
        prepare_buffer(last_upd_clk, last_clk_buf);
        prepare_buffer(restore_clk, restore_clk_buf);
    }
    assert(last_clk_buf && restore_clk_buf);

    apn_ptr_t apn = acquire_apn_ptr();
    apn->handle = n->get_handle();
    prepare_node(apn->attrs,
                 *n,
                 apn->creat_clk_buf,
                 apn->props_buf,
                 apn->out_edges_buf,
                 last_clk_buf,
                 restore_clk_buf,
                 apn->aliases_buf,
                 apn->num_attrs);

    bool success = call_no_loop(&hyperdex_client_put,
                                graph_space,
                                apn->handle.c_str(),
                                apn->handle.size(),
                                apn->attrs,
                                apn->num_attrs,
                                apn->op_id);

    if (success) {
        apn_count++;
        async_put_node_calls[apn->op_id] = apn;
        //async_put_node_calls.emplace_back(std::move(apn));

        for (const std::string &alias: n->aliases) {
            success = add_index_no_loop(n->get_handle(), alias) && success;
        }
    }

    return success;
}

bool
hyper_stub :: flush_put_edge(uint32_t evict_idx)
{
    ape_ptr_t ape = put_edge_batch[evict_idx];
    ape->attr = (hyperdex_client_map_attribute*)malloc(ape->batched.size() * sizeof(hyperdex_client_map_attribute));
    prepare_edges(ape->attr, ape->batched);

    bool success;
    success = map_call_no_loop(&hyperdex_client_map_add,
                               graph_space,
                               ape->node_handle.c_str(),
                               ape->node_handle.size(),
                               ape->attr,
                               ape->batched.size(),
                               ape->op_id);

    if (success) {
        for (const auto &apeu: ape->batched) {
            if (!apeu.alias.empty()) {
                success = add_index_no_loop(ape->node_handle, apeu.alias) && success;
            }

            if (apeu.del_after_call) {
                delete apeu.e;
            }
        }

        ape_count++;
        async_put_edge_calls[ape->op_id] = ape;
        //async_put_edge_calls.emplace_back(ape);
        put_edge_batch[evict_idx] = acquire_ape_ptr();
    }

    assert(put_edge_batch[evict_idx]->batched.empty());
    put_edge_batch[evict_idx]->used = false;

    return success;
}

bool
hyper_stub :: put_edge_no_loop(const node_handle_t &node_handle, db::edge *e, const std::string &alias, bool del_after_call)
{
    // search for this batch in cache
    uint32_t found_idx = UINT32_MAX;
    for (uint32_t i = 0; i < PUT_EDGE_BATCH_SZ; i++) {
        const ape_ptr_t ape = put_edge_batch[i];
        if (ape->node_handle == node_handle) {
            found_idx = i;
            break;
        }
    }

    uint32_t free_idx = UINT32_MAX;
    uint32_t evict_idx = UINT32_MAX;
    if (found_idx == UINT32_MAX) {
        // find slot for this entry
        // may have to evict
        uint32_t i;
        for (i = 0; i < PUT_EDGE_BATCH_SZ; i++) {
            uint32_t idx = (put_edge_batch_clkhand+i) % PUT_EDGE_BATCH_SZ;
            ape_ptr_t ape = put_edge_batch[idx];
            if (ape->batched.empty()) {
                free_idx = idx;
                put_edge_batch_clkhand = (idx+1) % PUT_EDGE_BATCH_SZ;
                break;
            } else if (!ape->used) {
                evict_idx = idx;
                put_edge_batch_clkhand = (idx+1) % PUT_EDGE_BATCH_SZ;
                break;
            } else {
                ape->used = false;
            }
        }

        // no free slot, evict the clock hand
        if (free_idx == UINT32_MAX && evict_idx == UINT32_MAX) {
            evict_idx = put_edge_batch_clkhand;
            ++put_edge_batch_clkhand;
        }
    }

    assert(found_idx != UINT32_MAX 
        || free_idx != UINT32_MAX
        || evict_idx != UINT32_MAX);

    bool success = true;
    uint32_t use_idx;
    if (found_idx != UINT32_MAX) {
        use_idx = found_idx;
    } else if (free_idx != UINT32_MAX) {
        use_idx = free_idx;
    } else {
        use_idx = evict_idx;

        // flush this batch to HyperDex
        success = flush_put_edge(evict_idx);
    }

    ape_ptr_t cur_ape = put_edge_batch[use_idx];
    cur_ape->used = true;
    cur_ape->node_handle = node_handle;
    async_put_edge_unit cur_apeu;
    cur_apeu.e = e;
    cur_apeu.edge_handle = e->get_handle();
    cur_apeu.alias = alias;
    cur_apeu.del_after_call = del_after_call;
    cur_ape->batched.emplace_back(std::move(cur_apeu));

    return success;
}

bool
hyper_stub :: add_index_no_loop(const node_handle_t &node_handle, const std::string &alias)
{
    aai_ptr_t aai = acquire_aai_ptr();
    aai->node_handle = node_handle;
    aai->alias = alias;
    // node handle
    aai->index_attrs[0].attr = index_attrs[0];
    aai->index_attrs[0].value = aai->node_handle.c_str();
    aai->index_attrs[0].value_sz = aai->node_handle.size();
    aai->index_attrs[0].datatype = index_dtypes[0];
    // shard
    aai->index_attrs[1].attr = index_attrs[1];
    aai->index_attrs[1].value = (const char*)&shard_id;
    aai->index_attrs[1].value_sz = sizeof(int64_t);
    aai->index_attrs[1].datatype = index_dtypes[1];

    bool success = call_no_loop(&hyperdex_client_put,
                                index_space,
                                aai->alias.c_str(),
                                aai->alias.size(),
                                aai->index_attrs, NUM_INDEX_ATTRS,
                                aai->op_id);

    if (success) {
        aai_count++;
        async_add_index_calls[aai->op_id] = aai;
        //async_add_index_calls.emplace_back(std::move(aai));
    }

    return success;
}

bool
hyper_stub :: flush_all_put_edge()
{
    bool success;

    for (uint32_t i = 0; i < PUT_EDGE_BATCH_SZ; i++) {
        if (!put_edge_batch[i]->batched.empty()) {
            success = flush_put_edge(i) && success;
        }
    }

    return success;
}

#define SUCCESS_LOOP(calls_map, release_func, count) \
    if (calls_map.find(op_id) != calls_map.end()) { \
        found = true; \
        release_func(calls_map[op_id]); \
        calls_map.erase(op_id); \
        count--; \
    }

bool
hyper_stub :: loop_async(uint64_t loops)
{
    bool success = true;

    for (uint64_t i = 0; i < loops; i++) {
        int64_t op_id;
        hyperdex_client_returncode code;

        if(loop(op_id, code)) {
            bool found = false;
            SUCCESS_LOOP(async_put_node_calls, release_apn_ptr, apn_count);
            SUCCESS_LOOP(async_put_edge_calls, release_ape_ptr, ape_count);
            SUCCESS_LOOP(async_add_index_calls, release_aai_ptr, aai_count);
            assert(found);
        } else {
            bool found = false;
            if (async_put_node_calls.find(op_id) != async_put_node_calls.end()) {
                found = true;
                apn_ptr_t apn = async_put_node_calls[op_id];
                async_put_node_calls.erase(op_id);

                if (code == HYPERDEX_CLIENT_COORDFAIL
                 || code == HYPERDEX_CLIENT_SERVERERROR
                 || code == HYPERDEX_CLIENT_RECONFIGURE
                 || code == HYPERDEX_CLIENT_INTERRUPTED
                 || code == HYPERDEX_CLIENT_CLUSTER_JUMP
                 || code == HYPERDEX_CLIENT_OFFLINE) {
                    bool cur_success = call_no_loop(&hyperdex_client_put,
                                                    graph_space,
                                                    apn->handle.c_str(),
                                                    apn->handle.size(),
                                                    apn->attrs,
                                                    apn->num_attrs,
                                                    apn->op_id);
                    if (cur_success) {
                        async_put_node_calls[apn->op_id] = apn;
                    } else {
                        apn_count--;
                        success = false;
                        release_apn_ptr(apn);
                    }
                } else {
                    success = false;
                    release_apn_ptr(apn);
                }

            } else if (async_put_edge_calls.find(op_id) != async_put_edge_calls.end()) {
                found = true;
                ape_ptr_t ape = async_put_edge_calls[op_id];
                async_put_edge_calls.erase(op_id);

                if (code == HYPERDEX_CLIENT_COORDFAIL
                 || code == HYPERDEX_CLIENT_SERVERERROR
                 || code == HYPERDEX_CLIENT_RECONFIGURE
                 || code == HYPERDEX_CLIENT_INTERRUPTED
                 || code == HYPERDEX_CLIENT_CLUSTER_JUMP
                 || code == HYPERDEX_CLIENT_OFFLINE) {
                    bool cur_success = map_call_no_loop(&hyperdex_client_map_add,
                                                        graph_space,
                                                        ape->node_handle.c_str(),
                                                        ape->node_handle.size(),
                                                        ape->attr,
                                                        ape->batched.size(),
                                                        ape->op_id);

                    if (cur_success) {
                        async_put_edge_calls[ape->op_id] = ape;
                    } else {
                        ape_count--;
                        success = false;
                        release_ape_ptr(ape);
                    }
                } else {
                    success = false;
                    release_ape_ptr(ape);
                }
            } else if (async_add_index_calls.find(op_id) != async_add_index_calls.end()) {
                found = true;
                aai_ptr_t aai = async_add_index_calls[op_id];
                async_add_index_calls.erase(op_id);

                if (code == HYPERDEX_CLIENT_COORDFAIL
                 || code == HYPERDEX_CLIENT_SERVERERROR
                 || code == HYPERDEX_CLIENT_RECONFIGURE
                 || code == HYPERDEX_CLIENT_INTERRUPTED
                 || code == HYPERDEX_CLIENT_CLUSTER_JUMP
                 || code == HYPERDEX_CLIENT_OFFLINE) {
                    bool cur_success = call_no_loop(&hyperdex_client_put,
                                                    index_space,
                                                    aai->alias.c_str(),
                                                    aai->alias.size(),
                                                    aai->index_attrs, NUM_INDEX_ATTRS,
                                                    aai->op_id);

                    if (cur_success) {
                        async_add_index_calls[aai->op_id] = aai;
                    } else {
                        aai_count--;
                        success = false;
                        release_aai_ptr(aai);
                    }
                } else {
                    success = false;
                    release_aai_ptr(aai);
                }
            }
            if (op_id >= 0) {
                assert(found);
            }
        }
    }

    WDEBUG << "#apn=" << apn_count << std::endl;
    WDEBUG << "#ape=" << ape_count << std::endl;
    WDEBUG << "#aai=" << aai_count << std::endl;

    return success;
}

bool
hyper_stub :: loop_async_calls(bool flush)
{
    // flush first ~50% calls
    uint64_t apn_calls = async_put_node_calls.size();
    uint64_t ape_calls = async_put_edge_calls.size();
    uint64_t aai_calls = async_add_index_calls.size();

    bool success = true;

    if (flush) {
        success = loop_async(apn_calls) && success;

        success = loop_async(ape_calls) && success;

        success = loop_async(aai_calls) && success;

        apn_pool.clear();
        ape_pool.clear();
        aai_pool.clear();
    } else {
        if (apn_calls > FLUSH_CALL_SZ) {
            uint32_t num_loop = apn_calls - FLUSH_CALL_SZ/2;
            success = loop_async(num_loop) && success;
        }

        if (ape_calls > FLUSH_CALL_SZ) {
            uint32_t num_loop = ape_calls - FLUSH_CALL_SZ/2;
            success = loop_async(num_loop) && success;
        }

        if (aai_calls > FLUSH_CALL_SZ) {
            uint32_t num_loop = aai_calls - FLUSH_CALL_SZ/2;
            success = loop_async(num_loop) && success;
        }
    }

    return success;
}

#undef SUCCESS_LOOP

#undef weaver_debug_
