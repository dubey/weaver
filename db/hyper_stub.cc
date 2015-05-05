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

using db::hyper_stub;

hyper_stub :: hyper_stub(uint64_t sid)
    : shard_id(sid)
{ }

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

    async_put_node apn;
    apn.handle = n->get_handle();
    prepare_node(apn.attrs,
                 *n,
                 apn.creat_clk_buf,
                 apn.props_buf,
                 apn.out_edges_buf,
                 last_clk_buf,
                 restore_clk_buf,
                 apn.aliases_buf);

    bool success = call_no_loop(&hyperdex_client_put,
                                graph_space,
                                apn.handle.c_str(),
                                apn.handle.size(),
                                apn.attrs,
                                NUM_GRAPH_ATTRS);

    if (success) {
        async_put_node_calls.emplace_back(std::move(apn));

        for (const std::string &alias: n->aliases) {
            success = add_index_no_loop(n->get_handle(), alias);
            if (!success) {
                break;
            }
        }
    }

    return success;
}

bool
hyper_stub :: put_edge_no_loop(const node_handle_t &node_handle, db::edge *e, const std::string &alias)
{
    async_put_edge ape;
    ape.node_handle = node_handle;
    ape.edge_handle = e->get_handle();
    prepare_edge(&ape.attr,
                 e,
                 ape.edge_handle,
                 ape.edge_buf);

    bool success = map_call_no_loop(&hyperdex_client_map_add,
                                    graph_space,
                                    ape.node_handle.c_str(),
                                    ape.node_handle.size(),
                                    &ape.attr, 1);

    if (success) {
        async_put_edge_calls.emplace_back(std::move(ape));

        if (!alias.empty()) {
            success = add_index_no_loop(node_handle, alias);
        }
    }

    return success;
}

bool
hyper_stub :: add_index_no_loop(const node_handle_t &node_handle, const std::string &alias)
{
    async_add_index aai;
    aai.node_handle = node_handle;
    aai.alias = alias;
    // node handle
    aai.index_attrs[0].attr = index_attrs[0];
    aai.index_attrs[0].value = aai.node_handle.c_str();
    aai.index_attrs[0].value_sz = aai.node_handle.size();
    aai.index_attrs[0].datatype = index_dtypes[0];
    // shard
    aai.index_attrs[1].attr = index_attrs[1];
    aai.index_attrs[1].value = (const char*)&shard_id;
    aai.index_attrs[1].value_sz = sizeof(int64_t);
    aai.index_attrs[1].datatype = index_dtypes[1];

    bool success = call_no_loop(&hyperdex_client_put,
                                index_space,
                                aai.alias.c_str(),
                                aai.alias.size(),
                                aai.index_attrs, NUM_INDEX_ATTRS);

    if (success) {
        async_add_index_calls.emplace_back(std::move(aai));
    }

    return success;
}

bool
hyper_stub :: loop_async_calls()
{
    uint64_t num_loops = async_put_node_calls.size() +
                         async_put_edge_calls.size() +
                         async_add_index_calls.size();
    WDEBUG << this << "\tasync loops #" << num_loops << std::endl;
    bool success = multiple_loop(num_loops);
    async_put_node_calls.clear();
    async_put_edge_calls.clear();
    async_add_index_calls.clear();
    return success;
}

#undef weaver_debug_
