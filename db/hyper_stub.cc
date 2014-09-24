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
hyper_stub :: restore_backup(std::unordered_map<node_handle_t, element::node*> *nodes,
    std::unordered_map<node_handle_t, std::unordered_set<node_handle_t>> &edge_map,
    po6::threads::mutex *shard_mutexes)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;

    // node list
    const hyperdex_client_attribute_check attr_check = {nmap_attr, (const char*)&shard_id, sizeof(int64_t), nmap_dtype, HYPERPREDICATE_EQUALS};
    enum hyperdex_client_returncode search_status, loop_status;

    int64_t call_id = hyperdex_client_search(cl, nmap_space, &attr_check, 1, &search_status, &cl_attr, &num_attrs);
    if (call_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << call_id
               << ", status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl;
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl;
        return;
    }

    std::vector<node_handle_t> node_list;
    int node_idx, loop_id;
    bool loop_done = false;
    char *handle_str = (char*)malloc(sizeof(char)*128);
    size_t handle_sz = 128;
    size_t cur_sz;

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
        } else {
            // search_status is HYPERDEX_CLIENT_SUCCESS
            assert(num_attrs == 2); // node and shard
            if (strncmp(cl_attr[0].attr, nmap_attr, 5) == 0) {
                node_idx = 1;
            } else {
                node_idx = 0;
            }

            // null terminated copy of the obtained handle
            cur_sz = cl_attr[node_idx].value_sz + 1;
            if (handle_sz < cur_sz) {
                while(handle_sz < cur_sz) {
                    handle_sz *= 2;
                }
                handle_str = (char*)realloc(handle_str, handle_sz);
            }
            memcpy(handle_str, cl_attr[node_idx].value, cur_sz-1);
            handle_str[cur_sz-1] = '\0';

            node_list.emplace_back(node_handle_t(handle_str));

            hyperdex_client_destroy_attrs(cl_attr, num_attrs);
        }
    }

    free(handle_str);

    WDEBUG << "Got " << node_list.size() << " nodes for shard " << shard_id << std::endl;
    std::vector<const char*> spaces(node_list.size(), graph_space);
    std::vector<const char*> keys(node_list.size());
    std::vector<size_t> key_szs(node_list.size());
    std::vector<const hyperdex_client_attribute**> cl_attrs;
    std::vector<size_t*> attrs_sz;
    cl_attrs.reserve(node_list.size());
    attrs_sz.reserve(node_list.size());
    const hyperdex_client_attribute *cl_attr_array[node_list.size()];
    size_t attr_sz_array[node_list.size()];
    for (uint64_t i = 0; i < node_list.size(); i++) {
        keys[i] = node_list[i].c_str();
        key_szs[i] = node_list[i].size();
        cl_attrs.emplace_back(cl_attr_array + i);
        attrs_sz.emplace_back(attr_sz_array + i);
    }

    multiple_get(spaces, keys, key_szs, cl_attrs, attrs_sz, false);

    vc::vclock dummy_clock;
    element::node *n;
    uint64_t map_idx;
    for (uint64_t i = 0; i < node_list.size(); i++) {
        assert(attr_sz_array[i] == NUM_GRAPH_ATTRS);

        const node_handle_t &node_handle = node_list[i];
        map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;
        n = new element::node(node_handle, dummy_clock, shard_mutexes+map_idx);

        recreate_node(cl_attr_array[i], *n);

        // edge map
        for (const auto &p: n->out_edges) {
            edge_map[p.second->nbr.handle].emplace(node_handle);
        }

        // node map
        auto &node_map = nodes[map_idx];
        assert(node_map.find(node_handle) == node_map.end());
        node_map.emplace(node_handle, n);

        hyperdex_client_destroy_attrs(cl_attr_array[i], attr_sz_array[i]);
    }
}

void
hyper_stub :: bulk_load(int tid, std::unordered_map<node_handle_t, element::node*> *nodes_arr)
{
    assert(NUM_NODE_MAPS % NUM_SHARD_THREADS == 0);
    std::vector<node_handle_t> node_handles;
    for (; tid < NUM_NODE_MAPS; tid += NUM_SHARD_THREADS) {
        std::unordered_map<node_handle_t, element::node*> &node_map = nodes_arr[tid];
        node_handles.reserve(node_handles.size() + node_map.size());
        for (auto &p: node_map) {
            // TODO change when single space for mapping and graph data
            //put_mapping(p.first, shard_id);
            //put_node(*p.second);
            node_handles.emplace_back(p.first);
        }
        put_nodes_bulk(node_map);
    }

    WDEBUG << "put nmap " << node_handles.size() << std::endl;
    assert(put_nmap(node_handles, shard_id));
}

bool
hyper_stub :: update_mapping(const node_handle_t &handle, uint64_t loc)
{
    return update_nmap(handle, loc);
}

#undef weaver_debug_
