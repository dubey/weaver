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
    , graph_attrs{"creat_time",
        "del_time",
        "properties",
        "out_edges",
        "in_nbrs",
        "tx_queue",
        "migr_status"} // 0 for stable, 1 for moving
    , graph_dtypes{HYPERDATATYPE_STRING,
        HYPERDATATYPE_STRING,
        HYPERDATATYPE_STRING, // can change to map(int, string) to simulate vector with random access
        HYPERDATATYPE_MAP_STRING_STRING,
        HYPERDATATYPE_SET_STRING,
        HYPERDATATYPE_STRING,
        HYPERDATATYPE_INT64}
    , shard_attrs{"qts", "migr_token"}
    , shard_dtypes{HYPERDATATYPE_MAP_INT64_INT64,
        HYPERDATATYPE_INT64}
{ }

void
hyper_stub :: init()
{
    std::unordered_map<uint64_t, uint64_t> qts_map;

    for (uint64_t vt_id = 0; vt_id < NumVts; vt_id++) {
        qts_map.emplace(vt_id, 0);
    }
    std::unique_ptr<e::buffer> qts_buf;
    prepare_buffer(qts_map, qts_buf);
    int64_t migr_token = (int64_t)INACTIVE;

    hyperdex_client_attribute cl_attr[NUM_SHARD_ATTRS];
    cl_attr[0].attr = shard_attrs[0];
    cl_attr[0].value = (const char*)qts_buf->data();
    cl_attr[0].value_sz = qts_buf->size();
    cl_attr[0].datatype = shard_dtypes[0];
    cl_attr[1].attr = shard_attrs[1];
    cl_attr[1].value = (const char*)&migr_token;
    cl_attr[1].value_sz = sizeof(int64_t);
    cl_attr[1].datatype = shard_dtypes[1];

    call(&hyperdex::Client::put, shard_space, (const char*)&shard_id, sizeof(int64_t), cl_attr, NUM_SHARD_ATTRS);
}

void
hyper_stub :: recreate_node(const hyperdex_client_attribute *cl_attr, element::node &n, std::unordered_set<node_handle_t> &nbr_map)
{
    std::vector<int> idx(NUM_GRAPH_ATTRS, -1);    
    for (int i = 0; i < NUM_GRAPH_ATTRS; i++) {
        for (int j = 0; j < NUM_GRAPH_ATTRS; j++) {
            if (strcmp(cl_attr[i].attr, graph_attrs[j]) == 0) {
                idx[j] = i;
                break;
            }
        }
    }
    std::vector<bool> check_idx(NUM_GRAPH_ATTRS, false);
    for (int i = 0; i < NUM_GRAPH_ATTRS; i++) {
        assert(idx[i] != -1);
        assert(!check_idx[idx[i]]);
        check_idx[idx[i]] = true;
    }

    // create clock
    vc::vclock create_clk;
    unpack_buffer(cl_attr[idx[0]].value, cl_attr[idx[0]].value_sz, create_clk);
    // delete clock
    vc::vclock delete_clk;
    unpack_buffer(cl_attr[idx[1]].value, cl_attr[idx[1]].value_sz, delete_clk);
    // properties
    std::vector<db::element::property> props;
    unpack_buffer(cl_attr[idx[2]].value, cl_attr[idx[2]].value_sz, props);

    n.state = element::node::mode::STABLE;
    n.in_use = false;
    n.base.update_creat_time(create_clk);
    n.base.update_del_time(delete_clk);
    n.base.set_properties(props);

    // out edges
    unpack_buffer(cl_attr[idx[3]].value, cl_attr[idx[3]].value_sz, n.out_edges);
    // in nbrs
    unpack_buffer(cl_attr[idx[4]].value, cl_attr[idx[4]].value_sz, nbr_map);
    // tx_queue
    unpack_buffer(cl_attr[idx[5]].value, cl_attr[idx[5]].value_sz, n.tx_queue);
}

void
hyper_stub :: restore_backup(std::unordered_map<uint64_t, uint64_t> &qts_map,
            bool &migr_token,
            std::unordered_map<node_handle_t, element::node*> *nodes,
            std::unordered_map<node_handle_t, std::unordered_set<node_handle_t>> &edge_map,
            po6::threads::mutex *shard_mutexes)
{
    // TODO everything sequential right now
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;

    // clocks
    get(shard_space, (const char*)&shard_id, sizeof(int64_t), &cl_attr, &num_attrs);
    assert(num_attrs == NUM_SHARD_ATTRS);
    for (uint64_t i = 0; i < num_attrs; i++) {
        assert(strcmp(cl_attr[i].attr, shard_attrs[i]) == 0);
    }

    std::vector<int64_t> idx_perm(NUM_SHARD_ATTRS, -1);
    for (int i = 0; i < NUM_SHARD_ATTRS; i++) {
        for (int j = 0; j < NUM_SHARD_ATTRS; j++) {
            if (strcmp(cl_attr[i].attr, shard_attrs[j]) == 0) {
                idx_perm[j] = i;
            }
        }
    }
    std::vector<bool> check_idx(NUM_SHARD_ATTRS, false);
    for (int i = 0; i < NUM_SHARD_ATTRS; i++) {
        assert(idx_perm[i] != -1);
        assert(!check_idx[idx_perm[i]]);
        check_idx[idx_perm[i]] = true;
    }

    unpack_buffer(cl_attr[idx_perm[0]].value, cl_attr[idx_perm[0]].value_sz, qts_map);
    assert(cl_attr[idx_perm[1]].value_sz == sizeof(int64_t));
    int64_t *m_token = (int64_t*)cl_attr[idx_perm[1]].value;
    migr_token = ((enum persist_migr_token)*m_token) == ACTIVE;

#ifdef weaver_debug_
    WDEBUG << "qts:" << std::endl;
    for (auto &x: qts_map) {
        std::cerr << x.first << " " << x.second << std::endl;
    }
#endif

    hyperdex_client_destroy_attrs(cl_attr, num_attrs);

    // node list
    const hyperdex_client_attribute_check attr_check = {nmap_attr, (const char*)&shard_id, sizeof(int64_t), nmap_dtype, HYPERPREDICATE_EQUALS};
    enum hyperdex_client_returncode status;

    int64_t hdex_id = cl.search(nmap_space, &attr_check, 1, &status, &cl_attr, &num_attrs);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << hdex_id << ", status = " << status << std::endl;
        return;
    }

    std::vector<node_handle_t> node_list;
    int node_idx;
    bool loop_done = false;
    while (!loop_done) {
        // loop until search done
        hdex_id = cl.loop(-1, &status);
        assert(status == HYPERDEX_CLIENT_SUCCESS || status == HYPERDEX_CLIENT_SEARCHDONE);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex function failed, op id = " << hdex_id << ", status = " << status << std::endl;
            return;
        }
        switch (status) {
            case HYPERDEX_CLIENT_SEARCHDONE:
                loop_done = true;
                break;

            case HYPERDEX_CLIENT_SUCCESS:
                assert(num_attrs == 2); // node and shard
                if (strncmp(cl_attr[0].attr, nmap_attr, 5) == 0) {
                    node_idx = 1;
                } else {
                    node_idx = 0;
                }
                node_list.emplace_back(node_handle_t(cl_attr[node_idx].value));
                hyperdex_client_destroy_attrs(cl_attr, num_attrs);
                break;

            default:
                WDEBUG << "should never reach here" << std::endl;
                loop_done = true;
                return;
        }
    }

    WDEBUG << "Got " << node_list.size() << " nodes for shard " << shard_id << std::endl;
    std::vector<const char*> spaces(node_list.size(), graph_space);
    std::vector<const char*> keys(node_list.size());
    std::vector<size_t> key_szs(node_list.size(), sizeof(int64_t));
    std::vector<const hyperdex_client_attribute**> cl_attrs;
    std::vector<size_t*> attrs_sz;
    cl_attrs.reserve(node_list.size());
    attrs_sz.reserve(node_list.size());
    const hyperdex_client_attribute *cl_attr_array[node_list.size()];
    size_t attr_sz_array[node_list.size()];
    for (uint64_t i = 0; i < node_list.size(); i++) {
        keys[i] = (const char*)&node_list[i];
        cl_attrs.emplace_back(cl_attr_array + i);
        attrs_sz.emplace_back(attr_sz_array + i);
    }

    multiple_get(spaces, keys, key_szs, cl_attrs, attrs_sz);

    vc::vclock dummy_clock;
    element::node *n;
    uint64_t map_idx;
    for (uint64_t i = 0; i < node_list.size(); i++) {
        assert(attr_sz_array[i] == NUM_GRAPH_ATTRS);

        const node_handle_t &node_handle = node_list[i];
        map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;
        n = new element::node(node_handle, dummy_clock, shard_mutexes+map_idx);

        assert(edge_map.find(node_handle) == edge_map.end());
        recreate_node(cl_attr_array[i], *n, edge_map[node_handle]);

        auto &node_map = nodes[map_idx];
        assert(node_map.find(node_handle) == node_map.end());
        node_map[node_handle] = n;

        hyperdex_client_destroy_attrs(cl_attr_array[i], attr_sz_array[i]);
    }
}

void
hyper_stub :: put_node(element::node &n, std::unordered_set<node_handle_t> &nbr_map)
{
    hyperdex_client_attribute cl_attr[NUM_GRAPH_ATTRS];

    // create clock
    std::unique_ptr<e::buffer> creat_clk_buf;
    prepare_buffer(n.base.get_creat_time(), creat_clk_buf);
    cl_attr[0].attr = graph_attrs[0];
    cl_attr[0].value = (const char*)creat_clk_buf->data();
    cl_attr[0].value_sz = creat_clk_buf->size();
    cl_attr[0].datatype = graph_dtypes[0];

    // delete clock
    std::unique_ptr<e::buffer> del_clk_buf;
    prepare_buffer(n.base.get_del_time(), del_clk_buf);
    cl_attr[1].attr = graph_attrs[1];
    cl_attr[1].value = (const char*)del_clk_buf->data();
    cl_attr[1].value_sz = del_clk_buf->size();
    cl_attr[1].datatype = graph_dtypes[1];

    // properties
    std::unique_ptr<e::buffer> props_buf;
    prepare_buffer(*n.base.get_props(), props_buf);
    cl_attr[2].attr = graph_attrs[2];
    cl_attr[2].value = (const char*)props_buf->data();
    cl_attr[2].value_sz = props_buf->size();
    cl_attr[2].datatype = graph_dtypes[2];

    // out edges
    std::unique_ptr<e::buffer> out_edges_buf;
    prepare_buffer<element::edge*>(n.out_edges, out_edges_buf);
    cl_attr[3].attr = graph_attrs[3];
    cl_attr[3].value = (const char*)out_edges_buf->data();
    cl_attr[3].value_sz = out_edges_buf->size();
    cl_attr[3].datatype = graph_dtypes[3];

    // in nbrs
    std::unique_ptr<e::buffer> in_nbrs_buf;
    prepare_buffer(nbr_map, in_nbrs_buf);
    cl_attr[4].attr = graph_attrs[4];
    cl_attr[4].value = (const char*)in_nbrs_buf->data();
    cl_attr[4].value_sz = in_nbrs_buf->size();
    cl_attr[4].datatype = graph_dtypes[4];

    // tx_queue
    std::unique_ptr<e::buffer> txq_buf;
    prepare_buffer(n.tx_queue, txq_buf);
    cl_attr[5].attr = graph_attrs[5];
    cl_attr[5].value = (const char*)txq_buf->data();
    cl_attr[5].value_sz = txq_buf->size();
    cl_attr[5].datatype = graph_dtypes[5];

    // migr status
    int64_t status = STABLE;
    cl_attr[6].attr = graph_attrs[6];
    cl_attr[6].value = (const char*)&status;
    cl_attr[6].value_sz = sizeof(int64_t);
    cl_attr[6].datatype = graph_dtypes[6];

    node_handle_t handle = n.get_handle();
    call(&hyperdex::Client::put, graph_space, handle.c_str(), handle.size(), cl_attr, NUM_GRAPH_ATTRS);
}

void
hyper_stub :: update_creat_time(element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> creat_clk_buf;
    prepare_buffer(n.base.get_creat_time(), creat_clk_buf);
    cl_attr.attr = graph_attrs[0];
    cl_attr.value = (const char*)creat_clk_buf->data();
    cl_attr.value_sz = creat_clk_buf->size();
    cl_attr.datatype = graph_dtypes[0];

    node_handle_t handle = n.get_handle();
    call(&hyperdex::Client::put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub :: del_node(element::node &n)
{
    node_handle_t handle = n.get_handle();
    del(graph_space, handle.c_str(), handle.size());
}

void
hyper_stub :: update_properties(element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> props_buf;
    prepare_buffer(*n.base.get_props(), props_buf);
    cl_attr.attr = graph_attrs[2];
    cl_attr.value = (const char*)props_buf->data();
    cl_attr.value_sz = props_buf->size();
    cl_attr.datatype = graph_dtypes[2];

    node_handle_t handle = n.get_handle();
    call(&hyperdex::Client::put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub :: add_out_edge(element::node &n, element::edge *e)
{
    hyperdex_client_map_attribute map_attr;
    std::unique_ptr<e::buffer> key_buf, val_buf;
    prepare_buffer(e->get_handle(), key_buf);
    prepare_buffer(e, val_buf);
    map_attr.attr = graph_attrs[3];
    map_attr.map_key = (const char*)key_buf->data();
    map_attr.map_key_sz = key_buf->size();
    map_attr.map_key_datatype = HYPERDATATYPE_STRING;
    map_attr.value = (const char*)val_buf->data();
    map_attr.value_sz = val_buf->size();
    map_attr.value_datatype = HYPERDATATYPE_STRING;

    node_handle_t handle = n.get_handle();
    map_call(&hyperdex::Client::map_add, graph_space, handle.c_str(), handle.size(), &map_attr, 1);
}

void
hyper_stub :: remove_out_edge(element::node &n, element::edge *e)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> key_buf;
    prepare_buffer(e->get_handle(), key_buf);
    cl_attr.attr = graph_attrs[3];
    cl_attr.value = (const char*)key_buf->data();
    cl_attr.value_sz = key_buf->size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    node_handle_t handle = n.get_handle();
    call(&hyperdex::Client::map_remove, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub :: add_in_nbr(const node_handle_t &n_hndl, const node_handle_t &nbr)
{
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[4];
    cl_attr.value = nbr.c_str();
    cl_attr.value_sz = nbr.size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    call(&hyperdex::Client::set_add, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}

void
hyper_stub :: remove_in_nbr(const node_handle_t &n_hndl, const node_handle_t &nbr)
{
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[4];
    cl_attr.value = nbr.c_str();
    cl_attr.value_sz = nbr.size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    call(&hyperdex::Client::set_remove, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}

void
hyper_stub :: update_tx_queue(element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> txq_buf;
    prepare_buffer(n.tx_queue, txq_buf);
    cl_attr.attr = graph_attrs[5];
    cl_attr.value = (const char*)txq_buf->data();
    cl_attr.value_sz = txq_buf->size();
    cl_attr.datatype = graph_dtypes[5];

    node_handle_t handle = n.get_handle();
    call(&hyperdex::Client::put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub :: update_migr_status(const node_handle_t &n_hndl, enum persist_node_state status)
{
    int64_t int_status = status;
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[6];
    cl_attr.value = (const char*)&int_status;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = graph_dtypes[6];

    call(&hyperdex::Client::put, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}

void
hyper_stub :: bulk_load(std::unordered_map<node_handle_t, element::node*> *nodes_arr,
    std::unordered_map<node_handle_t, std::unordered_set<node_handle_t>> &edge_map)
{
    uint64_t num_nodes = 0;
    for (int i = 0; i < NUM_NODE_MAPS; i++) {
        num_nodes += nodes_arr[i].size();
    }

    std::vector<hyper_func> funcs(num_nodes, &hyperdex::Client::put);
    std::vector<const char*> spaces(num_nodes, graph_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<hyperdex_client_attribute*> attrs(num_nodes, NULL);
    std::vector<size_t> num_attrs(num_nodes, NUM_GRAPH_ATTRS);

    hyperdex_client_attribute *attr_array = (hyperdex_client_attribute*)malloc(num_nodes * NUM_GRAPH_ATTRS * sizeof(hyperdex_client_attribute));
    hyperdex_client_attribute *cur_attr = attr_array;

    uint64_t idx = 0;
    for (int i = 0; i < NUM_NODE_MAPS; i++) {
        auto &nodes = nodes_arr[i];
        for (auto &node_pair: nodes) {
            const node_handle_t &node_handle = node_pair.first;
            element::node &n = *node_pair.second;
            hyperdex_client_attribute *cl_attr = cur_attr;
            cur_attr += NUM_GRAPH_ATTRS;

            // create clock
            std::unique_ptr<e::buffer> creat_clk_buf;
            prepare_buffer(n.base.get_creat_time(), creat_clk_buf);
            cl_attr[0].attr = graph_attrs[0];
            cl_attr[0].value = (const char*)creat_clk_buf->data();
            cl_attr[0].value_sz = creat_clk_buf->size();
            cl_attr[0].datatype = graph_dtypes[0];
            // delete clock
            std::unique_ptr<e::buffer> del_clk_buf;
            prepare_buffer(n.base.get_del_time(), del_clk_buf);
            cl_attr[1].attr = graph_attrs[1];
            cl_attr[1].value = (const char*)del_clk_buf->data();
            cl_attr[1].value_sz = del_clk_buf->size();
            cl_attr[1].datatype = graph_dtypes[1];
            // properties
            std::unique_ptr<e::buffer> props_buf;
            prepare_buffer(*n.base.get_props(), props_buf);
            cl_attr[2].attr = graph_attrs[2];
            cl_attr[2].value = (const char*)props_buf->data();
            cl_attr[2].value_sz = props_buf->size();
            cl_attr[2].datatype = graph_dtypes[2];
            // out edges
            std::unique_ptr<e::buffer> out_edges_buf;
            prepare_buffer<element::edge*>(n.out_edges, out_edges_buf);
            cl_attr[3].attr = graph_attrs[3];
            cl_attr[3].value = (const char*)out_edges_buf->data();
            cl_attr[3].value_sz = out_edges_buf->size();
            cl_attr[3].datatype = graph_dtypes[3];
            // in nbrs
            std::unique_ptr<e::buffer> in_nbrs_buf;
            prepare_buffer(edge_map[node_handle], in_nbrs_buf);
            cl_attr[4].attr = graph_attrs[4];
            cl_attr[4].value = (const char*)in_nbrs_buf->data();
            cl_attr[4].value_sz = in_nbrs_buf->size();
            cl_attr[4].datatype = graph_dtypes[4];
            // tx_queue
            std::unique_ptr<e::buffer> txq_buf;
            prepare_buffer(n.tx_queue, txq_buf);
            cl_attr[5].attr = graph_attrs[5];
            cl_attr[5].value = (const char*)txq_buf->data();
            cl_attr[5].value_sz = txq_buf->size();
            cl_attr[5].datatype = graph_dtypes[5];

            keys[idx] = node_handle.c_str();
            key_szs[idx] = node_handle.size();
            attrs[idx++] = cl_attr;
        }
    }

    multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);

    free(attr_array);
}

void
hyper_stub :: increment_qts(uint64_t vt_id, uint64_t incr)
{
    hyperdex_client_map_attribute map_attr;
    map_attr.attr = shard_attrs[0];
    map_attr.map_key = (const char*)&vt_id;
    map_attr.map_key_sz = sizeof(int64_t);
    map_attr.map_key_datatype = HYPERDATATYPE_INT64;
    map_attr.value = (const char*)&incr;
    map_attr.value_sz = sizeof(int64_t);
    map_attr.value_datatype = HYPERDATATYPE_INT64;

    map_call(&hyperdex::Client::map_atomic_add, shard_space, (const char*)&shard_id, sizeof(int64_t), &map_attr, 1);
}

void
hyper_stub :: update_migr_token(enum persist_migr_token token)
{
    int64_t int_token = (int64_t)token;
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = shard_attrs[1];
    cl_attr.value = (const char*)&int_token;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = shard_dtypes[1];

    call(&hyperdex::Client::put, shard_space, (const char*)&shard_id, sizeof(int64_t), &cl_attr, 1);
}

#undef weaver_debug_
