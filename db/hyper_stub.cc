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

#include "db/hyper_stub.h"

using db::hyper_stub;

hyper_stub :: hyper_stub(uint64_t sid)
    : shard_id(sid)
    , graph_attrs{"creat_time",
        "del_time",
        "properties",
        "out_edges",
        "in_nbrs"}
    , graph_dtypes{HYPERDATATYPE_STRING,
        HYPERDATATYPE_STRING,
        HYPERDATATYPE_STRING, // can change to map(int, string) to simulate vector with random access
        HYPERDATATYPE_MAP_INT64_STRING,
        HYPERDATATYPE_SET_INT64}
    , shard_attrs{"qts", "last_clocks"}
    , shard_dtypes{HYPERDATATYPE_MAP_INT64_INT64, HYPERDATATYPE_MAP_INT64_STRING}
{ }

void
hyper_stub :: init()
{
    vc::vclock_t zero_clk(NUM_VTS, 0);
    std::unordered_map<uint64_t, uint64_t> qts_map;
    std::unordered_map<uint64_t, vc::vclock_t> last_clocks;

    for (uint64_t vt_id = 0; vt_id < NUM_VTS; vt_id++) {
        qts_map.emplace(vt_id, 0);
        last_clocks.emplace(vt_id, zero_clk);
    }
    std::unique_ptr<char> qts_buf, lck_buf;
    uint64_t qts_buf_sz, lck_buf_sz;
    prepare_buffer(qts_map, qts_buf, qts_buf_sz);
    prepare_buffer(last_clocks, lck_buf, lck_buf_sz);

    hyperdex_client_attribute *cl_attr = (hyperdex_client_attribute*)malloc(2 * sizeof(hyperdex_client_attribute));
    cl_attr[0].attr = shard_attrs[0];
    cl_attr[0].value = qts_buf.get();
    cl_attr[0].value_sz = qts_buf_sz;
    cl_attr[0].datatype = shard_dtypes[0];
    cl_attr[1].attr = shard_attrs[1];
    cl_attr[1].value = lck_buf.get();
    cl_attr[1].value_sz = lck_buf_sz;
    cl_attr[1].datatype = shard_dtypes[1];

    hyper_call_and_loop(&hyperdex::Client::put, shard_space, shard_id, cl_attr, 2);
    free(cl_attr);
}

void
hyper_stub :: restore_backup(std::unordered_map<uint64_t, uint64_t> &qts_map,
            std::unordered_map<uint64_t, vc::vclock_t> &last_clocks)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;
    hyper_get_and_loop(shard_space, shard_id, &cl_attr, &num_attrs);
    assert(num_attrs == 2);
    for (uint64_t i = 0; i < num_attrs; i++) {
        assert(strcmp(cl_attr[i].attr, shard_attrs[i]) == 0);
    }

    unpack_buffer(cl_attr[0].value, cl_attr[0].value_sz, qts_map);
    unpack_buffer(cl_attr[1].value, cl_attr[1].value_sz, last_clocks);
}

void
hyper_stub :: put_node(element::node &n, std::unordered_set<uint64_t> &nbr_map)
{
    hyperdex_client_attribute *cl_attr = (hyperdex_client_attribute*)malloc(5 * sizeof(hyperdex_client_attribute));
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
    std::unique_ptr<char> out_edges_buf;
    uint64_t out_edges_buf_sz;
    prepare_buffer<element::edge*>(n.out_edges, out_edges_buf, out_edges_buf_sz);
    cl_attr[3].attr = graph_attrs[3];
    cl_attr[3].value = out_edges_buf.get();
    cl_attr[3].value_sz = out_edges_buf_sz;
    cl_attr[3].datatype = graph_dtypes[3];
    // in nbrs
    std::unique_ptr<char> in_nbrs_buf;
    uint64_t in_nbrs_buf_sz;
    prepare_buffer(nbr_map, in_nbrs_buf, in_nbrs_buf_sz);
    cl_attr[4].attr = graph_attrs[4];
    cl_attr[4].value = in_nbrs_buf.get();
    cl_attr[4].value_sz = in_nbrs_buf_sz;
    cl_attr[4].datatype = graph_dtypes[4];

    hyper_call_and_loop(&hyperdex::Client::put, graph_space, n.base.get_id(), cl_attr, 5);
    free(cl_attr);
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

    hyper_call_and_loop(&hyperdex::Client::put, graph_space, n.base.get_id(), &cl_attr, 1);
}

void
hyper_stub :: update_del_time(element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> del_clk_buf;
    prepare_buffer(n.base.get_del_time(), del_clk_buf);
    cl_attr.attr = graph_attrs[1];
    cl_attr.value = (const char*)del_clk_buf->data();
    cl_attr.value_sz = del_clk_buf->size();
    cl_attr.datatype = graph_dtypes[1];

    hyper_call_and_loop(&hyperdex::Client::put, graph_space, n.base.get_id(), &cl_attr, 1);
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

    hyper_call_and_loop(&hyperdex::Client::put, graph_space, n.base.get_id(), &cl_attr, 1);
}

void
hyper_stub :: add_out_edge(element::node &n, element::edge *e)
{
    hyperdex_client_map_attribute map_attr;
    //std::unique_ptr<int64_t> key_buf(new int64_t(e->base.get_id()));
    uint64_t key = e->base.get_id();
    std::unique_ptr<e::buffer> val_buf;
    prepare_buffer(e, val_buf);
    map_attr.attr = graph_attrs[3];
    //map_attr.map_key = (const char*)key_buf.get();
    map_attr.map_key = (const char*)&key;
    map_attr.map_key_sz = sizeof(int64_t);
    map_attr.map_key_datatype = HYPERDATATYPE_INT64;
    map_attr.value = (const char*)val_buf->data();
    map_attr.value_sz = val_buf->size();
    map_attr.value_datatype = HYPERDATATYPE_STRING;

    hypermap_call_and_loop(&hyperdex::Client::map_add, graph_space, n.base.get_id(), &map_attr, 1);
}

void
hyper_stub :: remove_out_edge(element::node &n, element::edge *e)
{
    hyperdex_client_attribute cl_attr;
    //std::unique_ptr<int64_t> key_buf(new int64_t(e->base.get_id()));
    uint64_t key = e->base.get_id();
    cl_attr.attr = graph_attrs[3];
    //cl_attr.value = (const char*)key_buf.get();
    cl_attr.value = (const char*)&key;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = HYPERDATATYPE_INT64;

    hyper_call_and_loop(&hyperdex::Client::map_remove, graph_space, n.base.get_id(), &cl_attr, 1);
}

void
hyper_stub :: add_in_nbr(uint64_t n_hndl, uint64_t nbr)
{
    hyperdex_client_attribute cl_attr;
    //std::unique_ptr<int64_t> nbr_buf(new int64_t(nbr));
    cl_attr.attr = graph_attrs[4];
    //cl_attr.value = (const char*)nbr_buf.get();
    cl_attr.value = (const char*)&nbr;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = HYPERDATATYPE_INT64;

    hyper_call_and_loop(&hyperdex::Client::set_add, graph_space, n_hndl, &cl_attr, 1);
}

void
hyper_stub :: remove_in_nbr(uint64_t n_hndl, uint64_t nbr)
{
    hyperdex_client_attribute cl_attr;
    //std::unique_ptr<int64_t> nbr_buf(new int64_t(nbr));
    cl_attr.attr = graph_attrs[4];
    //cl_attr.value = (const char*)nbr_buf.get();
    cl_attr.value = (const char*)&nbr;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = HYPERDATATYPE_INT64;

    hyper_call_and_loop(&hyperdex::Client::set_remove, graph_space, n_hndl, &cl_attr, 1);
}

void
hyper_stub :: increment_qts(uint64_t vt_id, uint64_t incr)
{
    hyperdex_client_map_attribute map_attr;
    //std::unique_ptr<int64_t> key_buf(new int64_t(vt_id));
    //std::unique_ptr<int64_t> val_buf(new int64_t(incr));
    map_attr.attr = shard_attrs[0];
    //map_attr.map_key = (const char*)key_buf.get();
    map_attr.map_key = (const char*)&vt_id;
    map_attr.map_key_sz = sizeof(int64_t);
    map_attr.map_key_datatype = HYPERDATATYPE_INT64;
    //map_attr.value = (const char*)val_buf.get();
    map_attr.value = (const char*)&incr;
    map_attr.value_sz = sizeof(int64_t);
    map_attr.value_datatype = HYPERDATATYPE_INT64;

    hypermap_call_and_loop(&hyperdex::Client::map_atomic_add, shard_space, shard_id, &map_attr, 1);
}

void
hyper_stub :: update_last_clocks(uint64_t vt_id, vc::vclock_t &vclk)
{
    hyperdex_client_map_attribute map_attr;
    //std::unique_ptr<int64_t> key_buf(new int64_t(vt_id));
    std::unique_ptr<e::buffer> clk_buf;
    prepare_buffer(vclk, clk_buf);
    map_attr.attr = shard_attrs[1];
    //map_attr.map_key = (const char*)key_buf.get();
    map_attr.map_key = (const char*)&vt_id;
    map_attr.map_key_sz = sizeof(int64_t);
    map_attr.map_key_datatype = HYPERDATATYPE_INT64;
    map_attr.value = (const char*)clk_buf->data();
    map_attr.value_sz = clk_buf->size();
    map_attr.value_datatype = HYPERDATATYPE_STRING;

    hypermap_call_and_loop(&hyperdex::Client::map_add, shard_space, shard_id, &map_attr, 1);
}

#undef weaver_debug_
