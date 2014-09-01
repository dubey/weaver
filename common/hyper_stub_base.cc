/*
 * ===============================================================
 *    Description:  Implementation of hyper_stub_base.
 *
 *        Created:  2014-02-26 15:28:58
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/hyper_stub_base.h"
#include "common/config_constants.h"

hyper_stub_base :: hyper_stub_base()
    : graph_attrs{"creat_time",
        "del_time",
        "properties",
        "out_edges",
        "migr_status", // 0 for stable, 1 for moving
        "last_upd_clk"}
    , graph_dtypes{HYPERDATATYPE_STRING,
        HYPERDATATYPE_STRING,
        HYPERDATATYPE_MAP_STRING_STRING,
        HYPERDATATYPE_MAP_STRING_STRING,
        HYPERDATATYPE_INT64,
        HYPERDATATYPE_STRING}
    , tx_attrs{"vt_id",
        "tx_data"}
    , tx_dtypes{HYPERDATATYPE_INT64,
        HYPERDATATYPE_STRING}
    , cl(hyperdex_client_create(HyperdexCoordIpaddr, HyperdexCoordPort))
{ }

#define HYPERDEX_CHECK_ID(status) \
    if (hdex_id < 0) { \
        WDEBUG << "Hyperdex function failed, op id = " << hdex_id \
               << ", status = " << hyperdex_client_returncode_to_string(status) << std::endl; \
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl; \
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl; \
        assert(false); \
        success = false; \
    } else { \
        success_calls++; \
    }

#define HYPERDEX_CALL(h, space, key, key_sz, attr, attr_sz, call_status) \
    hdex_id = h(hyper_tx, space, key, key_sz, attr, attr_sz, &call_status); \
    HYPERDEX_CHECK_ID(call_status);

#define HYPERDEX_GET(space, key, key_sz, get_status, attr, attr_sz) \
    hdex_id = hyperdex_client_xact_get(hyper_tx, space, key, key_sz, &get_status, attr, attr_sz); \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_DEL(space, key, key_sz, del_status) \
    hdex_id = hyperdex_client_xact_del(hyper_tx, space, key, key_sz, &del_status); \
    HYPERDEX_CHECK_ID(del_status);

#define HYPERDEX_LOOP \
    hdex_id = hyperdex_client_loop(cl, -1, &loop_status); \
    HYPERDEX_CHECK_ID(loop_status);

#define HYPERDEX_CHECK_STATUSES(status, fail_check) \
    if ((loop_status != HYPERDEX_CLIENT_SUCCESS) || (fail_check)) { \
        WDEBUG << "hyperdex error" \
               << ", call status: " << hyperdex_client_returncode_to_string(status) \
               << ", loop status: " << hyperdex_client_returncode_to_string(loop_status) << std::endl; \
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl; \
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl; \
        success = false; \
    }


void
hyper_stub_base :: begin_tx()
{
    hyper_tx = hyperdex_client_begin_transaction(cl);
}

void
hyper_stub_base :: commit_tx(hyperdex_client_returncode &status)
{
    bool success = true;
    int success_calls = 0;
    hyperdex_client_returncode loop_status;

    int64_t hdex_id = hyperdex_client_commit_transaction(hyper_tx, &status);
    HYPERDEX_CHECK_ID(status);

    if (success) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(status,
            status != HYPERDEX_CLIENT_ABORTED && status != HYPERDEX_CLIENT_SUCCESS);
    }
}

void
hyper_stub_base :: abort_tx()
{
    hyperdex_client_abort_transaction(hyper_tx);
}

// call hyperdex function h using params and then loop for response
bool
hyper_stub_base :: call(hyper_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_attribute *cl_attr, size_t num_attrs)
{
    hyperdex_client_returncode call_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    HYPERDEX_CALL(h, space, key, key_sz, cl_attr, num_attrs, call_status);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(call_status, call_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

// call hyperdex map function h using key hndl, attributes cl_attr, and then loop for response
bool
hyper_stub_base :: map_call(hyper_map_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_map_attribute *map_attr, size_t num_attrs)
{
    hyperdex_client_returncode call_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;
    
    HYPERDEX_CALL(h, space, key, key_sz, map_attr, num_attrs, call_status);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(call_status, call_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

// multiple hyperdex calls
bool
hyper_stub_base :: multiple_call(std::vector<hyper_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs)
{
    std::vector<const char*> map_spaces;
    std::vector<hyper_map_func> map_funcs;
    std::vector<const char*> map_keys;
    std::vector<size_t> map_key_szs;
    std::vector<hyperdex_client_map_attribute*> map_attrs;
    std::vector<size_t> map_num_attrs;
    return multiple_call(funcs,
        spaces,
        keys, key_szs,
        attrs, num_attrs,
        map_funcs,
        map_spaces,
        map_keys, map_key_szs,
        map_attrs, map_num_attrs);
}

bool
hyper_stub_base :: multiple_call(std::vector<hyper_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs,
    std::vector<hyper_map_func> &map_funcs,
    std::vector<const char*> &map_spaces,
    std::vector<const char*> &map_keys, std::vector<size_t> &map_key_szs,
    std::vector<hyperdex_client_map_attribute*> &map_attrs, std::vector<size_t> &map_num_attrs)
{
    uint64_t num_calls = funcs.size();
    assert(num_calls == spaces.size());
    assert(num_calls == keys.size());
    assert(num_calls == key_szs.size());
    assert(num_calls == num_attrs.size());
    assert(num_calls == attrs.size());

    uint64_t map_num_calls = map_funcs.size();
    assert(map_num_calls == map_spaces.size());
    assert(map_num_calls == map_keys.size());
    assert(map_num_calls == map_key_szs.size());
    assert(map_num_calls == map_num_attrs.size());
    assert(map_num_calls == map_attrs.size());

    hyperdex_client_returncode call_status[num_calls+map_num_calls];
    int hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;

    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_CALL(funcs[i], spaces[i], keys[i], key_szs[i], attrs[i], num_attrs[i], call_status[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    for (uint64_t j = 0; j < map_num_calls; j++, i++) {
        HYPERDEX_CALL(map_funcs[j], map_spaces[j], map_keys[j], map_key_szs[j], map_attrs[j], map_num_attrs[j], call_status[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        assert(opid_to_idx.find(hdex_id) != opid_to_idx.end());
        int64_t &idx = opid_to_idx[hdex_id];
        assert(idx >= 0);

        HYPERDEX_CHECK_STATUSES(call_status[idx], call_status[idx] != HYPERDEX_CLIENT_SUCCESS);

        idx = -1;
    }

    return success;
}

bool
hyper_stub_base :: get(const char *space,
    const char *key, size_t key_sz,
    const hyperdex_client_attribute **cl_attr, size_t *num_attrs)
{
    hyperdex_client_returncode get_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    HYPERDEX_GET(space, key, key_sz, get_status, cl_attr, num_attrs);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(get_status, get_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

bool
hyper_stub_base :: multiple_get(std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<const hyperdex_client_attribute**> &cl_attrs, std::vector<size_t*> &num_attrs)
{
    uint64_t num_calls = spaces.size();
    assert(num_calls == keys.size());
    assert(num_calls == key_szs.size());
    assert(num_calls == num_attrs.size());
    assert(num_calls == cl_attrs.size());

    hyperdex_client_returncode get_status[num_calls];
    int64_t hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;
    
    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_GET(spaces[i], keys[i], key_szs[i], get_status[i], cl_attrs[i], num_attrs[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        assert(opid_to_idx.find(hdex_id) != opid_to_idx.end());
        int64_t &idx = opid_to_idx[hdex_id];
        assert(idx >= 0);

        HYPERDEX_CHECK_STATUSES(get_status[idx], get_status[idx] != HYPERDEX_CLIENT_SUCCESS);

        idx = -1;
    }

    return success;
}

// delete the given key
bool
hyper_stub_base :: del(const char* space, const char *key, size_t key_sz)
{
    hyperdex_client_returncode del_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    HYPERDEX_DEL(space, key, key_sz, del_status);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(del_status, del_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

// delete multiple keys
bool
hyper_stub_base :: multiple_del(std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs)
{
    uint64_t num_calls = spaces.size();
    assert(keys.size() == num_calls);
    assert(key_szs.size() == num_calls);

    hyperdex_client_returncode del_status[num_calls];
    int64_t hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;
    
    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_DEL(spaces[i], keys[i], key_szs[i], del_status[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        assert(opid_to_idx.find(hdex_id) != opid_to_idx.end());
        int64_t &idx = opid_to_idx[hdex_id];
        assert(idx >= 0);

        HYPERDEX_CHECK_STATUSES(del_status[idx], del_status[idx] != HYPERDEX_CLIENT_SUCCESS && del_status[idx] != HYPERDEX_CLIENT_NOTFOUND);

        idx = -1;
    }

    return success;
}

#undef HYPERDEX_CHECK_ID
#undef HYPERDEX_CALL
#undef HYPERDEX_GET
#undef HYPERDEX_DEL
#undef HYPERDEX_LOOP
#undef HYPERDEX_CHECK_STATUSES

bool
hyper_stub_base :: recreate_node(const hyperdex_client_attribute *cl_attr, db::element::node &n)
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
    std::unordered_map<std::string, db::element::property> props;
    unpack_buffer<db::element::property>(cl_attr[idx[2]].value, cl_attr[idx[2]].value_sz, props);

    n.state = db::element::node::mode::STABLE;
    n.in_use = false;
    n.base.update_creat_time(create_clk);
    n.base.update_del_time(delete_clk);
    n.base.set_properties(props);

    // out edges
    unpack_buffer<db::element::edge*>(cl_attr[idx[3]].value, cl_attr[idx[3]].value_sz, n.out_edges);
    // last update clock
    unpack_buffer(cl_attr[idx[5]].value, cl_attr[idx[5]].value_sz, n.last_upd_clk);

    return true;
}
bool
hyper_stub_base :: get_node(db::element::node &n)
{
    const hyperdex_client_attribute *attr;
    size_t num_attrs;
    node_handle_t handle = n.get_handle();
    bool success = get(graph_space, handle.c_str(), handle.size(), &attr, &num_attrs);
    if (success) {
        success = recreate_node(attr, n);
    }

    return success;
}

bool
hyper_stub_base :: put_node(db::element::node &n)
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

    /*
    // properties
    std::unique_ptr<e::buffer> props_buf;
    prepare_buffer<db::element::property>(n.base.properties, props_buf);
    for (const auto &p: n.base.properties) {
        WDEBUG << p.second.key << " " << p.second.value << std::endl;
        WDEBUG << p.second.creat_time.vt_id << " " << p.second.creat_time.clock.size() << std::endl;
        WDEBUG << p.second.del_time.vt_id << " " << p.second.del_time.clock.size() << std::endl;
    }
    cl_attr[2].attr = graph_attrs[2];
    cl_attr[2].value = (const char*)props_buf->data();
    cl_attr[2].value_sz = props_buf->size();
    cl_attr[2].datatype = graph_dtypes[2];

    // out edges
    std::unique_ptr<e::buffer> out_edges_buf;
    prepare_buffer<db::element::edge*>(n.out_edges, out_edges_buf);
    cl_attr[3].attr = graph_attrs[3];
    cl_attr[3].value = (const char*)out_edges_buf->data();
    cl_attr[3].value_sz = out_edges_buf->size();
    cl_attr[3].datatype = graph_dtypes[3];

    // migr status
    int64_t status = STABLE;
    cl_attr[4].attr = graph_attrs[4];
    cl_attr[4].value = (const char*)&status;
    cl_attr[4].value_sz = sizeof(int64_t);
    cl_attr[4].datatype = graph_dtypes[4];

    // last update clock
    std::unique_ptr<e::buffer> last_clk_buf;
    prepare_buffer(n.last_upd_clk, last_clk_buf);
    cl_attr[5].attr = graph_attrs[5];
    cl_attr[5].value = (const char*)last_clk_buf->data();
    cl_attr[5].value_sz = last_clk_buf->size();
    cl_attr[5].datatype = graph_dtypes[5];
    */

    node_handle_t handle = n.get_handle();
    return call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), cl_attr, /*NUM_GRAPH_ATTRS*/2);
}

void
hyper_stub_base :: del_node(const node_handle_t &handle)
{
    del(graph_space, handle.c_str(), handle.size());
}

/*
void
hyper_stub_base :: update_creat_time(db::element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> creat_clk_buf;
    prepare_buffer(n.base.get_creat_time(), creat_clk_buf);
    cl_attr.attr = graph_attrs[0];
    cl_attr.value = (const char*)creat_clk_buf->data();
    cl_attr.value_sz = creat_clk_buf->size();
    cl_attr.datatype = graph_dtypes[0];

    node_handle_t handle = n.get_handle();
    call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub_base :: update_properties(db::element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> props_buf;
    prepare_buffer(*n.base.get_props(), props_buf);
    cl_attr.attr = graph_attrs[2];
    cl_attr.value = (const char*)props_buf->data();
    cl_attr.value_sz = props_buf->size();
    cl_attr.datatype = graph_dtypes[2];

    node_handle_t handle = n.get_handle();
    call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub_base :: add_out_edge(db::element::node &n, db::element::edge *e)
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
    map_call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), &map_attr, 1);
}

void
hyper_stub_base :: remove_out_edge(db::element::node &n, db::element::edge *e)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> key_buf;
    prepare_buffer(e->get_handle(), key_buf);
    cl_attr.attr = graph_attrs[3];
    cl_attr.value = (const char*)key_buf->data();
    cl_attr.value_sz = key_buf->size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    node_handle_t handle = n.get_handle();
    call(&hyperdex_client_xact_map_remove, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub_base :: add_in_nbr(const node_handle_t &n_hndl, const node_handle_t &nbr)
{
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[4];
    cl_attr.value = nbr.c_str();
    cl_attr.value_sz = nbr.size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    call(&hyperdex_client_xact_set_add, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}

void
hyper_stub_base :: remove_in_nbr(const node_handle_t &n_hndl, const node_handle_t &nbr)
{
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[4];
    cl_attr.value = nbr.c_str();
    cl_attr.value_sz = nbr.size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    call(&hyperdex_client_xact_set_remove, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}

void
hyper_stub_base :: update_tx_queue(db::element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> txq_buf;
    prepare_buffer(n.tx_queue, txq_buf);
    cl_attr.attr = graph_attrs[5];
    cl_attr.value = (const char*)txq_buf->data();
    cl_attr.value_sz = txq_buf->size();
    cl_attr.datatype = graph_dtypes[5];

    node_handle_t handle = n.get_handle();
    call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub_base :: update_migr_status(const node_handle_t &n_hndl, enum persist_node_state status)
{
    int64_t int_status = status;
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[6];
    cl_attr.value = (const char*)&int_status;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = graph_dtypes[6];

    call(&hyperdex_client_xact_put, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}


// store the given unordered_map<int, int> as a HYPERDATATYPE_MAP_INT64_INT64
void
hyper_stub_base :: prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map, std::unique_ptr<e::buffer> &buf)
{
    std::vector<uint64_t> sorted;
    sorted.reserve(map.size());
    for (auto &p: map) {
        sorted.emplace_back(p.first);
    }
    std::sort(sorted.begin(), sorted.end());

    uint64_t buf_sz = map.size() * (sizeof(int64_t) + sizeof(int64_t));
    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    for (uint64_t key: sorted) {
        pack_uint64(packer, key);
        pack_uint64(packer, map.at(key));
    }
}

// unpack the HYPERDATATYPE_MAP_INT64_INT64 in to an unordered_map<int, int>
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, uint64_t> &map)
{
    uint64_t num_entries = buf_sz / (sizeof(int64_t) + sizeof(int64_t));
    map.reserve(num_entries);
    uint64_t key, val;

    for (uint64_t i = 0; i < num_entries; i++) {
        buf = e::unpack64le(buf, &key);
        buf = e::unpack64le(buf, &val);
        map.emplace(key, val);
    }
}

// store the given unordered_set as a HYPERDATATYPE_SET_INT64
void
hyper_stub_base :: prepare_buffer(const std::unordered_set<uint64_t> &set, std::unique_ptr<e::buffer> &buf)
{
    std::vector<uint64_t> sorted;
    sorted.reserve(set.size());
    for (uint64_t x: set) {
        sorted.emplace_back(x);
    }
    std::sort(sorted.begin(), sorted.end());

    uint64_t buf_sz = sizeof(uint64_t) * set.size();
    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    // now iterate in sorted order
    for (uint64_t x: sorted) {
        pack_uint64(packer, x);
    }
}

// unpack the HYPERDATATYPE_SET_INT64 in to an unordered_set
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<uint64_t> &set)
{
    uint64_t set_sz = buf_sz / sizeof(uint64_t);
    uint64_t next;
    set.reserve(set_sz);

    for (uint64_t i = 0; i < set_sz; i++) {
        buf = e::unpack64le(buf, &next);
        set.emplace(next);
    }
}

// store the unordered_set<string> as a HYPERDATATYPE_SET_STRING
void
hyper_stub_base :: prepare_buffer(const std::unordered_set<std::string> &set, std::unique_ptr<e::buffer> &buf)
{
    std::vector<std::string> sorted;
    sorted.reserve(set.size());
    uint64_t buf_sz = 0;

    for (const std::string &s: set) {
        sorted.emplace_back(s);
        buf_sz += sizeof(uint32_t) + s.size();
    }
    std::sort(sorted.begin(), sorted.end());

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    for (const std::string &s: sorted) {
        message::pack_buffer(packer, s);
    }
}

// unpack the HYPERDATATYPE_SET_STRING in to an unordered_set<string>
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<std::string> &set)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    std::string next;

    while (!unpacker.empty()) {
        next.erase();
        message::unpack_buffer(unpacker, next);
        set.emplace(next);
    }
}
*/

bool
hyper_stub_base :: put_nmap(const node_handle_t &handle, uint64_t loc)
{
    hyperdex_client_attribute attr;
    attr.attr = nmap_attr;
    attr.value = (const char*)&loc;
    attr.value_sz = sizeof(int64_t);
    attr.datatype = HYPERDATATYPE_INT64;

    return call(hyperdex_client_xact_put, nmap_space, handle.c_str(), handle.size(), &attr, 1);
}

bool
hyper_stub_base :: put_nmap(std::unordered_map<node_handle_t, uint64_t> &pairs_to_add)
{
    int num_pairs = pairs_to_add.size();
    std::vector<hyper_func> funcs(num_pairs, &hyperdex_client_xact_put);
    std::vector<const char*> spaces(num_pairs, nmap_space);
    std::vector<const char*> keys(num_pairs);
    std::vector<size_t> key_szs(num_pairs);
    std::vector<hyperdex_client_attribute*> attrs(num_pairs);
    std::vector<size_t> num_attrs(num_pairs, 1);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_pairs * sizeof(hyperdex_client_attribute));

    int i = 0;
    for (auto &entry: pairs_to_add) {
        attrs[i] = attrs_to_add + i;
        attrs[i]->attr = nmap_attr;
        attrs[i]->value = (char*)&entry.second;
        attrs[i]->value_sz = sizeof(int64_t);
        attrs[i]->datatype = HYPERDATATYPE_INT64;
        keys[i] = entry.first.c_str();
        key_szs[i] = entry.first.size();
        i++;
    }

    bool success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);

    free(attrs_to_add);

    return success;
}


std::unordered_map<node_handle_t, uint64_t>
hyper_stub_base :: get_nmap(std::unordered_set<node_handle_t> &toGet)
{
    int64_t num_nodes = toGet.size();
    std::vector<const char*> spaces(num_nodes, nmap_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<const hyperdex_client_attribute**> attrs(num_nodes, NULL);
    std::vector<size_t*> num_attrs(num_nodes, NULL);

    const hyperdex_client_attribute *attr_array[num_nodes];
    size_t num_attrs_array[num_nodes];

    int i = 0;
    for (auto &n: toGet) {
        keys[i] = n.c_str();
        key_szs[i] = n.size();
        attrs[i] = attr_array + i;
        num_attrs[i] = num_attrs_array + i;

        i++;
    }

    bool success = multiple_get(spaces, keys, key_szs, attrs, num_attrs);
    UNUSED(success);

    uint64_t *val;
    std::unordered_map<node_handle_t, uint64_t> mappings;
    mappings.reserve(num_nodes);
    for (int64_t i = 0; i < num_nodes; i++) {
        if (*num_attrs[i] == 1) {
            val = (uint64_t*)attr_array[i]->value;
            mappings[node_handle_t(keys[i])] = *val;
            hyperdex_client_destroy_attrs(attr_array[i], 1);
        } else if (*num_attrs[i] > 0) {
            WDEBUG << "bad num attributes " << *num_attrs[i] << std::endl;
            hyperdex_client_destroy_attrs(attr_array[i], *num_attrs[i]);
        }
    }

    return mappings;
}


bool
hyper_stub_base :: del_nmap(std::unordered_set<node_handle_t> &toDel)
{
    int64_t num_nodes = toDel.size();
    std::vector<const char*> spaces(num_nodes, nmap_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);

    int i = 0;
    for (auto &n: toDel) {
        keys[i] = n.c_str();
        key_szs[i] = n.size();

        i++;
    }

    return multiple_del(spaces, keys, key_szs);
}
