/*
 * ===============================================================
 *    Description:  Implementation of hyperdex timestamper stub.
 *
 *        Created:  2014-02-27 15:18:59
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#include <e/buffer.h>

#define weaver_debug_
#include "common/config_constants.h"
#include "common/weaver_constants.h"
#include "common/event_order.h"
#include "coordinator/hyper_stub.h"

using coordinator::hyper_stub;
using coordinator::current_tx;

hyper_stub :: hyper_stub(uint64_t vtid)
    : vt_id(vtid)
{ }

std::unordered_map<node_handle_t, uint64_t>
hyper_stub :: get_mappings(std::unordered_set<node_handle_t> &get_set)
{
    return get_nmap(get_set);
}

void
hyper_stub :: do_tx(std::unordered_set<node_handle_t> &get_set,
    std::unordered_set<node_handle_t> &del_set,
    std::unordered_map<node_handle_t, uint64_t> &put_map,
    transaction::pending_tx *tx,
    bool &ready,
    bool &error)
{
#define ERROR_FAIL \
    error = true; \
    abort_tx(); \
    return;

    ready = false;
    error = false;

    begin_tx();

    std::unordered_map<node_handle_t, uint64_t> get_map = get_nmap(get_set);
    if (get_map.size() != get_set.size()) {
        ERROR_FAIL;
    }
    WDEBUG << "get set size " << get_set.size() << std::endl;

    //if (!put_nmap(put_map)) {
    //    WDEBUG << "put nmap fail" << std::endl;
    //    ERROR_FAIL;
    //}
    for (auto &p: put_map) {
        if (!put_nmap(p.first, p.second)) {
            WDEBUG << "put nmap fail" << std::endl;
            ERROR_FAIL;
        }
    }
    WDEBUG << "put map size " << put_map.size() << std::endl;

    //if (!del_nmap(del_set)) {
    //    ERROR_FAIL;
    //}
    WDEBUG << "del set size " << del_set.size() << std::endl;

    std::unordered_map<node_handle_t, std::unique_ptr<db::element::node>> nodes;
    //// get all nodes from Hyperdex (we need at least last upd clk)
    //for (const node_handle_t &h: get_set) {
    //    if (put_map.find(h) != put_map.end()) {
    //        ERROR_FAIL;
    //    }
    //    nodes[h].reset(new db::element::node(h, dummy_clk, &dummy_mtx));
    //    if (!get_node(*nodes[h])) {
    //        ERROR_FAIL;
    //    }
    //}
    //for (const node_handle_t &h: del_set) {
    //    nodes[h].reset(new db::element::node(h, dummy_clk, &dummy_mtx));
    //    if (!get_node(*nodes[h])) {
    //        ERROR_FAIL;
    //    }
    //}

    WDEBUG << "need to check before clocks for " << nodes.size() << " #nodes" << std::endl;
    //// last upd clk check
    //std::vector<vc::vclock> before;
    //before.reserve(nodes.size());
    //for (const auto &p: nodes) {
    //    before.emplace_back(p.second->last_upd_clk);
    //}
    //if (!order::assign_vt_order(before, tx->timestamp)) {
    //    // will retry with higher timestamp
    //    abort_tx();
    //    return;
    //}

    for (const auto &p: put_map) {
        nodes[p.first].reset(new db::element::node(p.first, tx->timestamp, &dummy_mtx));
        nodes[p.first]->last_upd_clk = tx->timestamp;
    }
    WDEBUG << "put map size " << put_map.size() << std::endl;

#define CHECK_LOC(loc, handle) \
    if (loc == UINT64_MAX) { \
        loc_iter = get_map.find(handle); \
        if (loc_iter == get_map.end()) { \
            ERROR_FAIL; \
        } \
        loc = loc_iter->second; \
    }

#define GET_NODE(handle) \
    node_iter = nodes.find(handle); \
    if (node_iter == nodes.end()) { \
        ERROR_FAIL; \
    } else { \
        n = &(*node_iter->second); \
    }

    auto node_iter = nodes.end();
    auto loc_iter = get_map.end();
    db::element::node *n = NULL;

    for (transaction::pending_update *upd: tx->writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                break;

            case transaction::EDGE_CREATE_REQ:
                CHECK_LOC(upd->loc1, upd->handle1);
                CHECK_LOC(upd->loc2, upd->handle2);
                GET_NODE(upd->handle1);
                if (n->out_edges.find(upd->handle) != n->out_edges.end()) {
                    ERROR_FAIL;
                }
                n->add_edge(new db::element::edge(upd->handle, tx->timestamp, upd->loc2, upd->handle2));
                break;

            case transaction::NODE_DELETE_REQ:
                CHECK_LOC(upd->loc1, upd->handle1);
                break;

            case transaction::NODE_SET_PROPERTY:
                CHECK_LOC(upd->loc1, upd->handle1);
                GET_NODE(upd->handle1);
                n->base.properties[*upd->key] = db::element::property(*upd->key, *upd->value, tx->timestamp);
                break;

            case transaction::EDGE_DELETE_REQ:
                CHECK_LOC(upd->loc1, upd->handle2);
                GET_NODE(upd->handle2);
                n->out_edges.erase(upd->handle1);
                break;

            case transaction::EDGE_SET_PROPERTY:
                CHECK_LOC(upd->loc1, upd->handle2);
                GET_NODE(upd->handle2);
                n->out_edges[upd->handle1]->base.properties[*upd->key] = db::element::property(*upd->key, *upd->value, tx->timestamp);
                break;

            default:
                WDEBUG << "bad upd type" << std::endl;
        }
        uint64_t idx = upd->loc1-ShardIdIncr;
        if (tx->shard_write.size() < idx+1) {
            tx->shard_write.resize(idx+1, false);
        }
        tx->shard_write[idx] = true;
    }

#undef CHECK_LOC
#undef GET_NODE

    for (auto &p: nodes) {
        put_node(*p.second);
    }

    //for (const node_handle_t &h: del_set) {
    //    del_node(h);
    //}

    //hyperdex_client_attribute attr[NUM_TX_ATTRS];
    //attr[0].attr = tx_attrs[0];
    //attr[0].value = (const char*)&vt_id;
    //attr[0].value_sz = sizeof(int64_t);
    //attr[0].datatype = tx_dtypes[0];

    //uint64_t buf_sz = message::size(tx);
    //std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
    //e::buffer::packer packer = buf->pack_at(0);
    //message::pack_buffer(packer, tx);

    //attr[1].attr = tx_attrs[1];
    //attr[1].value = (const char*)buf->data();
    //attr[1].value_sz = buf->size();
    //attr[1].datatype = tx_dtypes[1];

    //call(&hyperdex_client_xact_put, tx_space, (const char*)&tx->id, sizeof(int64_t), attr, NUM_TX_ATTRS);

    hyperdex_client_returncode commit_status = HYPERDEX_CLIENT_GARBAGE;
    commit_tx(commit_status);

    switch(commit_status) {
        case HYPERDEX_CLIENT_SUCCESS:
            ready = true;
            assert(!error);
            break;

        case HYPERDEX_CLIENT_ABORTED:
            ready = false;
            assert(!error);
            break;

        default:
            error = true;
            ready = false;
    }

#undef ERROR_FAIL
}


void
hyper_stub :: clean_tx(uint64_t tx_id)
{
    del(tx_space, (const char*)&tx_id, sizeof(int64_t));
}

/*
void
hyper_stub :: prepare_tx(transaction::pending_tx &tx)
{
    int num_calls = 2;
    hyperdex_client_attribute *cl_attr;
    std::vector<const char*> spaces;
    std::vector<hyperdex_client_attribute*> attrs;
    std::vector<hyper_func> funcs;
    std::vector<const char*> keys;
    std::vector<size_t> key_szs(num_calls, sizeof(int64_t));
    std::vector<size_t> num_attrs;

    spaces.reserve(num_calls);
    attrs.reserve(num_calls);
    funcs.reserve(num_calls);
    keys.reserve(num_calls);
    num_attrs.reserve(num_calls);

    hyperdex_client_attribute attr_array[1 + NUM_MAP_ATTRS];

    cl_attr = attr_array;
    cl_attr->attr = set_attr;
    cl_attr->value = (const char*)&tx.id;
    cl_attr->value_sz = sizeof(int64_t);
    cl_attr->datatype = HYPERDATATYPE_INT64;
    spaces.emplace_back(vt_set_space);
    attrs.emplace_back(cl_attr);
    funcs.emplace_back(&hyperdex::Client::set_add);
    keys.emplace_back((const char*)&vt_id);
    num_attrs.emplace_back(1);

    uint64_t buf_sz = message::size(tx);
    std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack_at(0);
    message::pack_buffer(packer, tx);

    cl_attr = attr_array + 1;
    cl_attr[0].attr = tx_data_attr;
    cl_attr[0].value = (const char*)buf->data();
    cl_attr[0].value_sz = buf->size();
    cl_attr[0].datatype = map_dtypes[0];
    int64_t status = 0;
    cl_attr[1].attr = tx_status_attr;
    cl_attr[1].value = (const char*)&status;
    cl_attr[1].value_sz = sizeof(int64_t);
    cl_attr[1].datatype = map_dtypes[1];
    spaces.emplace_back(vt_map_space);
    attrs.emplace_back(cl_attr);
    funcs.emplace_back(&hyperdex::Client::put);
    keys.emplace_back((const char*)&tx.id);
    num_attrs.emplace_back(NUM_MAP_ATTRS);

    multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);
}

void
hyper_stub :: commit_tx(transaction::pending_tx &tx)
{
    hyperdex_client_attribute cl_attr;
    int64_t status = 1;
    cl_attr.attr = tx_status_attr;
    cl_attr.value = (const char*)&status;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = map_dtypes[1];

    call(&hyperdex::Client::put, vt_map_space, (const char*)&tx.id, sizeof(int64_t), &cl_attr, 1);
}

void
hyper_stub :: del_tx(uint64_t tx_id)
{
    // set remove and del
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = set_attr;
    cl_attr.value = (const char*)&tx_id;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = HYPERDATATYPE_INT64;

    call(&hyperdex::Client::set_remove, vt_set_space, (const char*)&vt_id, sizeof(int64_t), &cl_attr, 1);

    del(vt_map_space, (const char*)&tx_id, sizeof(int64_t));
}

// status = false if not prepared
void
hyper_stub :: recreate_tx(const hyperdex_client_attribute *cl_attr,
    size_t num_attrs,
    current_tx &cur_tx,
    bool &status)
{
    assert(num_attrs == NUM_MAP_ATTRS);
    int idx[2];
    idx[0] = -1;
    idx[1] = -1;
    if (strcmp(cl_attr[0].attr, tx_data_attr) == 0) {
        assert(strcmp(cl_attr[1].attr, tx_status_attr) == 0);
        idx[0] = 0;
        idx[1] = 1;
    } else if (strcmp(cl_attr[1].attr, tx_data_attr) == 0) {
        assert(strcmp(cl_attr[0].attr, tx_status_attr) == 0);
        idx[0] = 1;
        idx[1] = 0;
    } else {
        WDEBUG << "unexpected attrs" << std::endl;
        assert(false);
    }
    assert(cl_attr[idx[0]].datatype == map_dtypes[0]);
    assert(cl_attr[idx[1]].datatype == map_dtypes[1]);

    cur_tx.tx.reset(new transaction::pending_tx());
    std::unique_ptr<e::buffer> buf(e::buffer::create(cl_attr[idx[0]].value,
                                                     cl_attr[idx[0]].value_sz));
    e::unpacker unpacker = buf->unpack_from(0);
    message::unpack_buffer(unpacker, *cur_tx.tx);

    assert(cl_attr[idx[1]].value_sz == sizeof(int64_t));
    int64_t *status_ptr = (int64_t*)cl_attr[idx[1]].value;
    assert(*status_ptr == 0 || *status_ptr == 1);
    status = (*status_ptr == 1);
}

void
hyper_stub :: restore_backup(std::unordered_map<uint64_t, current_tx> &prepare_tx,
    std::unordered_map<uint64_t, current_tx> &outstanding_tx)
{
    const hyperdex_client_attribute *cl_set_attr;
    size_t num_set_attrs;
    get(vt_set_space, (const char*)&vt_id, sizeof(int64_t), &cl_set_attr, &num_set_attrs);
    assert(num_set_attrs == 1);
    assert(strcmp(cl_set_attr->attr, set_attr) == 0);

    std::unordered_set<uint64_t> tx_ids;
    unpack_buffer(cl_set_attr->value, cl_set_attr->value_sz, tx_ids);
    hyperdex_client_destroy_attrs(cl_set_attr, 1);

    assert(tx_ids.find(INT64_MAX) != tx_ids.end());
    tx_ids.erase(INT64_MAX);

    std::vector<const char*> spaces(tx_ids.size(), vt_map_space);
    std::vector<const char*> keys(tx_ids.size());
    std::vector<size_t> key_szs(tx_ids.size(), sizeof(int64_t));
    std::vector<const hyperdex_client_attribute**> cl_attrs_vec;
    std::vector<size_t*> num_attrs_vec;
    cl_attrs_vec.reserve(tx_ids.size());
    num_attrs_vec.reserve(tx_ids.size());
    const hyperdex_client_attribute *cl_attr[tx_ids.size()];
    size_t num_attr[tx_ids.size()];
    auto iter = tx_ids.begin();
    for (uint64_t i = 0; i < tx_ids.size(); i++, iter++) {
        keys[i] = (const char*)&(*iter);
        cl_attrs_vec.emplace_back(cl_attr + i);
        num_attrs_vec.emplace_back(num_attr + i);
    }

    multiple_get(spaces, keys, key_szs, cl_attrs_vec, num_attrs_vec);

    auto tx_iter = tx_ids.begin();
    uint64_t tx_id;
    for (uint64_t i = 0; i < tx_ids.size(); i++, tx_iter++) {
        tx_id = *tx_iter;
        current_tx cur_tx;
        bool status;
        recreate_tx(cl_attr[i], num_attr[i], cur_tx, status);
        assert(cur_tx.tx->id == tx_id);

        if (status) {
            outstanding_tx.emplace(tx_id, std::move(cur_tx));
        } else {
            prepare_tx.emplace(tx_id, std::move(cur_tx));
        }
    }

    for (uint64_t i = 0; i < tx_ids.size(); i++) {
        hyperdex_client_destroy_attrs(cl_attr[i], num_attr[i]);
    }
}
*/
