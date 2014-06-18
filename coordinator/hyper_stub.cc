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
#include "coordinator/hyper_stub.h"

using coordinator::hyper_stub;

hyper_stub :: hyper_stub(uint64_t vtid)
    : vt_id(vtid)
    , map_dtypes{HYPERDATATYPE_STRING, HYPERDATATYPE_INT64}
{
    std::unordered_set<uint64_t> singleton;
    singleton.insert(INT64_MAX);
    std::unique_ptr<char> set_buf;
    uint64_t buf_sz;
    prepare_buffer(singleton, set_buf, buf_sz);

    hyperdex_client_attribute cl_attr;
    cl_attr.attr = set_attr;
    cl_attr.value = set_buf.get();
    cl_attr.value_sz = buf_sz;
    cl_attr.datatype = set_dtype;

    hyper_call_and_loop(&hyperdex::Client::put, vt_set_space, vt_id, &cl_attr, 1);
}

void
hyper_stub :: prepare_tx(transaction::pending_tx &tx)
{
    int num_calls = 2;
    hyperdex_client_attribute *cl_attr;
    std::vector<const char*> spaces;
    std::vector<hyperdex_client_attribute*> attrs;
    std::vector<hyper_func> funcs;
    std::vector<uint64_t> keys;
    std::vector<size_t> num_attrs;
    spaces.reserve(num_calls);
    attrs.reserve(num_calls);
    funcs.reserve(num_calls);
    keys.reserve(num_calls);
    num_attrs.reserve(num_calls);

    cl_attr = (hyperdex_client_attribute*)malloc(sizeof(hyperdex_client_attribute));
    cl_attr->attr = set_attr;
    cl_attr->value = (const char*)&tx.id;
    cl_attr->value_sz = sizeof(int64_t);
    cl_attr->datatype = HYPERDATATYPE_INT64;
    spaces.emplace_back(vt_set_space);
    attrs.emplace_back(cl_attr);
    funcs.emplace_back(&hyperdex::Client::set_add);
    keys.emplace_back(vt_id);
    num_attrs.emplace_back(1);

    uint64_t buf_sz = message::size(tx);
    std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack_at(0);
    message::pack_buffer(packer, tx);

    cl_attr = (hyperdex_client_attribute*)malloc(NUM_MAP_ATTRS * sizeof(hyperdex_client_attribute));
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
    keys.emplace_back(tx.id);
    num_attrs.emplace_back(NUM_MAP_ATTRS);

    hyper_multiple_call_and_loop(funcs, spaces, keys, attrs, num_attrs);
    
    for (hyperdex_client_attribute *del_attr: attrs) {
        free(del_attr);
    }
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

    hyper_call_and_loop(&hyperdex::Client::put, vt_map_space, tx.id, &cl_attr, 1);
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

    hyper_call_and_loop(&hyperdex::Client::set_remove, vt_set_space, vt_id, &cl_attr, 1);

    hyper_del_and_loop(vt_map_space, tx_id);
}

void
hyper_stub :: restore_backup(std::unordered_map<uint64_t, transaction::pending_tx> &txs)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;
    hyper_get_and_loop(vt_set_space, vt_id, &cl_attr, &num_attrs);
    assert(num_attrs == 1);
    assert(strcmp(cl_attr->attr, set_attr) == 0);

    std::unordered_set<uint64_t> tx_ids;
    unpack_buffer(cl_attr->value, cl_attr->value_sz, tx_ids);
    hyperdex_client_destroy_attrs(cl_attr, 1);

    std::vector<const char*> spaces(tx_ids.size(), vt_map_space);
    std::vector<uint64_t> keys(tx_ids.begin(), tx_ids.end());
    std::vector<const hyperdex_client_attribute**> cl_attrs;
    std::vector<size_t*> num_attrss;
    cl_attrs.reserve(tx_ids.size());
    num_attrss.reserve(tx_ids.size());
    const hyperdex_client_attribute *cl_attr_array[tx_ids.size()];
    size_t num_attr_array[tx_ids.size()];
    for (uint64_t i = 0; i < tx_ids.size(); i++) {
        cl_attrs.emplace_back(cl_attr_array + i);
        num_attrss.emplace_back(num_attr_array + i);
    }

    hyper_multiple_get_and_loop(spaces, keys, cl_attrs, num_attrss);

    txs.reserve(tx_ids.size());
    message::message buf_wrapper(message::CLIENT_TX_INIT);
    auto tx_iter = tx_ids.begin();
    for (uint64_t i = 0; i < tx_ids.size(); i++, tx_iter++) {
        transaction::pending_tx tx;
        buf_wrapper.buf.reset(e::buffer::create(cl_attr_array[i]->value, cl_attr_array[i]->value_sz));
        buf_wrapper.unpack_client_tx(txs[*tx_iter]);
    }

    for (uint64_t i = 0; i < tx_ids.size(); i++) {
        hyperdex_client_destroy_attrs(cl_attr_array[i], 1);
    }
}
