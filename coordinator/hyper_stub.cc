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

void
hyper_stub :: restore_backup(std::unordered_map<uint64_t, transaction::pending_tx> &txs)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;
    hyper_get_and_loop(vt_set_space, vt_id, &cl_attr, &num_attrs);
    assert(num_attrs == 1);
    assert(strcmp(cl_attr->attr, attr) == 0);

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
        message::unpack_client_tx(buf_wrapper, txs[*tx_iter]);
    }

    for (uint64_t i = 0; i < tx_ids.size(); i++) {
        hyperdex_client_destroy_attrs(cl_attr_array[i], 1);
    }
}

void
hyper_stub :: put_tx(uint64_t tx_id, message::message &tx_msg)
{
    std::vector<const char*> spaces;
    std::vector<hyperdex_client_attribute*> attrs(2, NULL);
    std::vector<hyper_func> funcs;
    std::vector<uint64_t> keys;
    std::vector<size_t> num_attrs(2, 1);

    attrs.emplace_back(new hyperdex_client_attribute());
    attrs.emplace_back(new hyperdex_client_attribute());

    attrs[0]->attr = attr;
    attrs[0]->value = (const char*)&tx_id;
    attrs[0]->value_sz = sizeof(int64_t);
    attrs[0]->datatype = set_dtype;
    spaces.emplace_back(vt_set_space);
    funcs.emplace_back(&hyperdex::Client::set_add);
    keys.emplace_back(vt_id);
    attrs[1]->attr = attr;
    attrs[1]->value = (const char*)tx_msg.buf->data();
    attrs[1]->value_sz = tx_msg.buf->size();
    attrs[1]->datatype = map_dtype;
    spaces.emplace_back(vt_map_space);
    funcs.emplace_back(&hyperdex::Client::put);
    keys.emplace_back(tx_id);

    hyper_multiple_call_and_loop(funcs, spaces, keys, attrs, num_attrs);
}

void
hyper_stub :: del_tx(uint64_t tx_id)
{
    // set remove and del
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = attr;
    cl_attr.value = (const char*)&tx_id;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = set_dtype;

    hyper_call_and_loop(&hyperdex::Client::set_remove, vt_set_space, vt_id, &cl_attr, 1);

    hyper_del_and_loop(vt_map_space, tx_id);
}
