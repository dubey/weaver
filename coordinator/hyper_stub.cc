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
#include "coordinator/hyper_stub.h"

using coordinator::hyper_stub;
using coordinator::current_tx;

hyper_stub :: hyper_stub(uint64_t vtid, bool put_initial)
    : vt_id(vtid)
    , map_dtypes{HYPERDATATYPE_STRING, HYPERDATATYPE_INT64}
{
    if (put_initial) {
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

        call(&hyperdex::Client::put, vt_set_space, (const char*)&vt_id, sizeof(int64_t), &cl_attr, 1);
    }
}

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
