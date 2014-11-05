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

hyper_stub :: hyper_stub()
    : vt_id(UINT64_MAX)
{ }

void
hyper_stub :: init(uint64_t vtid)
{
    vt_id = vtid;
}

std::unordered_map<node_handle_t, uint64_t>
hyper_stub :: get_mappings(std::unordered_set<node_handle_t> &get_set)
{
    std::unordered_map<node_handle_t, uint64_t> ret;

    if (get_set.size() == 1) {
        node_handle_t h = *get_set.begin();
        uint64_t loc = get_nmap(h);
        if (loc != UINT64_MAX) {
            ret.emplace(h, loc);
        } else {
            ret.clear();
        }
    } else if (!get_set.empty()) {
        ret = get_nmap(get_set, false);
    }

    return ret;
}

void
hyper_stub :: clean_up(std::unordered_map<node_handle_t, db::element::node*> &nodes)
{
    for (auto &p: nodes) {
        for (auto &e: p.second->out_edges) {
            delete e.second;
        }
        p.second->out_edges.clear();
        delete p.second;
    }
    nodes.clear();
}

void
hyper_stub :: do_tx(std::unordered_set<node_handle_t> &get_set,
    std::unordered_set<node_handle_t> &del_set,
    std::unordered_map<node_handle_t, uint64_t> &put_map,
    transaction::pending_tx *tx,
    bool &ready,
    bool &error,
    order::oracle *time_oracle)
{
#define ERROR_FAIL \
    error = true; \
    abort_tx(); \
    clean_up(nodes); \
    return;

    ready = false;
    error = false;

    std::unordered_map<node_handle_t, db::element::node*> nodes;
    begin_tx();

    std::unordered_map<node_handle_t, uint64_t> get_map = get_nmap(get_set, true);
    if (get_map.size() != get_set.size()) {
        ERROR_FAIL;
    }

    if (!put_nmap(put_map)) {
        ERROR_FAIL;
    }

    if (!del_nmap(del_set)) {
        ERROR_FAIL;
    }

    // get all nodes from Hyperdex (we need at least last upd clk)
    for (const node_handle_t &h: get_set) {
        if (put_map.find(h) != put_map.end()) {
            ERROR_FAIL;
        }
        nodes[h] = new db::element::node(h, dummy_clk, &dummy_mtx);
        if (!get_node(*nodes[h])) {
            ERROR_FAIL;
        }
    }
    for (const node_handle_t &h: del_set) {
        nodes[h] = new db::element::node(h, dummy_clk, &dummy_mtx);
        if (!get_node(*nodes[h])) {
            ERROR_FAIL;
        }
    }

    // last upd clk check
    std::vector<vc::vclock> before;
    before.reserve(nodes.size());
    for (const auto &p: nodes) {
        before.emplace_back(p.second->last_upd_clk);
    }
    if (!time_oracle->assign_vt_order(before, tx->timestamp)) {
        // will retry with higher timestamp
        abort_tx();
        return;
    }

    for (const auto &p: put_map) {
        nodes[p.first] = new db::element::node(p.first, tx->timestamp, &dummy_mtx);
        nodes[p.first]->last_upd_clk = tx->timestamp;
        nodes[p.first]->restore_clk = tx->timestamp.clock;
    }

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
    db::element::node *n = nullptr;
    auto loc_iter = get_map.end();

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

        if (n != nullptr) {
            assert(n->restore_clk.size() == ClkSz);
            n->restore_clk[vt_id+1] = tx->timestamp.get_clock();
        }

        n = nullptr;
    }

#undef CHECK_LOC
#undef GET_NODE

    put_nodes(nodes);

    for (const node_handle_t &h: del_set) {
        del_node(h);
    }

    hyperdex_client_attribute attr[NUM_TX_ATTRS];
    attr[0].attr = tx_attrs[0];
    attr[0].value = (const char*)&vt_id;
    attr[0].value_sz = sizeof(int64_t);
    attr[0].datatype = tx_dtypes[0];

    uint64_t buf_sz = message::size(*tx);
    std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack_at(0);
    message::pack_buffer(packer, *tx);

    attr[1].attr = tx_attrs[1];
    attr[1].value = (const char*)buf->data();
    attr[1].value_sz = buf->size();
    attr[1].datatype = tx_dtypes[1];

    if (!call(&hyperdex_client_xact_put, tx_space, (const char*)&tx->id, sizeof(int64_t), attr, NUM_TX_ATTRS)) {
        ERROR_FAIL;
    }

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

    clean_up(nodes);

#undef ERROR_FAIL
}


void
hyper_stub :: clean_tx(uint64_t tx_id)
{
    begin_tx();
    if (del(tx_space, (const char*)&tx_id, sizeof(int64_t))) {
        hyperdex_client_returncode commit_status = HYPERDEX_CLIENT_GARBAGE;
        commit_tx(commit_status);

        switch (commit_status) {
            case HYPERDEX_CLIENT_SUCCESS:
                break;
            
            default:
                WDEBUG << "problem in committing tx for clean_tx, returned status = "
                       << hyperdex_client_returncode_to_string(commit_status) << std::endl;
        }
    } else {
        abort_tx();
    }
}

// status = false if not prepared
void
hyper_stub :: recreate_tx(const hyperdex_client_attribute *cl_attr,
    transaction::pending_tx &tx)
{
    int tx_data_idx;
    if (strncmp(cl_attr[0].attr, tx_attrs[1], 7) == 0) {
        tx_data_idx = 0;
    } else if (strncmp(cl_attr[2].attr, tx_attrs[1], 7) == 0) {
        tx_data_idx = 2;
    } else {
        assert(strncmp(cl_attr[1].attr, tx_attrs[1], 7) == 0);
        tx_data_idx = 1;
    }
    assert(cl_attr[tx_data_idx].datatype == tx_dtypes[1]);

    std::unique_ptr<e::buffer> buf(e::buffer::create(cl_attr[tx_data_idx].value,
                                                     cl_attr[tx_data_idx].value_sz));
    e::unpacker unpacker = buf->unpack_from(0);
    message::unpack_buffer(unpacker, tx);
}

void
hyper_stub :: restore_backup(std::unordered_map<uint64_t, transaction::pending_tx*> &txs)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;

    // tx list
    const hyperdex_client_attribute_check attr_check = {tx_attrs[0], (const char*)&vt_id, sizeof(int64_t), tx_dtypes[0], HYPERPREDICATE_EQUALS};
    enum hyperdex_client_returncode search_status, loop_status;

    int64_t call_id = hyperdex_client_search(cl, tx_space, &attr_check, 1, &search_status, &cl_attr, &num_attrs);
    if (call_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << call_id
               << ", status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl;
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl;
        return;
    }

    int64_t loop_id;
    bool loop_done = false;
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

        switch (search_status) {
            case HYPERDEX_CLIENT_SEARCHDONE:
            loop_done = true;
            break;

            default: // search_status is HYPERDEX_CLIENT_SUCCESS
            WDEBUG << "search_status = " << hyperdex_client_returncode_to_string(search_status) << ", num attrs: " << num_attrs << std::endl;
            assert(num_attrs == (NUM_TX_ATTRS+1));

            transaction::pending_tx *tx = new transaction::pending_tx(transaction::UPDATE);
            recreate_tx(cl_attr, *tx);
            txs.emplace(tx->id, tx);

            hyperdex_client_destroy_attrs(cl_attr, num_attrs);
        }
    }

    WDEBUG << "Got " << txs.size() << " transactions for vt " << vt_id << std::endl;
}
