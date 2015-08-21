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

bool
hyper_stub :: get_idx(std::unordered_map<std::string, std::pair<std::string, uint64_t>> &idx_map)
{
    return get_indices(idx_map, false);
}

void
hyper_stub :: clean_node(db::node *n)
{
    for (auto &x: n->out_edges) {
        for (db::edge *e: x.second) {
            delete e;
        }
    }
    n->out_edges.clear();
    delete n;
}

void
hyper_stub :: clean_up(std::unordered_map<node_handle_t, db::node*> &nodes)
{
    for (auto &p: nodes) {
        clean_node(p.second);
    }
    nodes.clear();
}

void
hyper_stub :: clean_up(std::unordered_map<edge_handle_t, db::edge*> &edges)
{
    for (auto &p: edges) {
        delete p.second;
    }
    edges.clear();
}

bool
hyper_stub :: check_lastupd_clk(vc::vclock &before,
                                vc::vclock &tx_clk,
                                order::oracle *time_oracle)
{
    std::vector<vc::vclock> before_vec(1, before);
    return time_oracle->assign_vt_order(before_vec, tx_clk);
}

bool
hyper_stub :: del_key(const char *key, size_t key_sz,
                      const char *space,
                      std::unordered_map<int64_t, async_call_ptr_t> &async_calls)
{
    ad_ptr_t ad = std::make_shared<async_del>();
    return del_async(ad, key, key_sz, space, async_calls);
}

bool
hyper_stub :: loop_async_calls(std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                               std::unordered_map<int64_t, async_call_ptr_t> &done_calls)
{
    uint64_t num_timeouts = 0;
    while (!async_calls.empty()) {
        int64_t op_id;
        hyperdex_client_returncode loop_code;

        loop(op_id, loop_code);

        if (op_id < 0) {
            if (loop_code == HYPERDEX_CLIENT_TIMEOUT) {
                if (num_timeouts % 100 == 0) {
                    WDEBUG << "loop timeout in do_tx #" << num_timeouts << std::endl;
                }
            } else {
                WDEBUG << "loop_failed, op_id=" << op_id
                       << ", code=" << hyperdex_client_returncode_to_string(loop_code)
                       << std::endl;;
                return false;
            }
        } else {
            assert(loop_code == HYPERDEX_CLIENT_SUCCESS);

            auto call_iter = async_calls.find(op_id);
            if (call_iter == async_calls.end()) {
                WDEBUG << "did not find op_id=" << op_id << std::endl;
                return false;
            }

            done_calls.emplace(op_id, call_iter->second);
            async_calls.erase(op_id);
        }
    }

    return true;
}

bool
hyper_stub :: check_calls_status(std::unordered_map<int64_t, async_call_ptr_t> &async_calls, bool allow_cmpfail)
{
    for (auto &p: async_calls) {
        async_call_ptr_t ac_ptr = p.second;
        if (ac_ptr->status == HYPERDEX_CLIENT_SUCCESS
         || (allow_cmpfail && ac_ptr->status == HYPERDEX_CLIENT_CMPFAIL)) {
            continue;
        }

        debug_print_async_call(ac_ptr);
        return false;
    }

    return true;
}

bool
hyper_stub :: loop_and_check(std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                             std::unordered_map<int64_t, async_call_ptr_t> &done_calls,
                             bool allow_cmpfail)
{
    if (!loop_async_calls(async_calls, done_calls)) {
        return false;
    }

    if (!check_calls_status(done_calls, allow_cmpfail)) {
        return false;
    }

    done_calls.clear();

    return true;
}


// TODO make async for gets
void
hyper_stub :: do_tx(std::shared_ptr<transaction::pending_tx> tx,
                    bool &ready,
                    bool &error,
                    order::oracle *time_oracle)
{
    begin_tx();

#define ERROR_FAIL(error_msg) \
    WDEBUG << error_msg << std::endl; \
    error = true; \
    abort_tx(); \
    clean_up(nodes); \
    clean_up(edges); \
    return;

#define GET_NODE(handle) \
    nodes.emplace(handle, new db::node(handle, UINT64_MAX, dummy_clk, &dummy_mtx))

#define GET_EDGE(handle) \
    edges.emplace(handle, nullptr)

#define HANDLE_OR_ALIAS(handle, alias) \
    if (handle != "") { \
        if (seen_nodes.find(handle) == seen_nodes.end()) { \
            seen_nodes.emplace(handle); \
            GET_NODE(handle); \
        } \
    } else if (alias != "") { \
        if (seen_aliases.find(alias) == seen_aliases.end()) { \
            seen_aliases.emplace(alias); \
            GET_ALIAS(alias); \
        } \
    } else { \
        ERROR_FAIL("both handle and alias are empty"); \
    }

#define GET_ALIAS(alias) \
    aliases.emplace(alias, std::make_pair(std::string(), UINT64_MAX))

    // first pass
    // here we optimize the latency for the initial gets in the transaction
    // we iterate through the transaction and collect all nodes, edges, and aliases that we clearly need
    // we then issue a batch get for those keys
    // this parallelizes the requests, as opposed to getting each key sequentially
    // we also perform some sanity checks and fail the tx if any check does not pass

    std::unordered_set<node_handle_t> seen_nodes, seen_edges, seen_aliases;
    std::unordered_map<node_handle_t, db::node*> nodes;
    std::unordered_map<edge_handle_t, db::edge*> edges;
    std::unordered_map<edge_handle_t, aux_edge_data> edge_data;
    std::unordered_map<std::string, std::pair<node_handle_t, uint64_t>> aliases;

    for (std::shared_ptr<transaction::pending_update> upd: tx->writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                seen_nodes.emplace(upd->handle);
                seen_aliases.emplace(upd->handle);
                break;

            case transaction::NODE_DELETE_REQ:
            case transaction::NODE_SET_PROPERTY:
                HANDLE_OR_ALIAS(upd->handle1, upd->alias1);
                break;

            case transaction::EDGE_CREATE_REQ:
                HANDLE_OR_ALIAS(upd->handle1, upd->alias1);
                HANDLE_OR_ALIAS(upd->handle2, upd->alias2);
                seen_edges.emplace(upd->handle);
                break;

            case transaction::EDGE_DELETE_REQ:
            case transaction::EDGE_SET_PROPERTY:
                if (seen_edges.find(upd->handle1) == seen_edges.end()) {
                    seen_edges.emplace(upd->handle1);
                    GET_EDGE(upd->handle1);
                }
                if (upd->handle2 != ""
                 && seen_nodes.find(upd->handle2) == seen_nodes.end()) {
                    seen_nodes.emplace(upd->handle2);
                    GET_NODE(upd->handle2);
                }
                break;

            case transaction::ADD_AUX_INDEX:
                if (seen_nodes.find(upd->handle1) == seen_nodes.end()) {
                    seen_nodes.emplace(upd->handle1);
                    GET_NODE(upd->handle1);
                }
                break;
        }
    }

#undef HANDLE_OR_ALIAS

    // get aliases
    if (!aliases.empty() && !get_indices(aliases, true)) {
        ERROR_FAIL("get indices");
    }

#undef GET_ALIAS

    for (const auto &p: aliases) {
        if (seen_nodes.find(p.second.first) == seen_nodes.end()) {
            GET_NODE(p.second.first);
        }
    }

    // get edges
    if (!edges.empty()) {
        aux_edge_data data;
        for (auto &p: edges) {
            if (get_edge(p.first, &p.second, data, true)) {
                edge_data[p.first] = data;
                if (seen_nodes.find(data.node_handle) == seen_nodes.end()) {
                    GET_NODE(data.node_handle);
                }
            } else {
                ERROR_FAIL("get_edge, handle=" << p.first);
            }
        }
    }

#undef GET_EDGE

    // get nodes
    if (!get_nodes(nodes, false, true)) {
        ERROR_FAIL("get_nodes");
    }

#undef GET_NODE

    std::unordered_map<int64_t, async_call_ptr_t> async_calls,
                                                  done_calls;
    vc::vclock_ptr_t tx_clk_ptr(new vc::vclock(tx->timestamp));
    auto restore_clk = std::make_shared<vc::vclock_t>(tx_clk_ptr->clock);
    std::unique_ptr<e::buffer> lastupd_clk_buf, restore_clk_buf;
    prepare_buffer(tx_clk_ptr, lastupd_clk_buf);
    prepare_buffer(restore_clk, restore_clk_buf);
    std::vector<vc::vclock> before_vec;
    // done with initial optimization gets
    // now iterate through tx and process each op

#define CHECK_LASTUPD_CLK(node) \
    if (tx->timestamp != *node->last_upd_clk) { \
        before_vec.clear(); \
        before_vec.emplace_back(*node->last_upd_clk); \
        if (!time_oracle->assign_vt_order(before_vec, tx->timestamp)) { \
            abort_tx(); \
            return; \
        } \
    }
    //    *node->last_upd_clk = tx->timestamp;
    //(*node->restore_clk)[vt_id+1] = tx_clk_ptr->get_clock();

#define CHECK_NODE(handle, alias, loc) \
    if (handle == "") { \
        auto alias_iter = aliases.find(alias); \
        if (alias_iter == aliases.end()) { \
            ERROR_FAIL("logical error, expected alias=" << alias); \
        } \
        handle = alias_iter->second.first; \
    } \
    if (nodes.find(handle) == nodes.end()) { \
        ERROR_FAIL("logical error, not found node=" << handle); \
    } \
    db::node *n = nodes[handle]; \
    CHECK_LASTUPD_CLK(n); \
    loc = n->shard;

    for (std::shared_ptr<transaction::pending_update> upd: tx->writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ: {
                if (nodes.find(upd->handle) != nodes.end()) {
                    ERROR_FAIL("cannot create node already created/existing in this transaction, handle=" << upd->handle);
                }
                db::node *n = new db::node(upd->handle,
                                           upd->loc1,
                                           tx_clk_ptr,
                                           &dummy_mtx);
                n->last_upd_clk.reset(new vc::vclock(*tx_clk_ptr));
                n->restore_clk.reset(new vc::vclock_t(tx_clk_ptr->clock));
                n->max_edge_id = m_uint64max_dist(m_mt64_gen);

                nodes.emplace(upd->handle, n);

                apn_ptr_t apn = std::make_shared<async_put_node>();
                if (!put_node_async(apn,
                                    nodes[upd->handle],
                                    lastupd_clk_buf,
                                    restore_clk_buf,
                                    async_calls,
                                    true,
                                    true)) {
                    ERROR_FAIL("put node async");
                }
                             
                aai_ptr_t aai = std::make_shared<async_add_index>();
                if (!add_index_async(aai, upd->handle, upd->handle, n->shard, async_calls, true, true)) {
                    ERROR_FAIL("add_index_async, node=" << upd->handle << ", alias=" << upd->handle);
                }

                if (!loop_and_check(async_calls, done_calls, false)) {
                    ERROR_FAIL("loop_and_check");
                }

                break;
            }

            case transaction::EDGE_CREATE_REQ: {
                if (edges.find(upd->handle) != edges.end()) {
                    ERROR_FAIL("cannot create edge already created/existing in this transaction, handle=" << upd->handle);
                }

                if (upd->handle1 == "") {
                    auto &x = aliases[upd->alias1];
                    upd->handle1 = x.first;
                    upd->loc1    = x.second;
                }
                if (upd->handle2 == "") {
                    auto &x = aliases[upd->alias2];
                    upd->handle2 = x.first;
                    upd->loc2    = x.second;
                }

                auto node1_iter = nodes.find(upd->handle1);
                auto node2_iter = nodes.find(upd->handle2);
                if (node1_iter == nodes.end() || node2_iter == nodes.end()) {
                    ERROR_FAIL("logical error" << std::endl
                               << "node1=" << upd->handle1 << ", found=" << (node1_iter == nodes.end()) << std::endl
                               << "node2=" << upd->handle2 << ", found=" << (node2_iter == nodes.end()));
                }

                db::node *node1 = node1_iter->second;
                db::node *node2 = node2_iter->second;
                CHECK_LASTUPD_CLK(node1);
                CHECK_LASTUPD_CLK(node2);
                upd->loc1 = node1->shard;
                upd->loc2 = node2->shard;
                db::edge *e = new db::edge(upd->handle,
                                           tx_clk_ptr,
                                           upd->loc2,
                                           upd->handle2);

                edges.emplace(upd->handle, e);

                apei_ptr_t apei = std::make_shared<async_put_edge_id>();
                do {
                    uint64_t edge_id = node1->max_edge_id++;
                    e->edge_id       = edge_id;
                    if (!put_edge_id_async(apei,
                                           edge_id,
                                           upd->handle,
                                           async_calls,
                                           true)) {
                        ERROR_FAIL("put edge id async, edge_id=" << edge_id);
                    }

                    if (!loop_and_check(async_calls, done_calls, true)) {
                        ERROR_FAIL("loop_and_check");
                    }
                } while (apei->status == HYPERDEX_CLIENT_CMPFAIL);

                if (apei->status != HYPERDEX_CLIENT_SUCCESS) {
                    ERROR_FAIL("apei");
                }

                ape_ptr_t ape = std::make_shared<async_put_edge>();
                if (!put_edge_async(ape,
                                    node1->get_handle(),
                                    e,
                                    e->edge_id,
                                    node1->shard,
                                    false,
                                    async_calls,
                                    true,
                                    true)) {
                    ERROR_FAIL("put_edge");
                }
                if (!loop_and_check(async_calls, done_calls, false)) {
                    ERROR_FAIL("loop_and_check");
                }

                node1->edge_ids.emplace(e->edge_id);

                aux_edge_data aux_data;
                aux_data.node_handle   = upd->handle1;
                aux_data.shard         = upd->loc1;
                aux_data.edge_id       = e->edge_id;
                edge_data[upd->handle] = aux_data;

                break;
            }

            case transaction::NODE_DELETE_REQ: {
                CHECK_NODE(upd->handle1, upd->alias1, upd->loc1);

                if (!del_key(upd->handle1.c_str(), upd->handle1.size(),
                             node_space,
                             async_calls)) {
                    ERROR_FAIL("delete node=" << upd->handle1);
                }
                for (const std::string &alias: n->aliases) {
                    if (!del_key(alias.c_str(), alias.size(),
                                 edge_space,
                                 async_calls)) {
                        ERROR_FAIL("delete alias=" << alias);
                    }
                    aliases.erase(alias);
                }
                if (!loop_and_check(async_calls, done_calls, false)) {
                    ERROR_FAIL("node delete");
                }

                delete n;
                nodes.erase(upd->handle1);
                break;
            }

            case transaction::NODE_SET_PROPERTY: {
                CHECK_NODE(upd->handle1, upd->alias1, upd->loc1);

                if (!n->base.set_property(*upd->key, *upd->value, tx_clk_ptr)) {
                    ERROR_FAIL("set property " << *upd->key << ": " << *upd->value << " fail at node=" << upd->handle1);
                }
                break;
            }

            case transaction::EDGE_DELETE_REQ:
            case transaction::EDGE_SET_PROPERTY: {
                if (edges.find(upd->handle1) == edges.end()) {
                    ERROR_FAIL("logical error, did not find edge=" << upd->handle1);
                }

                db::edge *e = edges[upd->handle1];
                aux_edge_data &aux_data = edge_data[upd->handle1];
                if (upd->handle2 == "") {
                    upd->handle2 = aux_data.node_handle;
                } else if (upd->handle2 != aux_data.node_handle) {
                    ERROR_FAIL("edge=" << upd->handle1 << " not found at node=" << upd->handle1);
                }

                CHECK_NODE(upd->handle2, upd->alias2, upd->loc1);

                if (n->edge_ids.find(aux_data.edge_id) == n->edge_ids.end()) {
                    ERROR_FAIL("logical error, did not find edge_id=" << aux_data.edge_id << " at node=" << upd->handle2);
                }

                if (upd->type == transaction::EDGE_DELETE_REQ) {
                    if (!del_key((const char *)&aux_data.edge_id, sizeof(int64_t),
                                 edge_id_space,
                                 async_calls)) {
                        ERROR_FAIL("delete edge_id=" << aux_data.edge_id);
                    }
                    if (!del_key(upd->handle1.c_str(), upd->handle1.size(),
                                 edge_space,
                                 async_calls)) {
                        ERROR_FAIL("delete edge=" << upd->handle1);
                    }
                    if (!loop_and_check(async_calls, done_calls, false)) {
                        ERROR_FAIL("delete edge");
                    }

                    n->edge_ids.erase(aux_data.edge_id);
                    delete e;
                    edges.erase(upd->handle1);
                    edge_data.erase(upd->handle1);
                } else {
                    if (!e->base.set_property(*upd->key, *upd->value, tx_clk_ptr)) {
                        ERROR_FAIL("property " << *upd->key << ": " << *upd->value << " fail at edge " << upd->handle1);
                    }
                }

                break;
            }

            case transaction::ADD_AUX_INDEX: {
                CHECK_NODE(upd->handle1, upd->alias1, upd->loc1);

                aai_ptr_t aai = std::make_shared<async_add_index>();
                if (!add_index_async(aai, upd->handle1, upd->handle, n->shard, async_calls, true, true)) {
                    ERROR_FAIL("add_index_async, node=" << upd->handle1 << ", alias=" << upd->handle);
                }
                if (!loop_and_check(async_calls, done_calls, false)) {
                    ERROR_FAIL("add aux index");
                }

                n->add_alias(upd->handle);

                auto alias_iter = aliases.find(upd->handle);
                auto alias_pair = std::make_pair(upd->handle1, n->shard);
                if (alias_iter != aliases.end()
                 && alias_iter->second != alias_pair) {
                    ERROR_FAIL("conflicting aliases " << upd->handle);
                }
                aliases.emplace(upd->handle, alias_pair);
                break;
            }
        }

        uint64_t shard_write_idx = upd->loc1 - ShardIdIncr;
        if (shard_write_idx >= tx->shard_write.size()) {
            tx->shard_write.resize(shard_write_idx+1, false);
        }
        tx->shard_write[shard_write_idx] = true;
    }

#undef CHECK_LASTUPD_CLK
#undef CHECK_NODE

    assert(async_calls.empty());

    // write all nodes
    for (auto &p: nodes) {
        apn_ptr_t apn = std::make_shared<async_put_node>();
        if (!put_node_async(apn,
                            p.second,
                            lastupd_clk_buf,
                            restore_clk_buf,
                            async_calls,
                            false,
                            true)) {
            debug_print_async_call(apn);
            ERROR_FAIL("put_node");
        }
    }

    // write all edges
    for (auto &p: edges) {
        ape_ptr_t ape = std::make_shared<async_put_edge>();
        aux_edge_data &aux_data = edge_data[p.first];
        if (!put_edge_async(ape,
                            aux_data.node_handle,
                            p.second,
                            aux_data.edge_id,
                            aux_data.shard,
                            false,
                            async_calls,
                            false,
                            true)) {
            debug_print_async_call(ape);
            ERROR_FAIL("put_edge");
        }
    }

    if (!loop_and_check(async_calls, done_calls, false)) {
        ERROR_FAIL("loop_and_check");
    }

    // write tx data
    hyperdex_client_attribute attr[NUM_TX_ATTRS];
    attr[0].attr = tx_attrs[0];
    attr[0].value = (const char*)&vt_id;
    attr[0].value_sz = sizeof(int64_t);
    attr[0].datatype = tx_dtypes[0];

    uint64_t buf_sz = message::size(*tx);
    std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
    e::packer packer = buf->pack_at(0);
    message::pack_buffer(packer, *tx);

    attr[1].attr = tx_attrs[1];
    attr[1].value = (const char*)buf->data();
    attr[1].value_sz = buf->size();
    attr[1].datatype = tx_dtypes[1];

    if (!call(&hyperdex_client_xact_put,
              tx_space,
              (const char*)&tx->id, sizeof(int64_t),
              attr, NUM_TX_ATTRS)) {
        ERROR_FAIL("hyperdex tx put error, tx id " << tx->id);
    }

    // commit tx
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
    clean_up(edges);

    WDEBUG << std::endl << std::endl << "DONE TX" << std::endl << std::endl;

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
hyper_stub :: restore_backup(std::vector<std::shared_ptr<transaction::pending_tx>> &txs)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;

    // tx list
    const hyperdex_client_attribute_check attr_check = {tx_attrs[0], (const char*)&vt_id, sizeof(int64_t), tx_dtypes[0], HYPERPREDICATE_EQUALS};
    enum hyperdex_client_returncode search_status, loop_status;

    int64_t call_id = hyperdex_client_search(m_cl, tx_space, &attr_check, 1, &search_status, &cl_attr, &num_attrs);
    if (call_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << call_id
               << ", status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
        WDEBUG << "error message: " << hyperdex_client_error_message(m_cl) << std::endl;
        WDEBUG << "error loc: " << hyperdex_client_error_location(m_cl) << std::endl;
        return;
    }

    int64_t loop_id;
    bool loop_done = false;
    std::shared_ptr<transaction::pending_tx> tx;
    while (!loop_done) {
        // loop until search done
        loop_id = hyperdex_client_loop(m_cl, -1, &loop_status);
        if (loop_id != call_id
         || loop_status != HYPERDEX_CLIENT_SUCCESS
         || (search_status != HYPERDEX_CLIENT_SUCCESS && search_status != HYPERDEX_CLIENT_SEARCHDONE)) {
            WDEBUG << "Hyperdex function failed, call id = " << call_id
                   << ", loop_id = " << loop_id
                   << ", loop status = " << hyperdex_client_returncode_to_string(loop_status)
                   << ", search status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
            WDEBUG << "error message: " << hyperdex_client_error_message(m_cl) << std::endl;
            WDEBUG << "error loc: " << hyperdex_client_error_location(m_cl) << std::endl;
            return;
        }

        switch (search_status) {
            case HYPERDEX_CLIENT_SEARCHDONE:
            loop_done = true;
            break;

            case HYPERDEX_CLIENT_SUCCESS:
            assert(num_attrs == (NUM_TX_ATTRS+1));

            tx = std::make_shared<transaction::pending_tx>(transaction::UPDATE);
            recreate_tx(cl_attr, *tx);
            txs.emplace_back(tx);
            hyperdex_client_destroy_attrs(cl_attr, num_attrs);
            break;

            default:
            WDEBUG << "unexpected hyperdex client status on search: " << search_status << std::endl;
        }
    }

    WDEBUG << "Got " << txs.size() << " transactions for vt " << vt_id << std::endl;
}
