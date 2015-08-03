/*
 * ===============================================================
 *    Description:  Get BTC tx implementation.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/stl_serialization.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/get_btc_tx.h"

using node_prog::search_type;
using node_prog::get_btc_tx_params;
using node_prog::get_btc_tx_state;
using node_prog::cache_response;
using node_prog::Cache_Value_Base;

// params
get_btc_tx_params :: get_btc_tx_params()
    : returning(false)
{ }

uint64_t
get_btc_tx_params :: size() const
{
    return message::size(tx_handle)
         + message::size(ret_node)
         + message::size(returning)
         + message::size(consumed_map)
         + message::size(tx_rn);
}

void
get_btc_tx_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, tx_handle);
    message::pack_buffer(packer, ret_node);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, consumed_map);
    message::pack_buffer(packer, tx_rn);
}

void
get_btc_tx_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, tx_handle);
    message::unpack_buffer(unpacker, ret_node);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, consumed_map);
    message::unpack_buffer(unpacker, tx_rn);
}

get_btc_tx_state :: get_btc_tx_state()
    : outstanding_count(0)
{ }

uint64_t
get_btc_tx_state :: size() const
{
    return message::size(outstanding_count)
         + message::size(consumed_map);
}

void
get_btc_tx_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, outstanding_count);
    message::pack_buffer(packer, consumed_map);
}

void
get_btc_tx_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, outstanding_count);
    message::unpack_buffer(unpacker, consumed_map);
}

std::pair<search_type, std::vector<std::pair<db::remote_node, get_btc_tx_params>>>
node_prog :: get_btc_tx_node_program(node_prog::node &n,
   db::remote_node &rn,
   get_btc_tx_params &params,
   std::function<get_btc_tx_state&()> state_getter,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
   cache_response<Cache_Value_Base>*)
{
    get_btc_tx_state &state = state_getter();
    std::vector<std::pair<db::remote_node, get_btc_tx_params>> next;
    std::string txout_str = "TXOUT_";

    if (!params.returning) {
        // request spreading out

        if (n.get_handle() == params.tx_handle || n.is_alias(params.tx_handle)) {
            // this is btc tx vertex
            params.tx_rn = rn;
            state.outstanding_count = 0;
            std::vector<std::pair<db::remote_node, std::string>> txs_to_get;
            for (edge &e: n.get_edges()) {
                if (e.get_handle().compare(0, txout_str.size(), txout_str) == 0) {
                    std::string tx_to_get = e.get_handle().substr(txout_str.size());
                    WDEBUG << tx_to_get << std::endl;
                    txs_to_get.emplace_back(std::make_pair(e.get_neighbor(), tx_to_get));
                    state.consumed_map[tx_to_get] = std::string();
                }
            }

            if (txs_to_get.empty()) {
                n.get_client_node(params.ret_node, true, true, true);
                next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
            } else {
                for (const auto &p: txs_to_get) {
                    params.consumed_map.clear();
                    params.consumed_map[p.second] = std::string();
                    next.emplace_back(std::make_pair(p.first, params));
                    state.outstanding_count++;
                }
            }
        } else {
            // this is neighbor of btc tx, i.e. a btc addr
            assert(!params.consumed_map.empty());

            std::unordered_map<edge_handle_t, node_handle_t> consumed_map;
            for (const auto &p: params.consumed_map) {
                std::string consumed_handle = "CXOUT_" + p.first;
                if (n.edge_exists(consumed_handle)) {
                    edge &e = n.get_edge(consumed_handle);
                    consumed_map[p.first] = e.get_neighbor().handle;
                }
            }
            params.returning = true;
            params.consumed_map = consumed_map;
            next.emplace_back(std::make_pair(params.tx_rn, std::move(params)));
        }

    } else {
        // request returning to start node
        assert(n.get_handle() == params.tx_handle || n.is_alias(params.tx_handle));

        for (const auto &p: params.consumed_map) {
            if (!p.second.empty()) {
                state.consumed_map[p.first] = p.second;
            }
        }

        if (--state.outstanding_count == 0) {
            n.get_client_node(params.ret_node, true, true, true);
            for (auto &p: params.ret_node.out_edges) {
                if (p.first.compare(0, txout_str.size(), txout_str) == 0) {
                    std::string tx_to_get = p.first.substr(txout_str.size());
                    auto consumed_iter = state.consumed_map.find(tx_to_get);
                    if (consumed_iter != state.consumed_map.end()
                     && !consumed_iter->second.empty()) {
                        cl::edge &e = p.second;
                        std::shared_ptr<cl::property> consumed_prop(new cl::property());
                        consumed_prop->key = "consumed";
                        consumed_prop->value = consumed_iter->second;
                        e.properties.emplace_back(consumed_prop);
                    }
                }
            }
            next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
        }

    }

    return std::make_pair(search_type::BREADTH_FIRST, std::move(next));
}
