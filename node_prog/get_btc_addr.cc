/*
 * ===============================================================
 *    Description:  Get BTC addr implementation.
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
#include "node_prog/get_btc_addr.h"
#include "node_prog/parse_btc_tx.h"

using node_prog::search_type;
using node_prog::get_btc_addr_params;
using node_prog::get_btc_addr_state;
using node_prog::cache_response;
using node_prog::Cache_Value_Base;

// params
get_btc_addr_params :: get_btc_addr_params()
    : returning(false)
{ }

uint64_t
get_btc_addr_params :: size() const
{
    return message::size(addr_handle)
         + message::size(addr_node)
         + message::size(txs)
         + message::size(returning)
         + message::size(addr_rn);
}

void
get_btc_addr_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, addr_handle);
    message::pack_buffer(packer, addr_node);
    message::pack_buffer(packer, txs);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, addr_rn);
}

void
get_btc_addr_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, addr_handle);
    message::unpack_buffer(unpacker, addr_node);
    message::unpack_buffer(unpacker, txs);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, addr_rn);
}

get_btc_addr_state :: get_btc_addr_state()
    : outstanding_count(0)
{ }

uint64_t
get_btc_addr_state :: size() const
{
    return message::size(outstanding_count)
         + message::size(txs);
}

void
get_btc_addr_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, outstanding_count);
    message::pack_buffer(packer, txs);
}

void
get_btc_addr_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, outstanding_count);
    message::unpack_buffer(unpacker, txs);
}

std::pair<search_type, std::vector<std::pair<db::remote_node, get_btc_addr_params>>>
node_prog :: get_btc_addr_node_program(node_prog::node &n,
   db::remote_node &rn,
   get_btc_addr_params &params,
   std::function<get_btc_addr_state&()> state_getter,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
   cache_response<Cache_Value_Base>*)
{
    get_btc_addr_state &state = state_getter();
    std::vector<std::pair<db::remote_node, get_btc_addr_params>> next;

    if (!params.returning) {
        // request spreading out

        if (n.get_handle() == params.addr_handle || n.is_alias(params.addr_handle)) {
            // this is btc addr vertex
            std::unordered_set<std::string> txs;
            std::vector<db::remote_node> tx_rns;
            params.addr_rn = rn;
            state.outstanding_count = 0;
            for (edge &e: n.get_edges()) {
                if (txs.find(e.get_neighbor().handle) == txs.end()) {
                    txs.emplace(e.get_neighbor().handle);
                    tx_rns.emplace_back(e.get_neighbor());
                }
            }

            if (txs.empty()) {
                n.get_client_node(params.addr_node, true, true, true);
                next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
            } else {
                for (const auto &tx: tx_rns) {
                    next.emplace_back(std::make_pair(tx, params));
                    state.outstanding_count++;
                }
            }
        } else {
            cl::node tx;
            parse_btc_tx(tx, n);
            params.txs.emplace_back(tx);

            params.returning = true;
            next.emplace_back(std::make_pair(params.addr_rn, std::move(params)));
        }

    } else {
        // request returning to start node
        PASSERT(n.get_handle() == params.addr_handle || n.is_alias(params.addr_handle));

        for (const auto &tx: params.txs) {
            state.txs.emplace_back(tx);
        }

        if (--state.outstanding_count == 0) {
            n.get_client_node(params.addr_node, true, true, true);
            params.txs = state.txs;
            next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
        }

    }

    return std::make_pair(search_type::BREADTH_FIRST, std::move(next));
}
