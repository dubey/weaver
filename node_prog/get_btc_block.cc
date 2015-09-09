/*
 * ===============================================================
 *    Description:  Get BTC block implementation.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/stl_serialization.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/get_btc_block.h"
#include "node_prog/parse_btc_tx.h"

using node_prog::search_type;
using node_prog::get_btc_block_params;
using node_prog::get_btc_block_state;
using node_prog::cache_response;
using node_prog::Cache_Value_Base;

// params
get_btc_block_params :: get_btc_block_params()
    : returning(false)
{ }

uint64_t
get_btc_block_params :: size() const
{
    return message::size(block)
         + message::size(block_node)
         + message::size(txs)
         + message::size(returning)
         + message::size(block_rn);
}

void
get_btc_block_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, block);
    message::pack_buffer(packer, block_node);
    message::pack_buffer(packer, txs);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, block_rn);
}

void
get_btc_block_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, block);
    message::unpack_buffer(unpacker, block_node);
    message::unpack_buffer(unpacker, txs);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, block_rn);
}

get_btc_block_state :: get_btc_block_state()
    : outstanding_count(0)
{ }

uint64_t
get_btc_block_state :: size() const
{
    return message::size(outstanding_count)
         + message::size(txs);
}

void
get_btc_block_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, outstanding_count);
    message::pack_buffer(packer, txs);
}

void
get_btc_block_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, outstanding_count);
    message::unpack_buffer(unpacker, txs);
}

std::pair<search_type, std::vector<std::pair<db::remote_node, get_btc_block_params>>>
node_prog :: get_btc_block_node_program(node_prog::node &n,
   db::remote_node &rn,
   get_btc_block_params &params,
   std::function<get_btc_block_state&()> state_getter,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
   cache_response<Cache_Value_Base>*)
{
    get_btc_block_state &state = state_getter();
    std::vector<std::pair<db::remote_node, get_btc_block_params>> next;

    if (!params.returning) {
        // request spreading out

        if (n.get_handle() == params.block || n.is_alias(params.block)) {
            // this is btc block vertex
            WDEBUG << "at btc block=" << n.get_handle() << " aka=" << params.block << std::endl;
            std::string block_tx_str = "BLOCK_TX_";
            params.block_rn = rn;
            state.outstanding_count = 0;
            std::vector<db::remote_node> txs_to_get;
            for (edge &e: n.get_edges()) {
                if (e.get_handle().compare(0, block_tx_str.size(), block_tx_str) == 0) {
                    txs_to_get.emplace_back(e.get_neighbor());
                }
            }

            if (txs_to_get.empty()) {
                n.get_client_node(params.block_node, true, true, true);
                next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
            } else {
                std::string tx_list = "";
                for (const auto &tx: txs_to_get) {
                    next.emplace_back(std::make_pair(tx, params));
                    state.outstanding_count++;
                    tx_list += tx.handle + " ";
                }
                WDEBUG << "prop node prog from block=" << n.get_handle()
                       << " to " << next.size() << " transactions.  List of txs:" << std::endl
                       << tx_list << std::endl;
            }
        } else {
            WDEBUG << "at btc tx=" << n.get_handle()
                   << " for get btc block=" << params.block_rn.handle << std::endl;
            cl::node tx;
            parse_btc_tx(tx, n);
            params.txs.emplace_back(tx);

            params.returning = true;
            next.emplace_back(std::make_pair(params.block_rn, std::move(params)));
        }

    } else {
        // request returning to start node
        assert(n.get_handle() == params.block || n.is_alias(params.block));
        assert(params.txs.size() == 1);
        WDEBUG << "return at btc block=" << params.block
               << " from tx=" << params.txs.front().handle 
               << ", outstanding count=" << (state.outstanding_count-1) << std::endl;

        for (const auto &tx: params.txs) {
            state.txs.emplace_back(tx);
        }

        if (--state.outstanding_count == 0) {
            n.get_client_node(params.block_node, true, true, true);
            params.txs = state.txs;
            next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
        }

    }

    return std::make_pair(search_type::BREADTH_FIRST, std::move(next));
}
