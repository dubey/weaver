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
    : num_nodes_read(0)
    , returning(false)
{ }

uint64_t
get_btc_block_params :: size() const
{
    return message::size(block)
         + message::size(block_node)
         + message::size(txs)
         + message::size(num_nodes_read)
         + message::size(returning)
         + message::size(block_rn);
}

void
get_btc_block_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, block);
    message::pack_buffer(packer, block_node);
    message::pack_buffer(packer, txs);
    message::pack_buffer(packer, num_nodes_read);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, block_rn);
}

void
get_btc_block_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, block);
    message::unpack_buffer(unpacker, block_node);
    message::unpack_buffer(unpacker, txs);
    message::unpack_buffer(unpacker, num_nodes_read);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, block_rn);
}

get_btc_block_state :: get_btc_block_state()
    : outstanding_count(0)
    , num_nodes_read(0)
{ }

uint64_t
get_btc_block_state :: size() const
{
    return message::size(outstanding_count)
         + message::size(outstanding)
         + message::size(txs)
         + message::size(num_nodes_read);
}

void
get_btc_block_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, outstanding_count);
    message::pack_buffer(packer, outstanding);
    message::pack_buffer(packer, txs);
    message::pack_buffer(packer, num_nodes_read);
}

void
get_btc_block_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, outstanding_count);
    message::unpack_buffer(unpacker, outstanding);
    message::unpack_buffer(unpacker, txs);
    message::unpack_buffer(unpacker, num_nodes_read);
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
            //WDEBUG << "at btc block=" << n.get_handle() << " aka=" << params.block << std::endl;
            std::string block_tx_str = "BLOCK_TX_";
            params.block_rn = rn;
            //state.outstanding.clear();
            std::vector<db::remote_node> txs_to_get;
            for (edge &e: n.get_edges()) {
                if (e.get_handle().compare(0, block_tx_str.size(), block_tx_str) == 0) {
                    txs_to_get.emplace_back(e.get_neighbor());
                }
            }
            state.outstanding_count = txs_to_get.size();

            if (txs_to_get.empty()) {
                params.num_nodes_read = 1;
                n.get_client_node(params.block_node, true, true, true);
                next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
            } else {
                state.num_nodes_read = 2 + txs_to_get.size();
                for (const auto &tx: txs_to_get) {
                    next.emplace_back(std::make_pair(tx, params));
                    //WDEBUG << "prop node prog from block=" << n.get_handle()
                    //       << " to tx=" << tx.handle << " at shard=" << tx.loc
                    //       << std::endl;
                    //state.outstanding[tx.handle] = tx.loc;
                }
                //WDEBUG << "sent node prog from block=" << n.get_handle()
                //       << " to " << next.size() << " transactions."
                //       << std::endl;
            }
        } else {
            //WDEBUG << "at btc tx=" << n.get_handle()
            //       << " for get btc block=" << params.block_rn.handle << std::endl;
            cl::node tx;
            parse_btc_tx(tx, n);
            params.txs.emplace_back(tx);

            params.returning = true;
            next.emplace_back(std::make_pair(params.block_rn, std::move(params)));
        }

    } else {
        // request returning to start node
        PASSERT(n.get_handle() == params.block || n.is_alias(params.block));
        PASSERT(params.txs.size() == 1);
        //PASSERT(state.outstanding.size() == state.outstanding_count);

        //state.outstanding.erase(params.txs.front().handle);

        //WDEBUG << "return at btc block=" << params.block
        //       << " from tx=" << params.txs.front().handle 
        //       << ", outstanding count=" << (state.outstanding_count-1) << std::endl;
        //if (!state.outstanding.empty()) {
        //    for (const auto &p: state.outstanding) {
        //        WDEBUG << "pending tx=" << p.first << " from loc=" << p.second << std::endl;
        //    }
        //}

        for (const auto &tx: params.txs) {
            state.txs.emplace_back(tx);
        }

        if (--state.outstanding_count == 0) {
            n.get_client_node(params.block_node, true, true, true);
            params.txs = state.txs;
            params.num_nodes_read = state.num_nodes_read;
            next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
        }

    }

    return std::make_pair(search_type::BREADTH_FIRST, std::move(next));
}
