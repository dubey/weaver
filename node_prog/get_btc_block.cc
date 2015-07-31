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
         + message::size(node)
         + message::size(txs)
         + message::size(returning)
         + message::size(tx_to_get)
         + message::size(block_node);
}

void
get_btc_block_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, block);
    message::pack_buffer(packer, node);
    message::pack_buffer(packer, txs);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, tx_to_get);
    message::pack_buffer(packer, block_node);
}

void
get_btc_block_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, block);
    message::unpack_buffer(unpacker, node);
    message::unpack_buffer(unpacker, txs);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, tx_to_get);
    message::unpack_buffer(unpacker, block_node);
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
            std::string block_tx_str = "BLOCK_TX_";
            params.block_node = rn;
            state.outstanding_count = 0;
            std::vector<std::pair<db::remote_node, std::string>> txs_to_get;
            for (edge &e: n.get_edges()) {
                if (e.get_handle().compare(0, block_tx_str.size(), block_tx_str) == 0) {
                    std::string tx_to_get = e.get_handle().substr(block_tx_str.size());
                    WDEBUG << tx_to_get << std::endl;
                    txs_to_get.emplace_back(std::make_pair(e.get_neighbor(), tx_to_get));
                }
            }

            if (txs_to_get.empty()) {
                n.get_client_node(params.node, true, true, true);
                next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
            } else {
                for (const auto &p: txs_to_get) {
                    params.tx_to_get.clear();
                    params.tx_to_get.emplace_back(p.second);
                    next.emplace_back(std::make_pair(p.first, params));
                    state.outstanding_count++;
                }
            }
        } else {
            // this is neighbor of btc block, i.e. a btc tx vertex
            assert(!params.tx_to_get.empty());

            std::unordered_map<std::string, btc_tx_t> seen_txs;
            for (const std::string &tx_id: params.tx_to_get) {
                uint64_t tx_index = 0;
                while (true) {
                    std::string tx_handle = "TXOUT_" + tx_id + "_" + std::to_string(tx_index++);
                    if (n.edge_exists(tx_handle)) {
                        WDEBUG << tx_handle << std::endl;
                        edge &e = n.get_edge(tx_handle);
                        cl::edge cl_edge;
                        e.get_client_edge(n.get_handle(), cl_edge);

                        bool exists = seen_txs.find(tx_id) != seen_txs.end();
                        btc_tx_t &btc_tx = seen_txs[tx_id];
                        btc_tx.second.emplace_back(cl_edge);

                        if (!exists) {
                            std::vector<std::string> input_txes;
                            for (auto pvec: e.get_properties()) {
                                for (auto &p: pvec) {
                                    if (p->key == "input_txes") {
                                        input_txes.emplace_back("CXIN_" + p->value);
                                    }
                                }
                            }

                            for (const std::string &itx: input_txes) {
                                if (n.edge_exists(itx)) {
                                    edge &in_e = n.get_edge(itx);
                                    in_e.get_client_edge(n.get_handle(), cl_edge);
                                    btc_tx.first.emplace_back(cl_edge);
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            }

            for (const auto &p: seen_txs) {
                params.txs.emplace_back(p.second);
            }
            params.returning = true;
            next.emplace_back(std::make_pair(params.block_node, std::move(params)));
        }

    } else {
        // request returning to start node
        assert(n.get_handle() == params.block || n.is_alias(params.block));

        for (const auto &tx: params.txs) {
            state.txs.emplace_back(tx);
        }

        if (--state.outstanding_count == 0) {
            n.get_client_node(params.node, true, true, true);
            params.txs = state.txs;
            next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
        }

    }

    return std::make_pair(search_type::BREADTH_FIRST, std::move(next));
}
