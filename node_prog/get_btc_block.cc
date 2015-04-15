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
#include "common/message.h"
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
get_btc_block_params :: pack(e::buffer::packer &packer) const
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
get_btc_block_state :: pack(e::buffer::packer &packer) const
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
            std::string block_tx_str = "BLOCK_TX_";
            params.block_node = rn;
            state.outstanding_count = 0;
            for (edge &e: n.get_edges()) {
                if (e.get_handle().compare(0, block_tx_str.size(), block_tx_str) == 0) {
                    params.tx_to_get = e.get_handle().substr(block_tx_str.size());
                    next.emplace_back(std::make_pair(e.get_neighbor(), params));
                    state.outstanding_count++;
                }
            }

            if (next.empty()) {
                assert(state.outstanding_count == 0);
                n.get_client_node(params.node, true, true, true);
                next.emplace_back(std::make_pair(db::coordinator, std::move(params)));
            }
        } else {
            assert(!params.tx_to_get.empty());
            std::string &tx_handle = params.tx_to_get;
            if (n.edge_exists(tx_handle)) {
                btc_tx_t btc_tx;
                edge &e = n.get_edge(tx_handle);
                cl::edge cl_edge;
                e.get_client_edge(n.get_handle(), cl_edge);
                btc_tx.second.emplace_back(cl_edge);

                std::string tx_id;
                for (auto pvec: e.get_properties()) {
                    for (auto &p: pvec) {
                        if (p->key == "tx_id") {
                            tx_id = p->value;
                        }
                    }
                }
                assert(!tx_id.empty());

                predicate::prop_predicate pred;
                std::vector<predicate::prop_predicate> edge_preds;
                pred.key = "consumed_tx_id";
                std::string tx_out = "TXOUT_";
                pred.value = tx_id.substr(tx_out.size());
                pred.value.pop_back();
                pred.value.pop_back();
                pred.rel = predicate::STARTS_WITH;
                edge_preds.emplace_back(pred);

                for (edge &in_e: n.get_edges()) {
                    if (in_e.has_all_predicates(edge_preds)) {
                        e.get_client_edge(n.get_handle(), cl_edge);
                        btc_tx.first.emplace_back(cl_edge);
                    }
                }

                params.txs.emplace_back(std::move(btc_tx));
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
