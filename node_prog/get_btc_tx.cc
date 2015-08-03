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
#include "node_prog/parse_btc_tx.h"

using node_prog::search_type;
using node_prog::get_btc_tx_params;
using node_prog::get_btc_tx_state;
using node_prog::cache_response;
using node_prog::Cache_Value_Base;

// params
get_btc_tx_params :: get_btc_tx_params()
{ }

uint64_t
get_btc_tx_params :: size() const
{
    return message::size(ret_node);
}

void
get_btc_tx_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, ret_node);
}

void
get_btc_tx_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, ret_node);
}

get_btc_tx_state :: get_btc_tx_state()
{ }

uint64_t
get_btc_tx_state :: size() const
{
    return 0;
}

void
get_btc_tx_state :: pack(e::packer &) const
{ }

void
get_btc_tx_state :: unpack(e::unpacker &)
{ }

std::pair<search_type, std::vector<std::pair<db::remote_node, get_btc_tx_params>>>
node_prog :: get_btc_tx_node_program(node_prog::node &n,
   db::remote_node &,
   get_btc_tx_params &params,
   std::function<get_btc_tx_state&()>,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
   cache_response<Cache_Value_Base>*)
{
    std::vector<std::pair<db::remote_node, get_btc_tx_params>> next;
    parse_btc_tx(params.ret_node, n);
    next.emplace_back(std::make_pair(db::coordinator, std::move(params)));

    return std::make_pair(search_type::BREADTH_FIRST, std::move(next));
}
