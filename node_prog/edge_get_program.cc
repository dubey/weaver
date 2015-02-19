/*
 * ===============================================================
 *    Description:  Get edge program implementation.
 *
 *        Created:  2014-05-30 12:04:56
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "node_prog/edge_get_program.h"

using node_prog::search_type;
using node_prog::edge_get_params;
using node_prog::cache_response;

uint64_t edge_get_params :: size() const 
{
    uint64_t toRet = message::size(nbr_handle)
        + message::size(edges_props)
        + message::size(return_edges);
    return toRet;
}

void edge_get_params :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, nbr_handle);
    message::pack_buffer(packer, edges_props);
    message::pack_buffer(packer, return_edges);
}

void edge_get_params :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, nbr_handle);
    message::unpack_buffer(unpacker, edges_props);
    message::unpack_buffer(unpacker, return_edges);
}

std::pair<search_type, std::vector<std::pair<db::remote_node, edge_get_params>>>
node_prog :: edge_get_node_program(
    node &n,
    db::remote_node &,
    edge_get_params &params,
    std::function<edge_get_state&()>,
    std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
        std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
    cache_response<Cache_Value_Base>*)
{
    auto elist = n.get_edges();
    for (edge &e : elist) {
        if (e.get_neighbor().handle == params.nbr_handle
         && e.has_all_properties(params.edges_props)) {
            params.return_edges.emplace_back(e.get_handle());
        }
    }
    return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::remote_node, edge_get_params>>
            (1, std::make_pair(db::coordinator, std::move(params)))); 
}
