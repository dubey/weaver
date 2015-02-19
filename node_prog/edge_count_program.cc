/*
 * ===============================================================
 *    Description:  Edge count program implementation.
 *
 *        Created:  2014-05-30 11:36:19
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "node_prog/edge_count_program.h"

using node_prog::search_type;
using node_prog::edge_count_params;
using node_prog::cache_response;

// params
uint64_t
edge_count_params :: size() const 
{
    uint64_t toRet = message::size(edges_props)
        + message::size(edge_count);
    return toRet;
}

void edge_count_params :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, edges_props);
    message::pack_buffer(packer, edge_count);
}

void edge_count_params :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, edges_props);
    message::unpack_buffer(unpacker, edge_count);
}

// node prog code
std::pair<search_type, std::vector<std::pair<db::remote_node, edge_count_params>>>
node_prog :: edge_count_node_program(
    node &n,
    db::remote_node &,
    edge_count_params &params,
    std::function<edge_count_state&()>,
    std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
        std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
    cache_response<Cache_Value_Base>*)
{
    auto elist = n.get_edges();
    params.edge_count = 0;
    for (edge &e: elist) {
        if (e.has_all_properties(params.edges_props)) {
            params.edge_count++;
        }
    }

    return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::remote_node, edge_count_params>>
            (1, std::make_pair(db::coordinator, std::move(params)))); 
}
