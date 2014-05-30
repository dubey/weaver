/*
 * ===============================================================
 *    Description:  Implementation of read edge properties program
 *
 *        Created:  2014-05-30 12:15:50
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "node_prog/node.h"
#include "node_prog/edge.h"
#include "node_prog/read_edges_props_program.h"

using node_prog::search_type;
using node_prog::read_edges_props_params;
using node_prog::read_edges_props_state;
using node_prog::cache_response;

uint64_t
read_edges_props_params :: size() const 
{
    uint64_t toRet = message::size(edges)
        + message::size(keys)
        + message::size(edges_props);
    return toRet;
}

void
read_edges_props_params :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, edges);
    message::pack_buffer(packer, keys);
    message::pack_buffer(packer, edges_props);
}

void
read_edges_props_params :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, edges);
    message::unpack_buffer(unpacker, keys);
    message::unpack_buffer(unpacker, edges_props);
}

std::pair<search_type, std::vector<std::pair<db::element::remote_node, read_edges_props_params>>>
node_prog :: read_edges_props_node_program(
    node &n,
    db::element::remote_node&,
    read_edges_props_params &params,
    std::function<read_edges_props_state&()>,
    std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
        std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
    cache_response<Cache_Value_Base>*)
{
    for (edge &edge : n.get_edges()) {
        if (params.edges.empty() || (std::find(params.edges.begin(), params.edges.end(), edge.get_id()) != params.edges.end())) {
            std::vector<std::pair<std::string, std::string>> matching_edge_props;
            for (property &prop : edge.get_properties()) {
                if (params.keys.empty() || (std::find(params.keys.begin(), params.keys.end(), prop.get_key()) != params.keys.end())) {
                    matching_edge_props.emplace_back(prop.get_key(), prop.get_value());
                }
            }
            if (!matching_edge_props.empty()) {
                params.edges_props.emplace_back(edge.get_id(), std::move(matching_edge_props));
            }
        }
    }
    return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::element::remote_node, read_edges_props_params>>
            (1, std::make_pair(db::element::coordinator, std::move(params)))); 
}


