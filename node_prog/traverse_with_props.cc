/*
 * ===============================================================
 *    Description:  Implementation of traverse_with_props 
 *
 *        Created:  2014-06-23 11:23:46
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "common/cache_constants.h"
#include "node_prog/edge.h"
#include "node_prog/traverse_with_props.h"

using node_prog::search_type;
using node_prog::traverse_props_params;
using node_prog::traverse_props_state;
using node_prog::cache_response;

// params
traverse_props_params :: traverse_props_params()
    : returning(false)
{ }

uint64_t
traverse_props_params :: size() const
{
    return message::size(returning)
         + message::size(prev_node)
         + message::size(node_props)
         + message::size(edge_props)
         + message::size(return_nodes); 
}

void
traverse_props_params :: pack(e::buffer::packer &packer) const
{
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, node_props);
    message::pack_buffer(packer, edge_props);
    message::pack_buffer(packer, return_nodes);
}

void
traverse_props_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, node_props);
    message::unpack_buffer(unpacker, edge_props);
    message::unpack_buffer(unpacker, return_nodes);
}

// state
traverse_props_state :: traverse_props_state()
    : visited(false)
    , out_count(0)
{ }

uint64_t
traverse_props_state :: size() const
{
    return message::size(visited)
         + message::size(out_count)
         + message::size(prev_node);
}

void
traverse_props_state :: pack(e::buffer::packer &packer) const
{
    message::pack_buffer(packer, visited);
    message::pack_buffer(packer, out_count);
    message::pack_buffer(packer, prev_node);
}

void
traverse_props_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, visited);
    message::unpack_buffer(unpacker, out_count);
    message::unpack_buffer(unpacker, prev_node);
}

std::pair<search_type, std::vector<std::pair<db::element::remote_node, traverse_props_params>>>
node_prog :: traverse_props_node_program(node &n,
   db::element::remote_node &rn,
   traverse_props_params &params,
   std::function<traverse_props_state&()> state_getter,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
   cache_response<Cache_Value_Base>*)
{
    traverse_props_state &state = state_getter();
    std::vector<std::pair<db::element::remote_node, traverse_props_params>> next;

    if (!params.returning) {
        // request spreading out
        assert(params.node_props.size() == (params.edge_props.size()+1));

        if (state.visited || !n.has_all_properties(params.node_props.front())) {
            // either this node already visited
            // or node does not have requisite params
            // return now
            params.returning = true;
            next.emplace_back(std::make_pair(params.prev_node, params));
        } else {
            state.prev_node = params.prev_node;
            params.prev_node = rn; // this node
            params.node_props.pop_front();

            if (params.edge_props.empty()) {
                // reached the max hop, return now
                assert(params.node_props.empty());
                assert(params.return_nodes.empty());
                params.return_nodes.emplace_back(n.get_id());
            } else {
                auto edge_props = params.edge_props.front();
                params.edge_props.pop_front();

                for (edge &e: n.get_edges()) {
                    if (e.has_all_properties(edge_props)) {
                        next.emplace_back(std::make_pair(e.get_neighbor(), params));
                        state.out_count++;
                    }
                }
            }

            if (state.out_count == 0) {
                // either no edges satisfy props
                // or reached max hops
                // return now
                params.returning = true;
                next.emplace_back(std::make_pair(state.prev_node, params));
            }
        }

        state.visited = true;
        assert(!next.empty());

    } else {
        // request returning to start node
        for (uint64_t n: params.return_nodes) {
            state.return_nodes.emplace(n);
        }

        if (--state.out_count == 0) {
            params.return_nodes.clear();
            params.return_nodes.reserve(state.return_nodes.size());
            for (uint64_t n: state.return_nodes) {
                params.return_nodes.emplace_back(n);
            }
            next.emplace_back(std::make_pair(state.prev_node, params));
        }
    }

    return std::make_pair(search_type::BREADTH_FIRST, next);
}

