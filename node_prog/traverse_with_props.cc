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

#define weaver_debug_
#include "common/stl_serialization.h"
#include "common/cache_constants.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/edge.h"
#include "node_prog/traverse_with_props.h"

using node_prog::search_type;
using node_prog::traverse_props_params;
using node_prog::traverse_props_state;
using node_prog::cache_response;

// params
traverse_props_params :: traverse_props_params()
    : returning(false)
    , collect_nodes(false)
    , collect_edges(false)
{ }

uint64_t
traverse_props_params :: size() const
{
    return message::size(returning)
         + message::size(prev_node)
         + message::size(node_aliases)
         + message::size(node_props)
         + message::size(edge_props)
         + message::size(collect_nodes)
         + message::size(collect_edges)
         + message::size(return_nodes)
         + message::size(return_edges);
}

void
traverse_props_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, node_aliases);
    message::pack_buffer(packer, node_props);
    message::pack_buffer(packer, edge_props);
    message::pack_buffer(packer, collect_nodes);
    message::pack_buffer(packer, collect_edges);
    message::pack_buffer(packer, return_nodes);
    message::pack_buffer(packer, return_edges);
}

void
traverse_props_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, node_aliases);
    message::unpack_buffer(unpacker, node_props);
    message::unpack_buffer(unpacker, edge_props);
    message::unpack_buffer(unpacker, collect_nodes);
    message::unpack_buffer(unpacker, collect_edges);
    message::unpack_buffer(unpacker, return_nodes);
    message::unpack_buffer(unpacker, return_edges);
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
         + message::size(prev_node)
         + message::size(return_nodes)
         + message::size(return_edges);
}

void
traverse_props_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, visited);
    message::pack_buffer(packer, out_count);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, return_nodes);
    message::pack_buffer(packer, return_edges);
}

void
traverse_props_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, visited);
    message::unpack_buffer(unpacker, out_count);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, return_nodes);
    message::unpack_buffer(unpacker, return_edges);
}

bool
check_aliases(const node_prog::node &n, const std::vector<std::string> &aliases)
{
    for (const std::string &alias: aliases) {
        if (!n.is_alias(alias)) {
            return false;
        }
    }

    return true;
}

std::pair<search_type, std::vector<std::pair<db::remote_node, traverse_props_params>>>
node_prog :: traverse_props_node_program(node &n,
   db::remote_node &rn,
   traverse_props_params &params,
   std::function<traverse_props_state&()> state_getter,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
   cache_response<Cache_Value_Base>*)
{
    traverse_props_state &state = state_getter();
    std::vector<std::pair<db::remote_node, traverse_props_params>> next;

    if (!params.returning) {
        // request spreading out

        if (state.visited || !n.has_all_properties(params.node_props.front()) || !check_aliases(n, params.node_aliases.front())) {
            // either this node already visited
            // or node does not have requisite params
            // return now
            params.returning = true;
            next.emplace_back(std::make_pair(params.prev_node, params));
        } else {
            state.prev_node = params.prev_node;
            params.prev_node = rn; // this node
            params.node_aliases.pop_front();
            params.node_props.pop_front();

            if (params.edge_props.empty()) {
                // reached the max hop, return now
                PASSERT(params.node_props.empty());
                params.return_nodes.emplace(n.get_handle());
            } else {
                if (params.collect_nodes) {
                    params.return_nodes.emplace(n.get_handle());
                }
                auto edge_props = params.edge_props.front();
                params.edge_props.pop_front();

                bool collect_edges = params.collect_edges;
                bool propagate = !params.node_props.empty();
                if (!propagate) {
                    PASSERT(params.edge_props.empty());
                    collect_edges = true;
                }

                for (edge &e: n.get_edges()) {
                    if (e.has_all_properties(edge_props)) {
                        if (collect_edges) {
                            params.return_edges.emplace(e.get_handle());
                        }
                        if (propagate) {
                            next.emplace_back(std::make_pair(e.get_neighbor(), params));
                            state.out_count++;
                        }
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
        PASSERT(!next.empty());

    } else {
        // request returning to start node
        for (const node_handle_t &n: params.return_nodes) {
            state.return_nodes.emplace(n);
        }
        for (const edge_handle_t &e: params.return_edges) {
            state.return_edges.emplace(e);
        }

        if (--state.out_count == 0) {
            params.return_nodes = std::move(state.return_nodes);
            params.return_edges = std::move(state.return_edges);
            next.emplace_back(std::make_pair(state.prev_node, params));
        }
    }

    return std::make_pair(search_type::BREADTH_FIRST, next);
}

