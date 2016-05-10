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
#include "node_prog/edge.h"
#include "node_prog/traverse_with_props.h"

using node_prog::Node_Parameters_Base;
using node_prog::Node_State_Base;
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
traverse_props_params :: size(void *aux_args) const
{
    return message::size(aux_args, returning)
         + message::size(aux_args, prev_node)
         + message::size(aux_args, node_aliases)
         + message::size(aux_args, node_props)
         + message::size(aux_args, edge_props)
         + message::size(aux_args, collect_nodes)
         + message::size(aux_args, collect_edges)
         + message::size(aux_args, return_nodes)
         + message::size(aux_args, return_edges);
}

void
traverse_props_params :: pack(e::packer &packer, void *aux_args) const
{
    message::pack_buffer(packer, aux_args, returning);
    message::pack_buffer(packer, aux_args, prev_node);
    message::pack_buffer(packer, aux_args, node_aliases);
    message::pack_buffer(packer, aux_args, node_props);
    message::pack_buffer(packer, aux_args, edge_props);
    message::pack_buffer(packer, aux_args, collect_nodes);
    message::pack_buffer(packer, aux_args, collect_edges);
    message::pack_buffer(packer, aux_args, return_nodes);
    message::pack_buffer(packer, aux_args, return_edges);
}

void
traverse_props_params :: unpack(e::unpacker &unpacker, void *aux_args)
{
    message::unpack_buffer(unpacker, aux_args, returning);
    message::unpack_buffer(unpacker, aux_args, prev_node);
    message::unpack_buffer(unpacker, aux_args, node_aliases);
    message::unpack_buffer(unpacker, aux_args, node_props);
    message::unpack_buffer(unpacker, aux_args, edge_props);
    message::unpack_buffer(unpacker, aux_args, collect_nodes);
    message::unpack_buffer(unpacker, aux_args, collect_edges);
    message::unpack_buffer(unpacker, aux_args, return_nodes);
    message::unpack_buffer(unpacker, aux_args, return_edges);
}

// state
traverse_props_state :: traverse_props_state()
    : visited(false)
    , out_count(0)
{ }

uint64_t
traverse_props_state :: size(void *aux_args) const
{
    return message::size(aux_args, visited)
         + message::size(aux_args, out_count)
         + message::size(aux_args, prev_node)
         + message::size(aux_args, return_nodes)
         + message::size(aux_args, return_edges);
}

void
traverse_props_state :: pack(e::packer &packer, void *aux_args) const
{
    message::pack_buffer(packer, aux_args, visited);
    message::pack_buffer(packer, aux_args, out_count);
    message::pack_buffer(packer, aux_args, prev_node);
    message::pack_buffer(packer, aux_args, return_nodes);
    message::pack_buffer(packer, aux_args, return_edges);
}

void
traverse_props_state :: unpack(e::unpacker &unpacker, void *aux_args)
{
    message::unpack_buffer(unpacker, aux_args, visited);
    message::unpack_buffer(unpacker, aux_args, out_count);
    message::unpack_buffer(unpacker, aux_args, prev_node);
    message::unpack_buffer(unpacker, aux_args, return_nodes);
    message::unpack_buffer(unpacker, aux_args, return_edges);
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

extern "C" {

PROG_FUNC_DEFINE(traverse_props);

std::pair<search_type, std::vector<std::pair<db::remote_node, std::shared_ptr<Node_Parameters_Base>>>>
node_prog :: node_program(node &n,
   db::remote_node &rn,
   std::shared_ptr<Node_Parameters_Base> param_ptr,
   std::function<Node_State_Base&()> state_getter)
{
    Node_State_Base &state_base = state_getter();
    traverse_props_state& state = dynamic_cast<traverse_props_state&>(state_base);

    Node_Parameters_Base &param_base = *param_ptr;
    traverse_props_params &params = dynamic_cast<traverse_props_params&>(param_base);

    std::vector<std::pair<db::remote_node, std::shared_ptr<Node_Parameters_Base>>> next;

    if (!params.returning) {
        // request spreading out

        if (state.visited || !n.has_all_properties(params.node_props.front()) || !check_aliases(n, params.node_aliases.front())) {
            // either this node already visited
            // or node does not have requisite params
            // return now
            params.returning = true;
            next.emplace_back(std::make_pair(params.prev_node, std::make_shared<traverse_props_params>(params)));
        } else {
            state.prev_node = params.prev_node;
            params.prev_node = rn; // this node
            params.node_aliases.pop_front();
            params.node_props.pop_front();

            if (params.edge_props.empty()) {
                // reached the max hop, return now
                assert(params.node_props.empty());
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
                    assert(params.edge_props.empty());
                    collect_edges = true;
                }

                for (edge &e: n.get_edges()) {
                    if (e.has_all_properties(edge_props)) {
                        if (collect_edges) {
                            params.return_edges.emplace(e.get_handle());
                        }
                        if (propagate) {
                            next.emplace_back(std::make_pair(e.get_neighbor(), std::make_shared<traverse_props_params>(params)));
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
                next.emplace_back(std::make_pair(state.prev_node, std::make_shared<traverse_props_params>(params)));
            }
        }

        state.visited = true;
        assert(!next.empty());

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
            next.emplace_back(std::make_pair(state.prev_node, std::make_shared<traverse_props_params>(params)));
        }
    }

    return std::make_pair(search_type::BREADTH_FIRST, next);
}

}
