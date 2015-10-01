/*
 * ===============================================================
 *    Description:  Discover paths implementation.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/utils.h"
#include "common/stl_serialization.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/discover_paths.h"

using node_prog::search_type;
using node_prog::discover_paths_params;
using node_prog::discover_paths_state;
using node_prog::dp_len_state;
using node_prog::cache_response;
using node_prog::Cache_Value_Base;

// params
discover_paths_params :: discover_paths_params()
    : path_len(UINT32_MAX)
    , branching_factor(UINT32_MAX)
    , returning(false)
{ }

uint64_t
discover_paths_params :: size() const
{
    return message::size(dest)
         + message::size(path_len)
         + message::size(branching_factor)
         + message::size(node_preds)
         + message::size(edge_preds)
         + message::size(paths)
         + message::size(returning)
         + message::size(prev_node)
         + message::size(src)
         + message::size(path_ancestors);
}

void
discover_paths_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, dest);
    message::pack_buffer(packer, path_len);
    message::pack_buffer(packer, branching_factor);
    message::pack_buffer(packer, node_preds);
    message::pack_buffer(packer, edge_preds);
    message::pack_buffer(packer, paths);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, src);
    message::pack_buffer(packer, path_ancestors);
}

void
discover_paths_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, dest);
    message::unpack_buffer(unpacker, path_len);
    message::unpack_buffer(unpacker, branching_factor);
    message::unpack_buffer(unpacker, node_preds);
    message::unpack_buffer(unpacker, edge_preds);
    message::unpack_buffer(unpacker, paths);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, src);
    message::unpack_buffer(unpacker, path_ancestors);
}

// state
dp_len_state :: dp_len_state()
    : outstanding_count(0)
{ }

uint64_t
dp_len_state :: size() const
{
    return message::size(outstanding_count)
         + message::size(prev_nodes)
         + message::size(paths);
}

void
dp_len_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, outstanding_count);
    message::pack_buffer(packer, prev_nodes);
    message::pack_buffer(packer, paths);
}

void
dp_len_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, outstanding_count);
    message::unpack_buffer(unpacker, prev_nodes);
    message::unpack_buffer(unpacker, paths);
}

discover_paths_state :: discover_paths_state()
{ }

uint64_t
discover_paths_state :: size() const
{
    return message::size(vmap);
}

void
discover_paths_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, vmap);
}

void
discover_paths_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, vmap);
}

void
state_paths_to_params_paths(const std::unordered_map<node_handle_t, node_prog::edge_set> &state_paths,
    std::unordered_map<node_handle_t, std::vector<cl::edge>> &params_paths)
{
    params_paths.clear();
    for (const auto &p: state_paths) {
        std::vector<cl::edge> &evec = params_paths[p.first];
        evec.reserve(p.second.size());
        for (const cl::edge &e: p.second) {
            evec.emplace_back(e);
        }
    }
}

std::pair<search_type, std::vector<std::pair<db::remote_node, discover_paths_params>>>
node_prog :: discover_paths_node_program(node_prog::node &n,
   db::remote_node &rn,
   discover_paths_params &params,
   std::function<discover_paths_state&()> state_getter,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
   cache_response<Cache_Value_Base>*)
{
    discover_paths_state &state = state_getter();
    std::vector<std::pair<db::remote_node, discover_paths_params>> next;

    if (!params.returning) {
        // request spreading out

        auto vmap_iter = state.vmap.find(params.path_len);
        if (vmap_iter != state.vmap.end()) {
            // node already visited
            dp_len_state &dp_state = vmap_iter->second;
            if (dp_state.outstanding_count == 0) {
                // replies already gathered from children
                params.returning = true;
                state_paths_to_params_paths(dp_state.paths, params.paths);
                next.emplace_back(std::make_pair(params.prev_node, params));
            } else {
                // still awaiting replies, enqueue in prev nodes
                dp_state.prev_nodes.emplace_back(params.prev_node);
            }
        } else {
            // visit this node now
            dp_len_state &dp_state = state.vmap[params.path_len];
            if (!n.has_all_predicates(params.node_preds)) {
                // node does not have all required properties, return immediately
                params.returning = true;
                assert(params.paths.empty());
                next.emplace_back(std::make_pair(params.prev_node, params));
            } else {
                if (params.dest == n.get_handle() || n.is_alias(params.dest)) {
                    params.returning = true;
                    dp_state.paths[n.get_handle()] = edge_set();
                    state_paths_to_params_paths(dp_state.paths, params.paths);
                    next.emplace_back(std::make_pair(params.prev_node, params));
                } else if (params.path_len > 0) {
                    dp_state.prev_nodes.emplace_back(params.prev_node);
                    params.path_len--;
                    params.prev_node = rn;
                    params.path_ancestors.emplace(n.get_handle());
                    std::vector<db::remote_node> possible_next;

                    for (edge &e: n.get_edges()) {
                        const db::remote_node &nbr = e.get_neighbor();
                        if (params.path_ancestors.find(nbr.handle) == params.path_ancestors.end()
                        &&  e.has_all_predicates(params.edge_preds)) {
                            possible_next.emplace_back(nbr);
                        }
                    }

                    if (possible_next.empty()) {
                        params.returning = true;
                        params.path_len++;
                        next.emplace_back(std::make_pair(dp_state.prev_nodes[0], params));
                        dp_state.prev_nodes.clear();
                    } else {
                        std::unordered_set<uint32_t> choose_idx;
                        if (params.branching_factor >= possible_next.size()) {
                            for (uint32_t i = 0; i < possible_next.size(); i++) {
                                choose_idx.emplace(i);
                            }
                        }
                        while (choose_idx.size() < params.branching_factor
                            && choose_idx.size() < possible_next.size()) {
                            uint32_t idx = weaver_util::urandom_uint64() % possible_next.size();
                            choose_idx.emplace(idx);
                        }
                        assert(choose_idx.size() == params.branching_factor
                            || choose_idx.size() == possible_next.size());

                        for (uint32_t idx: choose_idx) {
                            next.emplace_back(std::make_pair(possible_next[idx], params));
                            dp_state.outstanding_count++;
                        }
                        dp_state.outstanding_count = choose_idx.size();
                    }
                } else {
                    params.returning = true;
                    assert(params.paths.empty());
                    next.emplace_back(std::make_pair(params.prev_node, params));
                }
            }
        }

    } else {
        // request returning to start node

        params.path_len++;
        auto vmap_iter = state.vmap.find(params.path_len);
        assert(vmap_iter != state.vmap.end());
        dp_len_state &dp_state = vmap_iter->second;

        if (!params.paths.empty()) {
            for (const auto &p: params.paths) {
                if (dp_state.paths.find(p.first) == dp_state.paths.end()) {
                    dp_state.paths.emplace(p.first, edge_set());
                }
                edge_set &eset = dp_state.paths[p.first];
                for (const cl::edge &cl_e: p.second) {
                    eset.emplace(cl_e);
                }
            }

            node_handle_t cur_node;
            if (dp_state.prev_nodes.size() == 1 && dp_state.prev_nodes[0] == db::coordinator) {
                cur_node = params.src;
            } else {
                cur_node = n.get_handle();
            }
            edge_set &eset = dp_state.paths[cur_node];
            for (edge &e: n.get_edges()) {
                node_handle_t nbr = e.get_neighbor().handle;
                if (e.has_all_predicates(params.edge_preds) && dp_state.paths.find(nbr) != dp_state.paths.end()) {
                    cl::edge cl_e;
                    e.get_client_edge(n.get_handle(), cl_e);
                    eset.emplace(cl_e);
                }
            }
        }

        if (--dp_state.outstanding_count == 0) {
            state_paths_to_params_paths(dp_state.paths, params.paths);
            for (db::remote_node &prev: dp_state.prev_nodes) {
                next.emplace_back(std::make_pair(prev, params));
            }
        }
    }

    return std::make_pair(search_type::BREADTH_FIRST, next);
}
