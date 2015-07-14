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
#include "common/stl_serialization.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/cause_and_effect.h"

using node_prog::search_type;
using node_prog::cause_and_effect_params;
using node_prog::cause_and_effect_state;
using node_prog::cdp_len_state;
using node_prog::cache_response;
using node_prog::Cache_Value_Base;

static const int MAX_RESULTS = 10;

// params
cause_and_effect_params :: cause_and_effect_params()
    : path_confid(1.0)
    , returning(false)
{
}

uint64_t
cause_and_effect_params :: size() const
{
    return message::size(dest)
         + message::size(path_confid)
         + message::size(prev_confid)
         + message::size(cutoff_confid)
         + message::size(node_preds)
         + message::size(edge_preds)
         + message::size(paths)
         + message::size(returning)
         + message::size(prev_node)
         + message::size(src)
         + message::size(path_ancestors);
}

void
cause_and_effect_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, dest);
    message::pack_buffer(packer, path_confid);
    message::pack_buffer(packer, prev_confid);
    message::pack_buffer(packer, cutoff_confid);
    message::pack_buffer(packer, node_preds);
    message::pack_buffer(packer, edge_preds);
    message::pack_buffer(packer, paths);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, src);
    message::pack_buffer(packer, path_ancestors);
}

void
cause_and_effect_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, dest);
    message::unpack_buffer(unpacker, path_confid);
    message::unpack_buffer(unpacker, prev_confid);
    message::unpack_buffer(unpacker, cutoff_confid);
    message::unpack_buffer(unpacker, node_preds);
    message::unpack_buffer(unpacker, edge_preds);
    message::unpack_buffer(unpacker, paths);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, src);
    message::unpack_buffer(unpacker, path_ancestors);
}

// state
cdp_len_state :: cdp_len_state()
    : outstanding_count(0)
{ }

uint64_t
cdp_len_state :: size() const
{
    return message::size(outstanding_count)
         + message::size(prev_nodes)
         + message::size(prev_confids)
         + message::size(paths);
}

void
cdp_len_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, outstanding_count);
    message::pack_buffer(packer, prev_nodes);
    message::pack_buffer(packer, prev_confids);
    message::pack_buffer(packer, paths);
}

void
cdp_len_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, outstanding_count);
    message::unpack_buffer(unpacker, prev_nodes);
    message::unpack_buffer(unpacker, prev_confids);
    message::unpack_buffer(unpacker, paths);
}

cause_and_effect_state :: cause_and_effect_state()
{ }

uint64_t
cause_and_effect_state :: size() const
{
    return message::size(vmap);
}

void
cause_and_effect_state :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, vmap);
}

void
cause_and_effect_state :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, vmap);
}

static uint32_t quantize(double f)
{
    return uint32_t(f * 1e6);
}

std::pair<search_type, std::vector<std::pair<db::remote_node, cause_and_effect_params>>>
node_prog :: cause_and_effect_node_program(node_prog::node &n,
   db::remote_node &rn,
   cause_and_effect_params &params,
   std::function<cause_and_effect_state&()> state_getter,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
   cache_response<Cache_Value_Base>*)
{
    cause_and_effect_state &state = state_getter();
    /* node progs to trigger next */
    std::vector<std::pair<db::remote_node, cause_and_effect_params>> next;

    if (!params.returning) {
        // request spreading out

        auto vmap_iter = state.vmap.find(quantize(params.path_confid));
        if (vmap_iter != state.vmap.end()) {
            // node already visited
            cdp_len_state &dp_state = vmap_iter->second;
            if (dp_state.outstanding_count == 0) {
                // replies already gathered from children
                params.returning = true;
                std::swap(params.path_confid, params.prev_confid);
                params.paths = dp_state.paths;
                next.emplace_back(std::make_pair(params.prev_node, params));
            } else {
                // still awaiting replies, enqueue in prev nodes
                dp_state.prev_nodes.emplace_back(params.prev_node);
                dp_state.prev_confids.emplace_back(params.prev_confid);
            }
        } else {
            fprintf(stderr, "propagate to id: %s %d\n", n.get_handle().c_str(),quantize(params.path_confid));
            // visit this node now
            cdp_len_state &dp_state = state.vmap[quantize(params.path_confid)];
            if (!n.has_all_predicates(params.node_preds)) {
                // node does not have all required properties, return immediately
                params.returning = true;
                std::swap(params.path_confid, params.prev_confid);
                assert(params.paths.empty());
                next.emplace_back(std::make_pair(params.prev_node, params));
            } else {
                /* already reaches the target */
                if (params.dest == n.get_handle() || n.is_alias(params.dest)) {
                    fprintf(stderr, "hit\n");
                    params.returning = true;
                    params.paths.emplace_back(1.0, std::vector<node_handle_t>());
                    params.paths[0].second.emplace_back(n.get_handle());
                    std::swap(params.path_confid, params.prev_confid);
                    next.emplace_back(std::make_pair(params.prev_node, params));
                    fprintf(stderr, "return to %s\n", params.prev_node.handle.c_str());
                } else if (params.path_confid > params.cutoff_confid) {
                    dp_state.prev_nodes.emplace_back(params.prev_node);
                    dp_state.prev_confids.emplace_back(params.prev_confid);
                    params.prev_node = rn;
                    params.path_ancestors.emplace(n.get_handle());
                    params.prev_confid = params.path_confid;
                    for (edge &e: n.get_edges()) {
                        const db::remote_node &nbr = e.get_neighbor();
                        if (params.path_ancestors.find(nbr.handle) == params.path_ancestors.end()
                        &&  e.has_all_predicates(params.edge_preds)) {
                            double confid = -1;
                            for (auto iter: e.get_properties()) {
                                if (iter[0]->key == "confidence")
                                {
                                    confid = std::stod(iter[0]->value);
                                    break;
                                }
                            }
                            assert(confid >= 0);
                            params.path_confid = params.prev_confid * confid;
                            next.emplace_back(std::make_pair(nbr, params));
                            dp_state.outstanding_count++;
                        }
                    }

                    if (dp_state.outstanding_count == 0) {
                        params.returning = true;
                        params.prev_confid = params.path_confid;
                        params.path_confid = dp_state.prev_confids[0];
                        next.emplace_back(std::make_pair(dp_state.prev_nodes[0], params));
                        dp_state.prev_nodes.clear();
                        dp_state.prev_confids.clear();
                    }
                } else { /* run out of path length */
                    params.returning = true;
                    std::swap(params.path_confid, params.prev_confid);
                    assert(params.paths.empty());
                    next.emplace_back(std::make_pair(params.prev_node, params));
                }
            }
        }

    } else {
        // request returning to start node
        fprintf(stderr, "back to id: %s %d %d\n", n.get_handle().c_str(),quantize(params.path_confid), quantize(params.prev_confid));
        auto vmap_iter = state.vmap.find(quantize(params.path_confid));
        assert(vmap_iter != state.vmap.end());
        cdp_len_state &dp_state = vmap_iter->second;

        path_res new_paths;
        auto &spaths = dp_state.paths;
        auto &ppaths = params.paths;
        for (auto &path: ppaths)
            path.first *= params.prev_confid / params.path_confid;
        for (auto siter = spaths.begin(),
                    piter = ppaths.begin();
                    (siter != spaths.end() || piter != ppaths.end())
                        && new_paths.size() < MAX_RESULTS;) {
            if (piter == ppaths.end() || (siter != spaths.end() && siter->first < piter->first))
                new_paths.emplace_back(*siter++);
            else
                new_paths.emplace_back(*piter++);
        }

        dp_state.paths = new_paths;

        if (--dp_state.outstanding_count == 0) {

            for (auto &path: dp_state.paths)
                path.second.emplace_back(n.get_handle());

            params.paths = dp_state.paths;
            auto prev_nodes = dp_state.prev_nodes;
            auto prev_confids = dp_state.prev_confids;
            auto niter = prev_nodes.begin();
            auto citer = prev_confids.begin();
            for (;niter != prev_nodes.end(); niter++, citer++) {
                auto &prev = *niter;
                auto &confid = *citer;
                params.prev_confid = params.path_confid;
                params.path_confid = confid;
                next.emplace_back(std::make_pair(prev, params));
            }
        }
    }

    return std::make_pair(search_type::BREADTH_FIRST, next);
}
