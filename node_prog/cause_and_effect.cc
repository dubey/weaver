/*
 * ===============================================================
 *    Description:  Cause and effect implementation.
 *
 *         Author:  Ted Yin, ted.sybil@gmail.com
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
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
using node_prog::cause_and_effect_substate;
using node_prog::cache_response;
using node_prog::Cache_Value_Base;

// params
cause_and_effect_params :: cause_and_effect_params()
    : cutoff_confid(0.1)
    , confidence(1.0)
    , path_hash(0)
    , prev_path_hash(0)
    , max_results(10)
    , returning(false)
{
}

uint64_t
cause_and_effect_params :: size() const
{
    return message::size(dest)
         + message::size(cutoff_confid)
         + message::size(node_preds)
         + message::size(edge_preds)
         + message::size(confidence)
         + message::size(ancestors)
         + message::size(path_id)
         + message::size(path_hash)
         + message::size(prev_path_hash)
         + message::size(max_results)
         + message::size(paths)
         + message::size(returning)
         + message::size(prev_node)
         ;
}

void
cause_and_effect_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, dest);
    message::pack_buffer(packer, cutoff_confid);
    message::pack_buffer(packer, node_preds);
    message::pack_buffer(packer, edge_preds);
    message::pack_buffer(packer, confidence);
    message::pack_buffer(packer, ancestors);
    message::pack_buffer(packer, path_id);
    message::pack_buffer(packer, path_hash);
    message::pack_buffer(packer, prev_path_hash);
    message::pack_buffer(packer, max_results);
    message::pack_buffer(packer, paths);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, prev_node);
}

void
cause_and_effect_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, dest);
    message::unpack_buffer(unpacker, cutoff_confid);
    message::unpack_buffer(unpacker, node_preds);
    message::unpack_buffer(unpacker, edge_preds);
    message::unpack_buffer(unpacker, confidence);
    message::unpack_buffer(unpacker, ancestors);
    message::unpack_buffer(unpacker, path_id);
    message::unpack_buffer(unpacker, path_hash);
    message::unpack_buffer(unpacker, prev_path_hash);
    message::unpack_buffer(unpacker, max_results);
    message::unpack_buffer(unpacker, paths);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, prev_node);
}

// state
cause_and_effect_substate :: cause_and_effect_substate()
    : outstanding_count(0)
{ }

uint64_t
cause_and_effect_substate :: size() const
{
    return message::size(outstanding_count)
         + message::size(confidence)
         + message::size(prev_path_hash)
         + message::size(path_hash)
         + message::size(path_id)
         + message::size(prev_node)
         + message::size(paths)
         ;
}

void
cause_and_effect_substate :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, outstanding_count);
    message::pack_buffer(packer, confidence);
    message::pack_buffer(packer, prev_path_hash);
    message::pack_buffer(packer, path_hash);
    message::pack_buffer(packer, path_id);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, paths);
}

void
cause_and_effect_substate :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, outstanding_count);
    message::unpack_buffer(unpacker, confidence);
    message::unpack_buffer(unpacker, prev_path_hash);
    message::unpack_buffer(unpacker, path_hash);
    message::unpack_buffer(unpacker, path_id);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, paths);
}

void
cause_and_effect_substate::get_prev_substate_identifier(cause_and_effect_params &params)
{
    params.path_hash = prev_path_hash;
    /* copy path handle */
    params.path_id = path_id;
    params.path_id.pop();
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

uint32_t incremental_bkdr_hash(uint32_t hv, const node_handle_t &node)
{
    static const uint32_t seed = 131;
    for (char ch: node)
        hv = hv * seed + ch;
    hv = hv * seed + '\0';
    return hv;
}

cause_and_effect_substate *cause_and_effect_state::get_substate(const cause_and_effect_params &params, bool create = false)
{
    auto iter = vmap.find(params.path_hash);
    if (create)
    {
        std::vector<cause_and_effect_substate> *substates;
        if (iter == vmap.end())
        {
            substates = &(vmap.emplace(params.path_hash, std::vector<cause_and_effect_substate>()).first->second);
        }
        else
            substates = &(iter->second);
        substates->emplace_back();
        cause_and_effect_substate *substate = &*substates->rbegin();
        return substate;
    }
    else
    {
        if (iter == vmap.end())
            return nullptr;
        for (auto &substate: iter->second) {
            if (substate.path_id == substate.path_id)
            return &substate;
        }
        return nullptr;
    }
}

void node_prog::path_handle::pop() {
    if (!nodes.empty())
    {
        nodes.pop_back();
        edges.pop_back();
    }
}

uint64_t node_prog::path_handle::size() const {
    return message::size(nodes) + message::size(edges);
}

void node_prog::path_handle::pack(e::packer &packer) const {
    message::pack_buffer(packer, nodes);
    message::pack_buffer(packer, edges);
}

void node_prog::path_handle::unpack(e::unpacker &unpacker) {
    message::unpack_buffer(unpacker, nodes);
    message::unpack_buffer(unpacker, edges);
}

bool node_prog::path_handle::operator==(const path_handle &b) const {
    if (nodes.size() != b.nodes.size())
        return false;
    for (size_t i = 0; i < nodes.size(); i++)
        if (nodes[i] != b.nodes[i] || edges[i] != b.edges[i])
            return false;
    return true;
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
    node_handle_t cur_handle = n.get_handle();

    if (!params.returning) {
        // request spreading out

        cause_and_effect_substate *ret = state.get_substate(params);
        if (ret != nullptr) {
            /* node with the same substate already visited */
            assert(0 && "impossible");
        } else {
            /* fprintf(stderr, "propagate to id: %s %.3f\n", cur_handle.c_str(),params.confidence); */
            // visit this node now
            cause_and_effect_substate &dp_state = *state.get_substate(params, true);
            dp_state.confidence = params.confidence;
            dp_state.path_hash = params.path_hash;
            dp_state.prev_path_hash = params.prev_path_hash;
            dp_state.path_id = params.path_id;
            dp_state.prev_node = params.prev_node;

            if (!n.has_all_predicates(params.node_preds)) {
                // node does not have all required properties, return immediately
                params.returning = true;
                dp_state.get_prev_substate_identifier(params);
                assert(params.paths.empty());
                next.emplace_back(std::make_pair(params.prev_node, params));
                /* fprintf(stderr, "returning to %s\n", params.prev_node.handle.c_str()); */
            } else {
                /* already reaches the target */
                if (params.dest == cur_handle || n.is_alias(params.dest)) {
                    fprintf(stderr, "hit\n");
                    params.returning = true;
                    dp_state.get_prev_substate_identifier(params);
                    params.paths.emplace_back(dp_state.confidence, std::vector<node_handle_t>());
                    params.paths[0].second.emplace_back(cur_handle);
                    next.emplace_back(std::make_pair(params.prev_node, params));
                    /* fprintf(stderr, "returning to %s\n", params.prev_node.handle.c_str()); */
                } else if (params.confidence > params.cutoff_confid) {
                    double prev_conf = params.confidence;
                    cause_and_effect_params params0 = params;
                    params.prev_node = rn;
                    params.ancestors.emplace(cur_handle);
                    params.path_id.nodes.emplace_back(cur_handle);
                    params.prev_path_hash = params.path_hash;
                    double path_hash = incremental_bkdr_hash(params.path_hash,
                                                    cur_handle);
                    /* fprintf(stderr, "%s old_hv: %u new_hv: %u\n", cur_handle.c_str(), params.prev_path_hash, params.path_hash); */

                    for (edge &e: n.get_edges()) {
                        const db::remote_node &nbr = e.get_neighbor();
                        if (params.ancestors.find(nbr.handle) == params.ancestors.end()
                            && e.has_all_predicates(params.edge_preds)) {
                            double confid = -1;
                            for (auto iter: e.get_properties()) {
                                if (iter[0]->key == "confidence")
                                {
                                    confid = std::stod(iter[0]->value);
                                    break;
                                }
                            }
                            assert(confid >= 0);
                            params.confidence = prev_conf * confid;
                            params.path_id.edges.emplace_back(e.get_handle());
                            params.path_hash = incremental_bkdr_hash(path_hash, e.get_handle());
                            next.emplace_back(std::make_pair(nbr, params));
                            params.path_id.edges.pop_back(); /* revert to original edges */
                            dp_state.outstanding_count++;
                        }
                    }

                    if (dp_state.outstanding_count == 0) {
                        params0.returning = true;
                        dp_state.get_prev_substate_identifier(params0);
                        next.emplace_back(std::make_pair(params0.prev_node, params0));
                /* fprintf(stderr, "returning to %s\n", params0.prev_node.handle.c_str()); */
                    }
                } else { /* run out of path length */
                    params.returning = true;
                    dp_state.get_prev_substate_identifier(params);
                    assert(params.paths.empty());
                    next.emplace_back(std::make_pair(params.prev_node, params));
                    /* fprintf(stderr, "returning to %s\n", params.prev_node.handle.c_str()); */
                }
            }
        }

    } else {
        // request returning to start node
        /* fprintf(stderr, "back to %s with hash: %u", cur_handle.c_str(), params.path_hash); */
        /*
        for (const auto &t: params.ancestors)
            fprintf(stderr, " %s", t.c_str());
        fprintf(stderr, "\n");
        */
        cause_and_effect_substate *ret = state.get_substate(params);
        assert(ret != nullptr);
        cause_and_effect_substate &dp_state = *ret;
        /* fprintf(stderr, "back to id: %s %.3f\n", cur_handle.c_str(), dp_state.confidence); */
        path_res new_paths;
        auto &spaths = dp_state.paths;
        auto &ppaths = params.paths;
        for (auto siter = spaths.begin(),
                    piter = ppaths.begin();
                    (siter != spaths.end() || piter != ppaths.end())
                        && new_paths.size() < params.max_results;) {
            if (piter == ppaths.end() || (siter != spaths.end() && siter->first > piter->first))
                new_paths.emplace_back(*siter++);
            else
                new_paths.emplace_back(*piter++);
        }

        dp_state.paths = new_paths;

        if (--dp_state.outstanding_count == 0) {

            for (auto &path: dp_state.paths)
                path.second.emplace_back(cur_handle);
            dp_state.get_prev_substate_identifier(params);
            params.paths = dp_state.paths;
            next.emplace_back(std::make_pair(dp_state.prev_node, params));
        }
    }

    return std::make_pair(search_type::BREADTH_FIRST, next);
}
