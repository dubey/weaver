/*
 * ===============================================================
 *    Description:  Clustering program implementation.
 *
 *        Created:  2014-05-30 11:48:02
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "node_prog/edge.h"
#include "node_prog/clustering_program.h"

using node_prog::search_type;
using node_prog::clustering_params;
using node_prog::clustering_node_state;
using node_prog::node_cache_context;
using node_prog::cache_response;

// params
uint64_t
clustering_params :: size() const 
{
    uint64_t toRet = message::size(_search_cache)
        + message::size(_cache_key)
        + message::size(is_center)
        + message::size(center)
        + message::size(outgoing)
        + message::size(neighbors)
        + message::size(clustering_coeff);
    return toRet;
}

void
clustering_params :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, _search_cache);
    message::pack_buffer(packer, _cache_key);
    message::pack_buffer(packer, is_center);
    message::pack_buffer(packer, center);
    message::pack_buffer(packer, outgoing);
    message::pack_buffer(packer, neighbors);
    message::pack_buffer(packer, clustering_coeff);
}

void
clustering_params :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, _search_cache);
    message::unpack_buffer(unpacker, _cache_key);
    message::unpack_buffer(unpacker, is_center);
    message::unpack_buffer(unpacker, center);
    message::unpack_buffer(unpacker, outgoing);
    message::unpack_buffer(unpacker, neighbors);
    message::unpack_buffer(unpacker, clustering_coeff);
}

// state
uint64_t
clustering_node_state :: size() const
{
    return message::size(neighbor_counts)
        + message::size(responses_left);
}

void
clustering_node_state :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, neighbor_counts);
    message::pack_buffer(packer, responses_left);
}

void
clustering_node_state :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, neighbor_counts);
    message::unpack_buffer(unpacker, responses_left);
}

// node prog code
std::pair<search_type, std::vector<std::pair<db::element::remote_node, clustering_params>>>
node_prog :: clustering_node_program(
    node &n,
    db::element::remote_node &rn,
    clustering_params &params,
    std::function<clustering_node_state&()> get_state,
    std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
        std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
    cache_response<Cache_Value_Base>*)
{
    // TODO can we change this to a three enum switch to reduce number of if statements
    std::vector<std::pair<db::element::remote_node, clustering_params>> next;
    if (params.is_center) {
        node_prog::clustering_node_state &cstate = get_state();
        if (params.outgoing) {
            params.is_center = false;
            params.center = rn;
            for (edge& edge : n.get_edges()) {
                edge.traverse();
                next.emplace_back(std::make_pair(edge.get_neighbor(), params));
                cstate.neighbor_counts.insert(std::make_pair(edge.get_neighbor().get_id(), 0));
                cstate.responses_left++;
            }
            if (cstate.responses_left < 2) { // if no or one neighbor we know clustering coeff already
                params.clustering_coeff = 0;
                next = {std::make_pair(db::element::coordinator, std::move(params))};
            }
        } else {
            for (uint64_t &nbr_id : params.neighbors) {
                if (cstate.neighbor_counts.count(nbr_id) > 0) {
                    cstate.neighbor_counts[nbr_id]++;
                }
            }
            if (--cstate.responses_left == 0) {
                assert(cstate.neighbor_counts.size() > 1);
                double denominator = (double) (cstate.neighbor_counts.size() * (cstate.neighbor_counts.size() - 1));
                uint64_t numerator = 0;
                for (std::pair<const uint64_t, int>& nbr_count : cstate.neighbor_counts){
                    numerator += nbr_count.second;
                }
                params.clustering_coeff = (double) numerator / denominator;
                next = {std::make_pair(db::element::coordinator, std::move(params))};
            }
        }
    } else { // not center
        for (edge& edge : n.get_edges()) {
            params.neighbors.push_back(edge.get_neighbor().get_id());
        }
        params.outgoing = false;
        params.is_center = true;
        next = {std::make_pair(params.center, std::move(params))};
    }
    return std::make_pair(search_type::BREADTH_FIRST, next);
}

