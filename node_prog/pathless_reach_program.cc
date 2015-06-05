/*
 * ===============================================================
 *    Description:  Reachability program.
 *
 *        Created:  Sunday 23 April 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#include "common/stl_serialization.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/edge.h"
#include "node_prog/pathless_reach_program.h"

using node_prog::search_type;
using node_prog::pathless_reach_params;
using node_prog::pathless_reach_node_state;
using node_prog::node_cache_context;
using node_prog::cache_response;

// params
pathless_reach_params :: pathless_reach_params()
    : returning(false)
    , reachable(false)
{ }

bool
pathless_reach_params :: search_cache()
{
    return false;
}

uint64_t
pathless_reach_params :: size() const 
{
    uint64_t toRet = message::size(returning)
        + message::size(prev_node)
        + message::size(dest) 
        + message::size(edge_props)
        + message::size(reachable);
    return toRet;
}

void
pathless_reach_params :: pack(e::packer &packer) const 
{
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, dest);
    message::pack_buffer(packer, edge_props);
    message::pack_buffer(packer, reachable);
}

void
pathless_reach_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, dest);
    message::unpack_buffer(unpacker, edge_props);
    message::unpack_buffer(unpacker, reachable);
}

// state
pathless_reach_node_state :: pathless_reach_node_state()
: visited(false)
, out_count(0)
, reachable(false)
{ }

uint64_t
pathless_reach_node_state :: size() const 
{
    uint64_t toRet = message::size(visited)
        + message::size(prev_node)
        + message::size(out_count)
        + message::size(reachable);
    return toRet;
}

void
pathless_reach_node_state :: pack(e::packer& packer) const 
{
    message::pack_buffer(packer, visited);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, out_count);
    message::pack_buffer(packer, reachable);
}

void
pathless_reach_node_state :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, visited);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, out_count);
    message::unpack_buffer(unpacker, reachable);
}


// node prog code

std::pair<search_type, std::vector<std::pair<db::remote_node, pathless_reach_params>>>
node_prog :: pathless_reach_node_program(
        node &n,
        db::remote_node &rn,
        pathless_reach_params &params,
        std::function<pathless_reach_node_state&()> state_getter,
        std::function<void(std::shared_ptr<Cache_Value_Base>,
            std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
        cache_response<Cache_Value_Base>*)
{
    pathless_reach_node_state &state = state_getter();
    std::vector<std::pair<db::remote_node, pathless_reach_params>> next;
    if (state.reachable == true) {
        return std::make_pair(search_type::BREADTH_FIRST, next);
    }
    bool false_reply = false;
    db::remote_node prev_node = params.prev_node;
    params.prev_node = rn;
    if (!params.returning) { // request mode
        if (params.dest == n.get_handle()) {
            // we found the node we are looking for, prepare a reply
            params.reachable = true;
            //    WDEBUG  << "found dest!" << std::endl;
            next.emplace_back(std::make_pair(db::coordinator, params));
            return std::make_pair(search_type::DEPTH_FIRST, next);
        } else {
            // have not found it yet so follow all out edges
            if (!state.visited) {
                state.prev_node = prev_node;
                state.visited = true;

                for (edge &e: n.get_edges()) {
                    // checking edge properties
                    if (e.has_all_properties(params.edge_props)) {
                        // e->traverse(); no more traversal recording

                        // propagate reachability request
                        next.emplace_back(std::make_pair(e.get_neighbor(), params));
                        //WDEBUG  << "emplacing " << e.get_neighbor().id << " to next" << std::endl;
                        state.out_count++;
                    }
                }
                if (state.out_count == 0) {
                    false_reply = true;
                }
            } else {
                false_reply = true;
            }
        }
        if (false_reply) {
            params.returning = true;
            params.reachable = false;
            next.emplace_back(std::make_pair(prev_node, params));
        }
        return std::make_pair(search_type::BREADTH_FIRST, next);
    } else { // reply mode
        if (((--state.out_count == 0) || params.reachable) && !state.reachable) {
            state.reachable |= params.reachable;
            next.emplace_back(std::make_pair(state.prev_node, params));
        }
        if ((int)state.out_count < 0) {
            WDEBUG << "ALERT! Bad state value in reach program" << std::endl;
            next.clear();
        }
        if (params.reachable) {
            return std::make_pair(search_type::DEPTH_FIRST, next);
        } else {
            return std::make_pair(search_type::BREADTH_FIRST, next);
        }
    }
}
