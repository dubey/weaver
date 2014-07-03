/*
 * ===============================================================
 *    Description:  Reachability program implementation..
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

#include "common/message.h"
#include "common/cache_constants.h"
#include "node_prog/edge.h"
#include "node_prog/reach_program.h"

using node_prog::search_type;
using node_prog::reach_params;
using node_prog::reach_node_state;
using node_prog::reach_cache_value;
using node_prog::node_cache_context;
using node_prog::cache_response;

// params
reach_params :: reach_params()
    : _search_cache(false)
    , _cache_key(0)
    , returning(false)
    , hops(0)
    , reachable(false)
{ }

uint64_t
reach_params :: size() const 
{
    uint64_t toRet = message::size(_search_cache)
        + message::size(_cache_key)
        + message::size(returning)
        + message::size(prev_node)
        + message::size(dest) 
        + message::size(edge_props)
        + message::size(hops)
        + message::size(reachable)
        + message::size(path);
    return toRet;
}

void
reach_params :: pack(e::buffer::packer &packer) const 
{
    message::pack_buffer(packer, _search_cache);
    message::pack_buffer(packer, _cache_key);
    message::pack_buffer(packer, returning);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, dest);
    message::pack_buffer(packer, edge_props);
    message::pack_buffer(packer, hops);
    message::pack_buffer(packer, reachable);
    message::pack_buffer(packer, path);
}

void
reach_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, _search_cache);
    message::unpack_buffer(unpacker, _cache_key);
    message::unpack_buffer(unpacker, returning);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, dest);
    message::unpack_buffer(unpacker, edge_props);
    message::unpack_buffer(unpacker, hops);
    message::unpack_buffer(unpacker, reachable);
    message::unpack_buffer(unpacker, path);
}

// state
reach_node_state :: reach_node_state()
    : visited(false)
    , out_count(0)
    , reachable(false)
    , hops(UINT16_MAX)
{ }

uint64_t
reach_node_state :: size() const 
{
    uint64_t toRet = message::size(visited)
        + message::size(prev_node)
        + message::size(out_count)
        + message::size(reachable)
        + message::size(hops);
    return toRet;
}

void
reach_node_state :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, visited);
    message::pack_buffer(packer, prev_node);
    message::pack_buffer(packer, out_count);
    message::pack_buffer(packer, reachable);
    message::pack_buffer(packer, hops);
}

void
reach_node_state :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, visited);
    message::unpack_buffer(unpacker, prev_node);
    message::unpack_buffer(unpacker, out_count);
    message::unpack_buffer(unpacker, reachable);
    message::unpack_buffer(unpacker, hops);
}

// cache
reach_cache_value :: reach_cache_value(std::vector<db::element::remote_node> &cpy)
    : path(cpy) { }

uint64_t
reach_cache_value :: size() const 
{
    return message::size(path);
}

void
reach_cache_value :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, path);
}

void
reach_cache_value :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, path);
}


// node prog code

bool
check_cache_context(cache_response<reach_cache_value> &cr)
{
    std::vector<node_cache_context>& contexts = cr.get_context();
    if (contexts.size() == 0) {
        return true;
    }
    reach_cache_value &cv = *cr.get_value();
    // path not valid if broken by:
    for (node_cache_context& node_context : contexts) {
        if (node_context.node_deleted){  // node deletion
            WDEBUG  << "Cache entry invalid because of node deletion" << std::endl;
            return false;
        }
        // edge deletion, see if path was broken
        for (size_t i = 1; i < cv.path.size(); i++) {
            if (node_context.node == cv.path.at(i)) {
                db::element::remote_node &path_next_node = cv.path.at(i-1);
                for(auto &edge : node_context.edges_deleted){
                    if (edge.nbr == path_next_node) {
                        WDEBUG  << "Cache entry invalid because of edge deletion" << std::endl;
                        return false;
                    }
                }
                break; // path not broken here, move on
            }
        }
    }
    WDEBUG  << "Cache entry with context size " << contexts.size() << " valid" << std::endl;
    return true;
}

std::pair<search_type, std::vector<std::pair<db::element::remote_node, reach_params>>>
node_prog :: reach_node_program(
        node &n,
        db::element::remote_node &rn,
        reach_params &params,
        std::function<reach_node_state&()> state_getter,
        std::function<void(std::shared_ptr<reach_cache_value>, // TODO make const
            std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
        cache_response<reach_cache_value>*cache_response)
{
    reach_node_state &state = state_getter();
    std::vector<std::pair<db::element::remote_node, reach_params>> next;
    if (state.reachable == true) {
        return std::make_pair(search_type::BREADTH_FIRST, next);
    }
    bool false_reply = false;
    db::element::remote_node prev_node = params.prev_node;
    params.prev_node = rn;
    if (!params.returning) { // request mode
        if (params.dest == rn.get_id()) {
            // we found the node we are looking for, prepare a reply
            params.returning = true;
            params.reachable = true;
            params.path.emplace_back(rn);
            params._search_cache = false; // never search on way back
            next.emplace_back(std::make_pair(prev_node, params));
            return std::make_pair(search_type::DEPTH_FIRST, next);
        } else {
            // have not found it yet so follow all out edges
            if (!state.visited) {
                state.prev_node = prev_node;
                state.visited = true;

                if (MaxCacheEntries) {
                    if (params._search_cache  && cache_response != NULL) {
                        // check context, update cache
                        if (check_cache_context(*cache_response)) { // if context is valid
                            // we found the node we are looking for, prepare a reply
                            params.returning = true;
                            params.reachable = true;
                            params._search_cache = false; // don't search on way back

                            // context for cached value contains the nodes in the path to the dest_idination from this node
                            params.path = std::dynamic_pointer_cast<reach_cache_value>(cache_response->get_value())->path; // XXX double check this path
                            assert(params.path.size() > 0);
                            next.emplace_back(std::make_pair(prev_node, params));
                            return std::make_pair(search_type::DEPTH_FIRST, next); // single length vector
                        } else {
                            cache_response->invalidate();
                        }
                    }
                }

                for (edge &e: n.get_edges()) {
                    // checking edge properties
                    if (e.has_all_properties(params.edge_props)) {
                        // propagate reachability request
                        next.emplace_back(std::make_pair(e.get_neighbor(), params));
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
        if (params.reachable) {
            if (state.hops > params.hops) {
                state.hops = params.hops;
            }
        }
        if (((--state.out_count == 0) || params.reachable) && !state.reachable) {
            state.reachable |= params.reachable;
            if (params.reachable) {
                params.hops = state.hops + 1;
                params.path.emplace_back(rn);
                if (MaxCacheEntries)
                {
                    // now add to cache
                    std::shared_ptr<node_prog::reach_cache_value> toCache(new reach_cache_value(params.path));
                    std::shared_ptr<std::vector<db::element::remote_node>> watch_set(new std::vector<db::element::remote_node>(params.path)); // copy return path from params
                    add_cache_func(toCache, watch_set, params.dest);
                }
            }
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
