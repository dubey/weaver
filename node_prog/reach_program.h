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

#ifndef weaver_node_prog_reach_program_h_
#define weaver_node_prog_reach_program_h_

#include <vector>

#include "node_prog/base_classes.h"
#include "common/weaver_constants.h"
//#include "db/cache/prog_cache.h"
#include "common/message.h"
#include "common/vclock.h"
#include "common/event_order.h"
#include "db/element/edge.h"
#include "node.h"
#include "edge.h"
#include "db/element/remote_node.h"

namespace node_prog
{
    class reach_params : public virtual Node_Parameters_Base  
    {
        public:
            bool _search_cache;
            uint64_t _cache_key;
            bool returning; // false = request, true = reply
            db::element::remote_node prev_node;
            uint64_t dest;
            std::vector<std::pair<std::string, std::string>> edge_props;
            uint32_t hops;
            bool reachable;
            std::vector<db::element::remote_node> path;

        public:
            reach_params()
                : _search_cache(false)
                , _cache_key(0)
                , returning(false)
                , hops(0)
                , reachable(false)
            { }
            
            virtual ~reach_params() { }

            virtual bool search_cache() {
                return _search_cache;
            }

            virtual uint64_t cache_key() {
                return _cache_key;
            }

            virtual uint64_t size() const 
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

            virtual void pack(e::buffer::packer &packer) const 
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

            virtual void unpack(e::unpacker &unpacker)
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
    };

    struct reach_node_state : public virtual Node_State_Base 
    {
        bool visited;
        db::element::remote_node prev_node; // previous node
        uint32_t out_count; // number of requests propagated
        bool reachable;
        uint64_t hops;

        reach_node_state()
            : visited(false)
            , out_count(0)
            , reachable(false)
            , hops(MAX_UINT64)
        { }

        virtual ~reach_node_state() { }

        virtual uint64_t size() const 
        {
            uint64_t toRet = message::size(visited)
                + message::size(prev_node)
                + message::size(out_count)
                + message::size(reachable)
                + message::size(hops);
            return toRet;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, visited);
            message::pack_buffer(packer, prev_node);
            message::pack_buffer(packer, out_count);
            message::pack_buffer(packer, reachable);
            message::pack_buffer(packer, hops);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, visited);
            message::unpack_buffer(unpacker, prev_node);
            message::unpack_buffer(unpacker, out_count);
            message::unpack_buffer(unpacker, reachable);
            message::unpack_buffer(unpacker, hops);
        }
    };

    struct reach_cache_value : public virtual Cache_Value_Base 
    {
        public:
            std::vector<db::element::remote_node> path;

            reach_cache_value(std::vector<db::element::remote_node> &copy_path)
                : path(copy_path) { };
        virtual ~reach_cache_value () { }

        virtual uint64_t size() const 
        {
            return message::size(path);
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, path);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, path);
        }
    };

    inline bool
    check_cache_context(cache_response<reach_cache_value> &cr)
    {
        std::vector<node_cache_context>& contexts = cr.get_context();
        if (contexts.size() == 0) {
            return true;
        }
        reach_cache_value &cv = *cr.get_value();
        // path not valid if broken by:
        for (node_cache_context& node_context : contexts)
        {
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

    inline std::vector<std::pair<db::element::remote_node, reach_params>> 
    reach_node_program(
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
        bool false_reply = false;
        db::element::remote_node prev_node = params.prev_node;
        params.prev_node = rn;
        if (!params.returning) { // request mode
            if (params.dest == rn.get_id()) {
                // we found the node we are looking for, prepare a reply
                params.returning = true;
                params.reachable = true;
                params.path.emplace_back(rn);
                return {std::make_pair(prev_node, params)};
            } else {
                // have not found it yet so follow all out edges
                if (!state.visited) {
                    state.prev_node = prev_node;
                    state.visited = true;

                    if (MAX_CACHE_ENTRIES)
                    {
                        if (params._search_cache /*&& !params.returning */ && cache_response != NULL){
                            // check context, update cache
                            bool valid = check_cache_context(*cache_response);
                            if (valid) {
                                WDEBUG  << "Cache worked at node " << rn.id << std::endl;
                                // we found the node we are looking for, prepare a reply
                                params.returning = true;
                                params.reachable = true;
                                params._search_cache = false; // don't search on way back

                                // context for cached value contains the nodes in the path to the dest_idination from this node
                                params.path = std::dynamic_pointer_cast<reach_cache_value>(cache_response->get_value())->path; // XXX double check this path
                                /*
                                for (auto& node_context : cache_response->get_context()) { 
                                    params.path.emplace_back(node_context.node); // XXX THEse can be shuffled, change to storing this in cache
                                }
                                */
                                return {std::make_pair(prev_node, params)}; // single length vector
                            } else {
                                cache_response->invalidate();
                            }
                        }
                    }

                    for (edge &e: n.get_edges()) {
                        // checking edge properties
                        if (e.has_all_properties(params.edge_props)) {
                            e.traverse();
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
                    if (MAX_CACHE_ENTRIES)
                    {
                        // now add to cache
                        WDEBUG << "adding to cache on way back from dest on node " << rn.id << std::endl;
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
        }
        return next;
    }
}

#endif
