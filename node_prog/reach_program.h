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

#ifndef __REACH_PROG__
#define __REACH_PROG__

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
            bool mode; // false = request, true = reply
            db::element::remote_node prev_node;
            uint64_t dest_handle;
            std::vector<std::pair<std::string, std::string>> edge_props;
            uint32_t hops;
            bool reachable;
            std::vector<db::element::remote_node> path;

        public:
            reach_params()
                : _search_cache(false)
                , _cache_key(0)
                , mode(false)
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
                    + message::size(mode)
                    + message::size(prev_node)
                    + message::size(dest_handle) 
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
                message::pack_buffer(packer, mode);
                message::pack_buffer(packer, prev_node);
                message::pack_buffer(packer, dest_handle);
                message::pack_buffer(packer, edge_props);
                message::pack_buffer(packer, hops);
                message::pack_buffer(packer, reachable);
                message::pack_buffer(packer, path);
            }

            virtual void unpack(e::unpacker &unpacker)
            {
                message::unpack_buffer(unpacker, _search_cache);
                message::unpack_buffer(unpacker, _cache_key);
                message::unpack_buffer(unpacker, mode);
                message::unpack_buffer(unpacker, prev_node);
                message::unpack_buffer(unpacker, dest_handle);
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

        virtual ~reach_cache_value () { }

        virtual uint64_t size() const 
        {
            return 0;
        }

        virtual void pack(e::buffer::packer&) const 
        {
        }

        virtual void unpack(e::unpacker&)
        {
        }
    };

    inline bool
    check_cache_context(std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>>& context)
    {
        //WDEBUG  << "$$$$$ checking context of size "<< context.size() << std::endl;
        // path not valid if broken by:
        for (std::pair<db::element::remote_node, db::caching::node_cache_context>& pair : context)
        {
            if (pair.second.node_deleted){  // node deletion
                WDEBUG  << "Cache entry invalid because of node deletion" << std::endl;
                return false;
            }
            // edge deletion, currently n^2 check for any edge deletion between two nodes in watch set, could poss be better
            if (!pair.second.edges_deleted.empty()) {
                for(auto edge : pair.second.edges_deleted){
                    for (std::pair<db::element::remote_node, db::caching::node_cache_context>& pair2 : context)
                    {
                        if ((db::element::remote_node) edge.nbr == pair2.first) { // XXX casted unti I fix cache context for public bgraph elems
                            WDEBUG  << "Cache entry invalid because of edge deletion" << std::endl;
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    std::vector<std::pair<db::element::remote_node, reach_params>> 
    reach_node_program(
            node &n,
            db::element::remote_node &rn,
            reach_params &params,
            std::function<reach_node_state&()> state_getter,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
            std::unique_ptr<db::caching::cache_response> cache_response)
    {
        if (MAX_CACHE_ENTRIES)
        {
            if (params._search_cache  && !params.mode && cache_response){
                // check context, update cache
                bool valid = check_cache_context(cache_response->context);
                if (valid) {
                    //WDEBUG  << "valid CACHE RESPONSE node handle: " << rn.handle << std::endl;
                    // we found the node we are looking for, prepare a reply
                    params.mode = true;
                    params.reachable = true;
                    params._search_cache = false; // don't search on way back

                    // context for cached value contains the nodes in the path to the dest_handleination from this node
                    for (auto& context_pair : cache_response->context) { 
                        params.path.emplace_back(context_pair.first);
                    }
                    return {std::make_pair(params.prev_node, params)}; // single length vector
                } else {
                    cache_response->invalidate();
                }
            }
        }

        reach_node_state &state = state_getter();
        std::vector<std::pair<db::element::remote_node, reach_params>> next;
        bool false_reply = false;
        db::element::remote_node prev_node = params.prev_node;
        params.prev_node = rn;
        if (!params.mode) { // request mode
            if (params.dest_handle == rn.get_id()) {
                // we found the node we are looking for, prepare a reply
                params.mode = true;
                params.reachable = true;
                params.path.emplace_back(rn);
                next.emplace_back(std::make_pair(prev_node, params));
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
                            state.out_count++;
                        }
                    }
                    if (state.out_count == 0) {
                        false_reply = true;
                    }
                }
            }
            if (false_reply) {
                params.mode = true;
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
                        std::shared_ptr<node_prog::reach_cache_value> toCache(new reach_cache_value());
                        std::shared_ptr<std::vector<db::element::remote_node>> watch_set(new std::vector<db::element::remote_node>(params.path)); // copy return path from params
                        add_cache_func(toCache, watch_set, params.dest_handle);
                    }
                }
                next.emplace_back(std::make_pair(state.prev_node, params));
            }
            if ((int)state.out_count < 0) {
                WDEBUG << "ALERT! Bad state value in reach program" << std::endl;
                next.clear();
                while(1);
            }
        }
        return next;
    }
}

#endif
