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
#include "db/element/node.h"
#include "db/element/remote_node.h"
#include "db/cache/prog_cache.h"
#include "common/message.h"
#include "common/vclock.h"
#include "common/event_order.h"

namespace node_prog
{
    class reach_params : public virtual Node_Parameters_Base  
    {
        public:
            bool _search_cache;
            uint64_t _cache_key;
            bool mode; // false = request, true = reply
            db::element::remote_node prev_node;
            uint64_t dest;
            std::vector<common::property> edge_props;
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
                message::pack_buffer(packer, mode);
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
                message::unpack_buffer(unpacker, mode);
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
            //std::vector<db::element::remote_node> path;

        virtual ~reach_cache_value () { }

        virtual uint64_t size() const 
        {
            /*
            uint64_t toRet = message::size(path);
            return toRet;
            */
            return 0;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            //message::pack_buffer(packer, path);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            //message::unpack_buffer(unpacker, path);
        }
    };

    inline bool
    check_context(std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>>& context)
    {
        //WDEBUG  << "$$$$$ checking context of size "<< context.size() << std::endl;
        // path not valid if broken by:
        for (auto& pair : context)
        {
            if (pair.second.node_deleted){  // node deletion
                WDEBUG  << "Cache entry invalid because of node deletion" << std::endl;
                return false;
            }
            // edge deletion, currently n^2 check for any edge deletion between two nodes in watch set, could poss be better
            if (!pair.second.edges_deleted.empty()) {
                for(auto edge :  pair.second.edges_deleted){
                    for (auto& pair2 : context)
                    {
                        if (edge.nbr.handle == pair2.first.handle){
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
    reach_node_program(uint64_t, // TODO used to be req_id, now replaced by vclock
            db::element::node &n,
            db::element::remote_node &rn,
            reach_params &params,
            std::function<reach_node_state&()> state_getter,
            std::shared_ptr<vc::vclock> &req_vclock,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
            std::unique_ptr<db::caching::cache_response> cache_response)
    {
        std::vector<std::pair<db::element::remote_node, reach_params>> next;
        if (MAX_CACHE_ENTRIES)
        {
        if (params._search_cache && cache_response != NULL){
            // check context, update cache
            bool valid = check_context(cache_response->context);
            if (valid) {
                //WDEBUG  << "WEEE GOT A valid CACHE RESPONSE, short circuit" << std::endl;
                params.mode = true;
                params.reachable = true;
                next.emplace_back(std::make_pair(params.prev_node, params));
                return next;
            }
            cache_response->invalidate();
        }
        params._search_cache = false; // only search cache for first of req
        }

        reach_node_state &state = state_getter();
        bool false_reply = false;
        db::element::remote_node prev_node = params.prev_node;
        params.prev_node = rn;
        if (!params.mode) { // request mode
            if (params.dest == rn.handle) {
                // we found the node we are looking for, prepare a reply
                params.mode = true;
                params.reachable = true;
                params.path.emplace_back(rn);
                next.emplace_back(std::make_pair(prev_node, params));
            } else {
                // have not found it yet, check the cache, then follow all out edges
                bool got_cache = false; // TODO
                if (!state.visited && !got_cache) {
                    db::element::edge *e;
                    state.prev_node = prev_node;
                    state.visited = true;
                    for (auto &iter: n.out_edges) {
                        e = iter.second;
                        // TODO change this so that the user does not see invalid edges
                        // check edge created and deleted in acceptable timeframe
                        bool traverse_edge = order::clock_creat_before_del_after(*req_vclock, e->get_creat_time(), e->get_del_time());

                        // checking edge properties
                        for (auto &prop: params.edge_props) {
                            if (!e->has_property(prop, *req_vclock)) {
                                traverse_edge = false;
                                break;
                            }
                        }

                        if (traverse_edge) {
                            e->traverse();
                            next.emplace_back(std::make_pair(e->nbr, params)); // propagate reachability request
                            state.out_count++;
                        }
                    }
                    if (state.out_count == 0) {
                        false_reply = true;
                    }
                } else if (!got_cache) {
                    false_reply = true;
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
                    std::shared_ptr<node_prog::reach_cache_value > toCache(new reach_cache_value());
                    std::shared_ptr<std::vector<db::element::remote_node>> watch_set(new std::vector<db::element::remote_node>(params.path)); // copy return path from params
                    uint64_t cache_key = params.dest;
                    add_cache_func(toCache, watch_set, cache_key);
                    }
                }
                next.emplace_back(std::make_pair(state.prev_node, params));
            }
            if ((int)state.out_count < 0) {
                WDEBUG << "ALERT! Bad state value in reach program for node " << rn.handle
                        << " at loc " << rn.loc << std::endl;
                next.clear();
                while(1);
            }
        }
        return next;
    }

    /*
    std::vector<std::pair<db::element::remote_node, reach_params>> 
    reach_node_deleted_program(uint64_t req_id,
                db::element::node &n, // node who asked to go to deleted node
                uint64_t deleted_handle, // handle of node that didn't exist
            reach_params &params_given, // params we had sent to deleted node
            std::function<reach_node_state&()> state_getter)
    {
        UNUSED(req_id);
        UNUSED(n);
        UNUSED(deleted_handle);
        UNUSED(params_given);
        UNUSED(state_getter);
        return std::vector<std::pair<db::element::remote_node, reach_params>>(); 
    }
    */
}

#endif
