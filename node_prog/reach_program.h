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

#include "db/element/node.h"
#include "db/element/remote_node.h"
#include "common/message.h"

namespace node_prog
{
    class reach_params : public virtual Packable 
    {
        public:
            bool mode; // false = request, true = reply
            db::element::remote_node prev_node;
            uint64_t dest;
            std::vector<common::property> edge_props;
            uint32_t hops;
            bool reachable;
            std::vector<db::element::remote_node> path;

        public:
            reach_params()
                : mode(false)
                , hops(0)
                , reachable(false)
            {
            }
            
            virtual ~reach_params() { }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(mode)
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
                message::unpack_buffer(unpacker, mode);
                message::unpack_buffer(unpacker, prev_node);
                message::unpack_buffer(unpacker, dest);
                message::unpack_buffer(unpacker, edge_props);
                message::unpack_buffer(unpacker, hops);
                message::unpack_buffer(unpacker, reachable);
                message::unpack_buffer(unpacker, path);
            }
    };

    struct reach_node_state : Packable_Deletable
    {
        bool visited;
        db::element::remote_node prev_node; // previous node
        uint32_t out_count; // number of requests propagated
        bool reachable;

        reach_node_state()
            : visited(false)
            , out_count(0)
            , reachable(false)
        { }

        virtual ~reach_node_state() { }

        virtual uint64_t size() const 
        {
            uint64_t toRet = message::size(visited)
                + message::size(prev_node)
                + message::size(out_count)
                + message::size(reachable);
            return toRet;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, visited);
            message::pack_buffer(packer, prev_node);
            message::pack_buffer(packer, out_count);
            message::pack_buffer(packer, reachable);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, visited);
            message::unpack_buffer(unpacker, prev_node);
            message::unpack_buffer(unpacker, out_count);
            message::unpack_buffer(unpacker, reachable);
        }
    };

    struct reach_cache_value : CacheValueBase 
    {
        uint64_t reachable_node;
        virtual ~reach_cache_value() { }
    };

    std::vector<std::pair<db::element::remote_node, reach_params>> 
    reach_node_program(uint64_t req_id,
            db::element::node &n,
            db::element::remote_node &rn,
            reach_params &params,
            std::function<reach_node_state&()> state_getter,
            std::function<reach_cache_value&()> cache_putter,
            std::function<std::vector<std::shared_ptr<reach_cache_value>>()> cached_values_getter)
    {
        UNUSED(cache_putter);
        reach_node_state &state = state_getter();
        bool false_reply = false;
        db::element::remote_node prev_node = params.prev_node;
        params.prev_node = rn;
        std::vector<std::pair<db::element::remote_node, reach_params>> next;
        if (!params.mode) { // request mode
            if (params.dest == rn.handle) {
                // we found the node we are looking for, prepare a reply
                params.mode = true;
                params.reachable = true;
                next.emplace_back(std::make_pair(prev_node, params));
            } else {
                // have not found it yet, check the cache, then follow all out edges
                bool got_cache = false;
                std::vector<std::shared_ptr<reach_cache_value>> cache = cached_values_getter();
                for (auto &rcv: cache) {
                    if (rcv->reachable_node == params.dest) {
                        rcv->mark();
                        params.mode = true;
                        params.reachable = true;
                        next.emplace_back(std::make_pair(prev_node, params));
                        got_cache = true;
                        break;
                    }
                }
                if (!state.visited && !got_cache) {
                    db::element::edge *e;
                    state.prev_node = prev_node;
                    state.visited = true;
                    for (auto &iter: n.out_edges) {
                        e = iter.second;
                        // TODO change this so that the user does not see invalid edges
                        bool traverse_edge = e->get_creat_time() <= req_id
                            && e->get_del_time() > req_id; // edge created and deleted in acceptable timeframe
                        // checking edge properties
                        for (auto &prop: params.edge_props) {
                            if (!e->has_property(prop, req_id)) {
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
            if (((--state.out_count == 0) || params.reachable) && !state.reachable) {
                state.reachable |= params.reachable;
                db::element::edge *e;
                for (auto e_iter: n.in_edges) {
                    e = e_iter.second;
                    if (e->nbr == state.prev_node) {
                        e->traverse();
                        break;
                    }
                }
                if (params.reachable) {
                    params.hops++;
                    params.path.emplace_back(rn);
                }
                next.emplace_back(std::make_pair(state.prev_node, params));
                // next block of code enables caching
                if (params.reachable) {
                    // adding to cache
                    reach_cache_value &rcv = cache_putter();
                    rcv.reachable_node = params.dest;
                }
            }
            if ((int)state.out_count < 0) {
                DEBUG << "ALERT! Bad state value in reach program for node " << rn.handle
                        << " at loc " << rn.loc << std::endl;
                next.clear();
                //while(1);
            }
        }
        return next;
    }

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
}

#endif //__DIKJSTRA_PROG__
