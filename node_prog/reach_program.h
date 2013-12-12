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

#include "common/weaver_constants.h"
#include "db/element/node.h"
#include "db/element/remote_node.h"
#include "common/message.h"
#include "common/vclock.h"
#include "common/event_order.h"

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
            { }
            
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

/*
    struct reach_cache_value : CacheValueBase 
    {
        uint64_t reachable_node;
        virtual ~reach_cache_value() { }
    };
    */

    std::vector<std::pair<db::element::remote_node, reach_params>> 
    reach_node_program(uint64_t, // TODO used to be req_id, now replaced by vclock
            db::element::node &n,
            db::element::remote_node &rn,
            reach_params &params,
            std::function<reach_node_state&()> state_getter,
            vc::vclock &req_vclock)
    {
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
                bool got_cache = false; // TODO
                if (!state.visited && !got_cache) {
                    db::element::edge *e;
                    state.prev_node = prev_node;
                    state.visited = true;
                    for (auto &iter: n.out_edges) {
                        e = iter.second;
                        // TODO change this so that the user does not see invalid edges
                        // check edge created and deleted in acceptable timeframe
                        int64_t cmp_1 = order::compare_two_vts(e->get_creat_time(), req_vclock);
                        assert(cmp_1 != 2);
                        bool traverse_edge = (cmp_1 == 0);
                        if (traverse_edge) {
                            int64_t cmp_2 = order::compare_two_vts(e->get_del_time(), req_vclock);
                            assert(cmp_2 != 2);
                            traverse_edge = (cmp_2 == 1);
                        }
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
}

#endif
