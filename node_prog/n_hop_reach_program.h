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

#ifndef __N_HOP_REACH_PROG__
#define __N_HOP_REACH_PROG__

#include <vector>

#include "common/weaver_constants.h"
#include "db/element/node.h"
#include "db/element/remote_node.h"
#include "common/message.h"
#include "common/vclock.h"
#include "common/event_order.h"

namespace node_prog
{
    class n_hop_reach_params : public virtual Packable 
    {
        public:
            bool returning; // false = request, true = reply
            bool reachable;
            db::element::remote_node prev_node;
            uint64_t dest;
            uint32_t hops;
            uint32_t max_hops;
            std::vector<db::element::remote_node> path;

        public:
            n_hop_reach_params()
                : returning(false)
                , reachable(false)
                , hops(0)
                , max_hops(0)
            {
            }
            
            virtual ~n_hop_reach_params() { }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(returning)
                    + message::size(prev_node)
                    + message::size(dest) 
                    //+ message::size(edge_props)
                    + message::size(hops)
                    + message::size(max_hops)
                    + message::size(reachable)
                    + message::size(path);
                return toRet;
            }

            virtual void pack(e::buffer::packer &packer) const 
            {
                message::pack_buffer(packer, returning);
                message::pack_buffer(packer, prev_node);
                message::pack_buffer(packer, dest);
                //message::pack_buffer(packer, edge_props);
                message::pack_buffer(packer, hops);
                message::pack_buffer(packer, max_hops);
                message::pack_buffer(packer, reachable);
                message::pack_buffer(packer, path);
            }

            virtual void unpack(e::unpacker &unpacker)
            {
                message::unpack_buffer(unpacker, returning);
                message::unpack_buffer(unpacker, prev_node);
                message::unpack_buffer(unpacker, dest);
                //message::unpack_buffer(unpacker, edge_props);
                message::unpack_buffer(unpacker, hops);
                message::unpack_buffer(unpacker, max_hops);
                message::unpack_buffer(unpacker, reachable);
                message::unpack_buffer(unpacker, path);
            }
    };

    struct n_hop_reach_node_state : Packable_Deletable
    {
        //bool visited;
        uint64_t already_visited_hops; // number of hops for a traversal that has alraedy visited this node
        db::element::remote_node prev_node; // previous node
        uint32_t out_count; // number of requests propagated
        bool reachable;
        uint64_t hops;

        n_hop_reach_node_state()
            : already_visited_hops(0)
            , out_count(0)
            , reachable(false)
            , hops(MAX_UINT64)
        { }

        virtual ~n_hop_reach_node_state() { }

        virtual uint64_t size() const 
        {
            uint64_t toRet = message::size(already_visited_hops)
                + message::size(prev_node)
                + message::size(out_count)
                + message::size(reachable)
                + message::size(hops);
            return toRet;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, already_visited_hops);
            message::pack_buffer(packer, prev_node);
            message::pack_buffer(packer, out_count);
            message::pack_buffer(packer, reachable);
            message::pack_buffer(packer, hops);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, already_visited_hops);
            message::unpack_buffer(unpacker, prev_node);
            message::unpack_buffer(unpacker, out_count);
            message::unpack_buffer(unpacker, reachable);
            message::unpack_buffer(unpacker, hops);
        }
    };

    std::vector<std::pair<db::element::remote_node, n_hop_reach_params>> 
    n_hop_reach_node_program(uint64_t, // TODO used to be req_id, now replaced by vclock
            db::element::node &n,
            db::element::remote_node &rn,
            n_hop_reach_params &params,
            std::function<n_hop_reach_node_state&()> state_getter,
            vc::vclock &req_vclock)
    {
        DEBUG << "inside node prog!\n";
        n_hop_reach_node_state &state = state_getter();
        DEBUG << "got state\n";
        bool false_reply = false;
        db::element::remote_node prev_node = params.prev_node;
        //db::element::remote_node *next_node = NULL;
        params.prev_node = rn;
        std::vector<std::pair<db::element::remote_node, n_hop_reach_params>> next;
        if (!params.returning) { // request mode
            DEBUG << "outgoing mode\n";
            if (params.dest == rn.handle) {
                DEBUG << "found dest node\n";
                // we found the node we are looking for, prepare a reply
                params.returning = true;
                params.reachable = true;
        //        if (prev_node.loc < NUM_VTS) { // sending back to vt
        // XXX THIS BREAKS MIGRATION
         //           next_node = &prev_node;
                    DEBUG << "returning to vt\n";
                    /*
                } else {
                    DEBUG << "checking return edges\n";
                    for (auto &x: n.in_edges) { // return back on path taken to get to dest
                        DEBUG << "edge\n";
                        if (x.second->nbr.handle == prev_node.handle) {
                            next_node = &x.second->nbr;
                            x.second->traverse();
                            break;
                        }
                    }
                    DEBUG << "done checking return edges\n";
                }
                */
                next.emplace_back(std::make_pair(prev_node, params));
            } else {
                false_reply = params.hops == params.max_hops;
                DEBUG << params.max_hops - params.hops << " hops left\n";
                params.hops++;
                // have not found it yet or previously found it with longer path and don't not maxed out hops yet
                if (!false_reply && (state.already_visited_hops == 0 || (state.already_visited_hops > params.hops))) {
                    db::element::edge *e;
                    state.prev_node = prev_node;
                    state.already_visited_hops = params.hops;
                    for (auto &iter: n.out_edges) {
                        e = iter.second;
                        if (params.hops == 0 && e->nbr.handle != params.dest) { // this is the last hop, ignore all but those that could be dest
                            continue;
                        }
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
                        /*
                        // checking edge properties
                        for (auto &prop: params.edge_props) {
                            if (!e->has_property(prop, req_id)) {
                                traverse_edge = false;
                                break;
                            }
                        }
                        */
                        if (traverse_edge) {
                            e->traverse();
                            next.emplace_back(std::make_pair(e->nbr, params)); // propagate reachability request
                            state.out_count++;
                        }
                    }
                    DEBUG << state.out_count << " nodes next\n";
                    if (state.out_count == 0) {  // return false reply if there are no more nodes to traverse
                        DEBUG << "no more nodes to traverse\n";
                        false_reply = true;
                    }
                } else {
                    false_reply = true;
                }
            }
            if (false_reply) {
                params.returning = true;
                DEBUG << "returning negative reply\n";
                params.reachable = false;
                /*
                if (prev_node.loc < NUM_VTS) { // never got out of source node, sending back to vt
                    next_node = &prev_node;
                } else {
                    for (auto &x: n.in_edges) {
                        if (x.second->nbr.handle == prev_node.handle) {
                            next_node = &x.second->nbr;
                            x.second->traverse();
                            break;
                        }
                    }
                }
                */
                next.emplace_back(std::make_pair(prev_node, params));
            }
        } else { // reply mode
            DEBUG << "got reachable reply\n";
            if (params.reachable) {
                if (state.hops > params.hops) {
                    state.hops = params.hops;
                }
            }
            if (((--state.out_count == 0) || params.reachable) && !state.reachable) {
                state.reachable |= params.reachable;
                /*
                if (state.prev_node.loc < NUM_VTS) { // sending back to vt
                    next_node = &state.prev_node;
                } else {
                    for (auto &x: n.in_edges) {
                        if (x.second->nbr.handle == state.prev_node.handle) {
                            next_node = &x.second->nbr;
                            x.second->traverse();
                            //if (state.reachable) {
                            //}
                            break;
                        }
                    }
                }
                */
                if (params.reachable) {
                    params.hops = state.hops + 1;
                    params.path.emplace_back(rn);
                }
                next.emplace_back(std::make_pair(state.prev_node, params));
            }
            if ((int)state.out_count < 0) {
                DEBUG << "ALERT! Bad state value in reach program for node " << rn.handle
                        << " at loc " << rn.loc << std::endl;
                next.clear();
                while(1);
            }
        }
        //DEBUG << "done reach prog\n";
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

#endif //__N_HOP_REACH_PROG__
