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
            db::element::remote_node dest;
            std::vector<common::property> edge_props;
            bool reachable;

        public:
            reach_params()
                : mode(false)
                , reachable(false)
            {
            }

            virtual size_t size() const 
            {
                size_t toRet = message::size(mode)
                    + message::size(prev_node)
                    + message::size(dest) 
                    + message::size(edge_props)
                    + message::size(reachable);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, mode);
                message::pack_buffer(packer, prev_node);
                message::pack_buffer(packer, dest);
                message::pack_buffer(packer, edge_props);
                message::pack_buffer(packer, reachable);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, mode);
                message::unpack_buffer(unpacker, prev_node);
                message::unpack_buffer(unpacker, dest);
                message::unpack_buffer(unpacker, edge_props);
                message::unpack_buffer(unpacker, reachable);
            }
    };

    struct reach_node_state : Deletable 
    {
        bool visited;
        db::element::remote_node prev_node; // previous node
        uint32_t out_count; // number of requests propagated
        bool reachable;

        reach_node_state()
            : visited(false)
            , out_count(0)
            , reachable(false)
        {
        }

        virtual ~reach_node_state()
        {
        }
    };

    struct reach_cache_value : CacheValueBase 
    {
        int dummy;

        virtual ~reach_cache_value()
        {
        }
    };

    std::vector<std::pair<db::element::remote_node, reach_params>> 
    reach_node_program(uint64_t req_id,
            db::element::node &n,
            db::element::remote_node &rn,
            reach_params &params,
            std::function<reach_node_state&()> state_getter,
            std::function<reach_cache_value&()> cache_getter,
            std::function<std::vector<reach_cache_value *>()> cached_values_getter)
    {
        reach_node_state state = state_getter();
        std::cout << "Reachability program" << std::endl;
        std::cout << "Node handle " << rn.handle << " node loc " << rn.loc << std::endl;
        bool false_reply = false;
        db::element::remote_node prev_node = params.prev_node;
        params.prev_node = rn;
        std::vector<std::pair<db::element::remote_node, reach_params>> next;
        if (!params.mode) { // request mode
            std::cout << "Got request\n";
            if (params.dest.handle == rn.handle) {
                params.mode = true;
                params.reachable = true;
                next.emplace_back(std::make_pair(prev_node, params));
                std::cout << "True reply now\n";
                std::cout << "Prev node " << state.prev_node.handle << " " << state.prev_node.loc << std::endl;
                // TODO signal deletion of state
            } else if (!state.visited) {
                db::element::edge *e;
                state.prev_node = prev_node;
                state.visited = true;
                for (auto &iter: n.out_edges) {
                    e = iter.second;
                    bool traverse_edge = e->get_creat_time() <= req_id
                        && e->get_del_time() > req_id; // edge created and deleted in acceptable timeframe
                    // checking edge properties
                    for (auto &prop: params.edge_props) {
                        if (!e->has_property(prop)) {
                            traverse_edge = false;
                            break;
                        }
                    }
                    if (traverse_edge) {
                        next.emplace_back(std::make_pair(e->nbr, params)); // propagate reachability request
                        state.out_count++;
                    }
                }
                if (state.out_count == 0) {
                    false_reply = true;
                }
            } else {
                false_reply = true;
            }
            if (false_reply) {
                params.mode = true;
                params.reachable = false;
                next.emplace_back(std::make_pair(prev_node, params));
                std::cout << "False reply now\n";
            }
        } else { // reply mode
            std::cout << "Got reply\n";
            if (((state.out_count == 0) || params.reachable) && !state.reachable) {
                state.reachable |= params.reachable;
                next.emplace_back(std::make_pair(state.prev_node, params));
                std::cout << "Prop reply\n";
                std::cout << "Prev node " << state.prev_node.handle << " " << state.prev_node.loc << std::endl;
            }
        }
        return next;
    }
}

#endif //__DIKJSTRA_PROG__
