/*
 * ===============================================================
 *    Description:  Dijkstra shortest path program.
 *
 *        Created:  Sunday 21 April 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __CLUSTERING_PROG__
#define __CLUSTERING_PROG__

#include <vector>

#include "common/message.h"
#include "db/element/node.h"
#include "db/element/remote_node.h"
#include "common/vclock.h"
#include "common/event_order.h"

namespace node_prog
{
    class clustering_params : public virtual Node_Parameters_Base 
    {
        public:
            bool is_center;
            db::element::remote_node center;
            bool outgoing;
            std::vector<uint64_t> neighbors;
            double clustering_coeff;
            uint64_t vt_id;

        public:
            virtual bool search_cache() {
                return false;
            }

            virtual uint64_t cache_key() {
                return 0;
            }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(is_center) + message::size(center);
                toRet += message::size(outgoing) + message::size(neighbors);
                toRet += message::size(clustering_coeff);
                toRet += message::size(vt_id);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, is_center);
                message::pack_buffer(packer, center);
                message::pack_buffer(packer, outgoing);
                message::pack_buffer(packer, neighbors);
                message::pack_buffer(packer, clustering_coeff);
                message::pack_buffer(packer, vt_id);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, is_center);
                message::unpack_buffer(unpacker, center);
                message::unpack_buffer(unpacker, outgoing);
                message::unpack_buffer(unpacker, neighbors);
                message::unpack_buffer(unpacker, clustering_coeff);
                message::unpack_buffer(unpacker, vt_id);
            }
    };

    struct clustering_node_state : public virtual Node_State_Base
    {
        // map from a node (by its create time) to the number of neighbors who are connected to it
        std::unordered_map<uint64_t, int> neighbor_counts;
        int responses_left;


        virtual ~clustering_node_state() { }

        virtual uint64_t size() const
        {
            return message::size(neighbor_counts)
                + message::size(responses_left);
        }
        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, neighbor_counts);
            message::pack_buffer(packer, responses_left);
        }
        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, neighbor_counts);
            message::unpack_buffer(unpacker, responses_left);
        }
    };

    struct clustering_cache_value : public virtual Cache_Value_Base 
    {
        public:

        virtual ~clustering_cache_value () { }

        virtual uint64_t size() const 
        {
            return 0;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
        }

        virtual void unpack(e::unpacker& unpacker)
        {
        }
    };

    void calculate_response(clustering_node_state &cstate, 
            std::vector<std::pair<db::element::remote_node, clustering_params>> &next,
            clustering_params &params) 
    {
        if (cstate.neighbor_counts.size() > 1) {
            double denominator = (double) (cstate.neighbor_counts.size() * (cstate.neighbor_counts.size() - 1));
            int numerator = 0;
            for (std::pair<const uint64_t, int> nbr_count : cstate.neighbor_counts){
                numerator += nbr_count.second;
            }
            params.clustering_coeff = (double) numerator / denominator;
        } else {
            params.clustering_coeff = 0;
        }
        db::element::remote_node coord(params.vt_id, 1337);
        next.emplace_back(std::make_pair(db::element::remote_node(params.vt_id, 1337), params));
    }

    bool
    check_nbr(db::element::edge *e, vc::vclock &vclk)
    {
        int64_t cmp_1 = order::compare_two_vts(e->get_creat_time(), vclk);
        assert(cmp_1 != 2);
        bool traverse_edge = (cmp_1 == 0);
        if (traverse_edge) {
            int64_t cmp_2 = order::compare_two_vts(e->get_del_time(), vclk);
            assert(cmp_2 != 2);
            traverse_edge = (cmp_2 == 1);
        }
        return traverse_edge;
    }

    std::vector<std::pair<db::element::remote_node, clustering_params>> 
    clustering_node_program(uint64_t,
            db::element::node &n,
            db::element::remote_node &rn,
            clustering_params &params,
            std::function<clustering_node_state&()> state_getter,
            std::shared_ptr<vc::vclock> &req_vclock,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
            db::caching::cache_response *cache_response)
    {
        std::vector<std::pair<db::element::remote_node, clustering_params>> next;
        if (params.is_center) {
            node_prog::clustering_node_state &cstate = state_getter();
            if (params.outgoing) {
                    params.is_center = false;
                    params.center = rn;
                    db::element::edge *e;
                    for (std::pair<const uint64_t, db::element::edge*> &possible_nbr : n.out_edges) {
                        e = possible_nbr.second;
                        if (check_nbr(e, *req_vclock)) {
                            next.emplace_back(std::make_pair(possible_nbr.second->nbr, params));
                            cstate.neighbor_counts.insert(std::make_pair(possible_nbr.second->nbr.handle, 0));
                            cstate.responses_left++;
                            e->traverse();
                        }
                    }
                    if (cstate.responses_left == 0) {
                        calculate_response(cstate, next, params);
                    }
            } else {
                for (uint64_t poss_nbr : params.neighbors) {
                    if (cstate.neighbor_counts.count(poss_nbr) > 0) {
                        cstate.neighbor_counts[poss_nbr]++;
                    }
                }
                if (--cstate.responses_left == 0){
                    calculate_response(cstate, next, params);
                }
            }
        } else { // not center
            for (std::pair<const uint64_t, db::element::edge*> &possible_nbr : n.out_edges) {
                if (check_nbr(possible_nbr.second, *req_vclock)) {
                    params.neighbors.push_back(possible_nbr.second->nbr.handle);
                }
            }
            params.outgoing = false;
            params.is_center = true;
            next.emplace_back(std::make_pair(params.center, params));
        }
        return next;
    }

}


#endif
