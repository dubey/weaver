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
#include "common/vclock.h"
#include "common/event_order.h"
#include "common/public_graph_elems/node.h"
#include "common/public_graph_elems/edge.h"
#include "common/public_graph_elems/node_ptr.h"

namespace node_prog
{
    class clustering_params : public virtual Node_Parameters_Base 
    {
        public:
            bool _search_cache;
            uint64_t _cache_key;
            bool is_center;
            common::node_ptr center;
            bool outgoing;
            std::vector<uint64_t> neighbors;
            double clustering_coeff;
            uint64_t vt_id;

        public:
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
                    + message::size(is_center)
                    + message::size(center)
                    + message::size(outgoing)
                    + message::size(neighbors)
                    + message::size(clustering_coeff)
                    + message::size(vt_id);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, _search_cache);
                message::pack_buffer(packer, _cache_key);
                message::pack_buffer(packer, is_center);
                message::pack_buffer(packer, center);
                message::pack_buffer(packer, outgoing);
                message::pack_buffer(packer, neighbors);
                message::pack_buffer(packer, clustering_coeff);
                message::pack_buffer(packer, vt_id);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, _search_cache);
                message::unpack_buffer(unpacker, _cache_key);
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

    std::vector<std::pair<common::node_ptr, clustering_params>> 
    clustering_node_program(
            common::node &n,
            common::node_ptr &rn,
            clustering_params &params,
            std::function<clustering_node_state&()> get_state,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<common::node_ptr>>, uint64_t)>&,
            std::unique_ptr<db::caching::cache_response>)
    {
        // TODO can we change this to a three enum switch to reduce number of if statements
        std::vector<std::pair<common::node_ptr, clustering_params>> next;
        if (params.is_center) {
            node_prog::clustering_node_state &cstate = get_state();
            if (params.outgoing) {
                params.is_center = false;
                params.center = rn;
                for (common::edge& edge : n.get_edges()) {
                    next.emplace_back(std::make_pair(edge.get_neighbor(), params));
                    cstate.neighbor_counts.insert(std::make_pair(edge.get_neighbor().get_handle(), 0));
                    cstate.responses_left++;
                }
                if (cstate.responses_left < 2) { // if no or one neighbor we know clustering coeff already
                    params.clustering_coeff = 0;
                    next = {std::make_pair(common::coordinator, std::move(params))};
                }
            } else {
                for (uint64_t nbr : params.neighbors) {
                    if (cstate.neighbor_counts.count(nbr) > 0) {
                        cstate.neighbor_counts[nbr]++;
                    }
                }
                if (--cstate.responses_left == 0) {
                    assert(cstate.neighbor_counts.size() > 1);
                    double denominator = (double) (cstate.neighbor_counts.size() * (cstate.neighbor_counts.size() - 1));
                    uint64_t numerator = 0;
                    for (std::pair<const uint64_t, int>& nbr_count : cstate.neighbor_counts){
                        numerator += nbr_count.second;
                    }
                    params.clustering_coeff = (double) numerator / denominator;
                    next = {std::make_pair(common::coordinator, std::move(params))};
                } 
            }
        } else { // not center
            for (common::edge& edge : n.get_edges()) {
                params.neighbors.push_back(edge.get_neighbor().get_handle());
            }
            params.outgoing = false;
            params.is_center = true;
            next = {std::make_pair(params.center, std::move(params))};
        }
        return next;
    }
}

#endif
