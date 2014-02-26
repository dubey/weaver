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
#include "node.h"
#include "edge.h"
#include "db/element/remote_node.h"

namespace node_prog
{
    class clustering_params : public virtual Node_Parameters_Base 
    {
        public:
            bool _search_cache;
            uint64_t _cache_key;
            bool is_center;
            db::element::remote_node center;
            bool outgoing;
            std::vector<uint64_t> neighbors;
            double clustering_coeff;

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
                    + message::size(clustering_coeff);
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
            }
    };

    struct clustering_node_state : public virtual Node_State_Base
    {
        // map from a node id to the number of neighbors who are connected to it
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

    // TODO fix caching for new interface
    /*
    struct clustering_cache_value : public virtual Cache_Value_Base 
    {
        public:
        uint64_t numerator;

        clustering_cache_value(uint64_t num) : numerator(num) {}; 

        virtual ~clustering_cache_value () { }

        virtual uint64_t size() const 
        {
            return message::size(numerator);
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, numerator);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, numerator);
        }
    };

    inline void
    add_to_cache(uint64_t numerator, db::element::node &n, db::element::remote_node &rn, vc::vclock &req_vclock,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func)
    {
        std::shared_ptr<node_prog::clustering_cache_value> toCache(new clustering_cache_value(numerator));
        std::shared_ptr<std::vector<db::element::remote_node>> watch_set(new std::vector<db::element::remote_node>());
        // add center node and valid neighbors to watch set XXX re-use old list for re-caching
        watch_set->emplace_back(rn);
        for (std::pair<const uint64_t, db::element::edge*> &possible_nbr : n.out_edges) {
            if (order::clock_creat_before_del_after(req_vclock, possible_nbr.second->get_creat_time(), possible_nbr.second->get_del_time()))
            {
                watch_set->emplace_back(possible_nbr.second->nbr);
            }
        }
        uint64_t cache_key = rn.handle;
        add_cache_func(toCache, watch_set, cache_key);
    }

    inline void
    calculate_response(clustering_node_state &cstate, db::element::node &n, db::element::remote_node &rn, vc::vclock &req_vclock,
            std::vector<std::pair<db::element::remote_node, clustering_params>> &next,
            clustering_params &params, std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func)
    {
        if (cstate.neighbor_counts.size() > 1) {
            double denominator = (double) (cstate.neighbor_counts.size() * (cstate.neighbor_counts.size() - 1));
            uint64_t numerator = 0;
            for (std::pair<const uint64_t, int> nbr_count : cstate.neighbor_counts){
                numerator += nbr_count.second;
            }
            params.clustering_coeff = (double) numerator / denominator;
            if (MAX_CACHE_ENTRIES)
            {
                add_to_cache(numerator, n, rn, req_vclock, add_cache_func);
            }
        } else {
            params.clustering_coeff = 0;
        }
        db::element::remote_node coord(params.vt_id, 1337);
        next.emplace_back(std::make_pair(db::element::remote_node(params.vt_id, 1337), params));
    }

    inline bool
    check_cache_context(std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>>& context, db::element::remote_node& center)
    {
        // if center unchanged and no neighbor was deleted cache is valid
        for (auto& pair : context)
        {
            if (pair.first == center){
                assert(!pair.second.node_deleted);
                if (!pair.second.edges_added.empty() || !pair.second.edges_deleted.empty()){
                    return false;
                }
            } else if (pair.second.node_deleted){
                    return false;
            }
        }
        return true;
    }

    inline double
    calculate_from_cached(std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>>& context,
            std::vector<db::element::remote_node>& watch_set, db::element::remote_node& center, uint64_t numerator,
            vc::vclock &req_vclock, db::element::node &n, db::element::remote_node &rn,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func)
    {
        if (watch_set.size() <= 2){
            return 0;
        }
        double denominator = (watch_set.size()-1)*(watch_set.size()-2);

        for (auto& pair : context)
        {
            assert(!pair.second.node_deleted);
            if (pair.first == center){
                continue;
            }
            for (auto& e : pair.second.edges_added){
                if (e.nbr != center && std::find(watch_set.begin(), watch_set.end(), e.nbr) != watch_set.end()){
                    numerator++;
                }
            }
            for (auto& e : pair.second.edges_deleted){
                if (e.nbr != center && std::find(watch_set.begin(), watch_set.end(), e.nbr) != watch_set.end()){
                    numerator--;
                }
            }
        }
        add_to_cache(numerator, n, rn, req_vclock, add_cache_func);
        return (double) numerator / denominator;
    }
    */

    inline std::vector<std::pair<db::element::remote_node, clustering_params>> 
    clustering_node_program(
            node &n,
            db::element::remote_node &rn,
            clustering_params &params,
            std::function<clustering_node_state&()> get_state,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            cache_response<Cache_Value_Base>*)
    {
        // TODO can we change this to a three enum switch to reduce number of if statements
        std::vector<std::pair<db::element::remote_node, clustering_params>> next;
        if (params.is_center) {
            node_prog::clustering_node_state &cstate = get_state();
            if (params.outgoing) {
                params.is_center = false;
                params.center = rn;
                for (edge& edge : n.get_edges()) {
                    next.emplace_back(std::make_pair(edge.get_neighbor(), params));
                    cstate.neighbor_counts.insert(std::make_pair(edge.get_neighbor().get_id(), 0));
                    cstate.responses_left++;
                }
                if (cstate.responses_left < 2) { // if no or one neighbor we know clustering coeff already
                    params.clustering_coeff = 0;
                    next = {std::make_pair(db::element::coordinator, std::move(params))};
                }
            } else {
                for (uint64_t &nbr_id : params.neighbors) {
                    if (cstate.neighbor_counts.count(nbr_id) > 0) {
                        cstate.neighbor_counts[nbr_id]++;
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
                    next = {std::make_pair(db::element::coordinator, std::move(params))};
                } 
            }
        } else { // not center
            for (edge& edge : n.get_edges()) {
                params.neighbors.push_back(edge.get_neighbor().get_id());
            }
            params.outgoing = false;
            params.is_center = true;
            next = {std::make_pair(params.center, std::move(params))};
        }
        return next;
    }
}

#endif
