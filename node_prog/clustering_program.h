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
            bool _search_cache;
            uint64_t _cache_key;
            bool is_center;
            db::element::remote_node center;
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

    inline uint64_t
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
            }
            else {
                if (pair.second.node_deleted){
                    return false;
                }
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
        WDEBUG << "updating cached value from another" << std::endl;
        add_to_cache(numerator, n, rn, req_vclock, add_cache_func);
        return (double) numerator / denominator;
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
            std::unique_ptr<db::caching::cache_response> cache_response)
    {
        std::vector<std::pair<db::element::remote_node, clustering_params>> next;
        if (MAX_CACHE_ENTRIES)
        {
        if (params._search_cache && cache_response != NULL){
            // check context, update cache
            WDEBUG  << "checking clustering cache context" << std::endl;
            bool valid = check_cache_context(cache_response->context, params.center);
            if (valid) {
                WDEBUG  << "WEEE GOT A valid CACHE RESPONSE, short circuit" << std::endl;
                std::shared_ptr<clustering_cache_value> val = std::dynamic_pointer_cast<clustering_cache_value>(cache_response->value);
                params.clustering_coeff = calculate_from_cached(cache_response->context, *cache_response->watch_set,
                        params.center, val->numerator, *req_vclock, n, rn, add_cache_func);
                db::element::remote_node coord(params.vt_id, 1337);
                next.emplace_back(std::make_pair(coord, params));

                return next;
            }
            cache_response->invalidate();
        }
        params._search_cache = false; // only search cache for first of req
        }
        if (params.is_center) {
            node_prog::clustering_node_state &cstate = state_getter();
            if (params.outgoing) {
                    params.is_center = false;
                    params.center = rn;
                    db::element::edge *e;
                    for (std::pair<const uint64_t, db::element::edge*> &possible_nbr : n.out_edges) {
                        e = possible_nbr.second;
                        if (order::clock_creat_before_del_after(*req_vclock, e->get_creat_time(), e->get_del_time()))
                        {
                            next.emplace_back(std::make_pair(possible_nbr.second->nbr, params));
                            cstate.neighbor_counts.insert(std::make_pair(possible_nbr.second->nbr.handle, 0));
                            cstate.responses_left++;
                            e->traverse();
                        }
                    }
                    if (cstate.responses_left == 0) {
                        calculate_response(cstate, n, rn, *req_vclock, next, params, add_cache_func);
                    }
            } else {
                for (uint64_t poss_nbr : params.neighbors) {
                    if (cstate.neighbor_counts.count(poss_nbr) > 0) {
                        cstate.neighbor_counts[poss_nbr]++;
                    }
                }
                if (--cstate.responses_left == 0){
                    calculate_response(cstate, n, rn, *req_vclock, next, params, add_cache_func);
                }
            }
        } else { // not center
            for (std::pair<const uint64_t, db::element::edge*> &possible_nbr : n.out_edges) {
                if (order::clock_creat_before_del_after(
                            *req_vclock, possible_nbr.second->get_creat_time(), possible_nbr.second->get_del_time()))
                {
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
