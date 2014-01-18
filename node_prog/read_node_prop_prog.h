/*
 * ===============================================================
 *    Description:  Node program to read properties of a single node
 *
 *        Created:  Friday 17 January 2014 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __READ_NODE_PROP_PROG__
#define __READ_NODE_PROP_PROG__

#include <vector>
#include <string>

#include "common/message.h"
#include "db/element/node.h"
#include "db/element/remote_node.h"
#include "common/vclock.h"
#include "common/event_order.h"

namespace node_prog
{
    class read_node_prop_params : public virtual Node_Parameters_Base 
    {
        public:
            std::vector<std::string> keys; // empty vector means fetch all props
            uint64_t vt_id;
            std::vector<common::property> edge_props;

        public:
            virtual bool search_cache() {
                return false; // would never need to cache
            }

            virtual uint64_t cache_key() {
                return 0;
            }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(keys)
                    + message::size(edge_props);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, keys);
                message::pack_buffer(packer, vt_id);
                message::pack_buffer(packer, edge_props);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, keys);
                message::unpack_buffer(unpacker, vt_id);
                message::unpack_buffer(unpacker, edge_props);
            }
    };

    struct read_node_prop_node_state : public virtual Node_State_Base
    {
        virtual ~read_node_prop_node_state() { }

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

    struct read_node_prop_cache_value : public virtual Cache_Value_Base 
    {
        virtual ~read_node_prop_cache_value() { }

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

    std::vector<std::pair<db::element::remote_node, read_node_prop_params>> 
    read_node_prop_node_program(uint64_t,
            db::element::node &n,
            db::element::remote_node &rn,
            clustering_params &params,
            std::function<clustering_node_state&()> state_getter,
            std::shared_ptr<vc::vclock> &req_vclock,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
            std::unique_ptr<db::caching::cache_response> cache_response)
    {
        db::element::remote_node coord(params.vt_id, 1337);
        std::vector<std::pair<db::element::remote_node, read_node_prop_params>> next;
        next.emplace_back(std::make_pair(coord, std::move(params)));

        return next;

        if (MAX_CACHE_ENTRIES)
        {
        if (params._search_cache && cache_response != NULL){
            // check context, update cache
            bool valid = check_cache_context(cache_response->context, rn);
            if (valid) {
                //WDEBUG  << "WEEE GOT A valid CACHE RESPONSE, short circuit" << std::endl;
                std::shared_ptr<clustering_cache_value> val = std::dynamic_pointer_cast<clustering_cache_value>(cache_response->value);
                params.clustering_coeff = calculate_from_cached(cache_response->context, *cache_response->watch_set,
                        rn, val->numerator, *req_vclock, n, rn, add_cache_func);
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
