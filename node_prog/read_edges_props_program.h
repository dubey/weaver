/*
 * ===============================================================
 *    Description:  Node program to read properties of edges on a single node
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

#ifndef __READ_EDGES_PROPS_PROG__
#define __READ_EDGES_PROPS_PROG__

#include <vector>
#include <string>

#include "common/message.h"
#include "db/element/node.h"
#include "db/element/remote_node.h"
#include "common/vclock.h"
#include "common/event_order.h"

namespace node_prog
{
    class read_edges_props_params : public virtual Node_Parameters_Base 
    {
        public:
            std::vector<uint64_t> edges; // empty vector means fetch props for all edges
            std::vector<std::string> keys; // empty vector means fetch all props
            uint64_t vt_id;
            std::vector<std::pair<uint64_t, std::vector<common::property>>> edges_props;

        public:
            virtual bool search_cache() {
                return false; // would never need to cache
            }

            virtual uint64_t cache_key() {
                return 0;
            }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(edges)
                    + message::size(keys)
                    + message::size(vt_id)
                    + message::size(edges_props);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, edges);
                message::pack_buffer(packer, keys);
                message::pack_buffer(packer, vt_id);
                message::pack_buffer(packer, edges_props);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, edges);
                message::unpack_buffer(unpacker, keys);
                message::unpack_buffer(unpacker, vt_id);
                message::unpack_buffer(unpacker, edges_props);
            }
    };

    struct read_edges_props_state : public virtual Node_State_Base
    {
        virtual ~read_edges_props_state() { }

        virtual uint64_t size() const
        {
            return 0;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            UNUSED(packer);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            UNUSED(unpacker);
        }
    };

    struct read_edges_props_cache_value : public virtual Cache_Value_Base 
    {
        virtual ~read_edges_props_cache_value() { }

        virtual uint64_t size() const 
        {
            return 0;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            UNUSED(packer);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            UNUSED(unpacker);
        }
    };

    inline void
    record_desired_props(db::element::edge *edge, std::vector<std::string> &keys, vc::vclock &req_vclock,
            std::vector<std::pair<uint64_t, std::vector<common::property>>> &add_to)
    {
            std::vector<common::property> matching_edge_props;

            for (const common::property &prop : *edge->get_props())
            {
                bool key_match = keys.empty() || (std::find(keys.begin(), keys.end(), prop.key) != keys.end());
                if (key_match && order::clock_creat_before_del_after(req_vclock, prop.get_creat_time(), prop.get_del_time()))
                {
                    matching_edge_props.emplace_back(prop);
                }
            }
            if (!matching_edge_props.empty())
            {
                add_to.emplace_back(edge->get_handle(), std::move(matching_edge_props));
            }
    }

    std::vector<std::pair<db::element::remote_node, read_edges_props_params>> 
    read_edges_props_node_program(uint64_t,
            db::element::node &n,
            db::element::remote_node &,
            read_edges_props_params &params,
            std::function<read_edges_props_state&()>,
            std::shared_ptr<vc::vclock> &req_vclock,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            std::unique_ptr<db::caching::cache_response>)
    {
        db::element::remote_node coord(params.vt_id, 1337);
        std::vector<std::pair<db::element::remote_node, read_edges_props_params>> next;
        next.emplace_back(std::make_pair(coord, std::move(params)));

        if (params.edges.empty()) {
            for (auto &edge_pair : n.out_edges)
            {
                record_desired_props(edge_pair.second, params.keys, *req_vclock, next[0].second.edges_props);
            }
        } else {
            for(uint64_t edge_handle : params.edges)
            {
                auto edge_iter = n.out_edges.find(edge_handle);
                if (edge_iter == n.out_edges.end()){
                    WDEBUG << "edge handle didnt exist in edge prop read program" << std::endl;
                } else {
                    record_desired_props(edge_iter->second, params.keys, *req_vclock, next[0].second.edges_props);
                }
            }
        }
        return next;
    }
}

#endif
