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

#ifndef __READ_NODE_PROPS_PROG__
#define __READ_NODE_PROPS_PROG__

#include <vector>
#include <string>

#include "common/message.h"
#include "common/vclock.h"
#include "common/event_order.h"
#include "common/public_graph_elems/node.h"
#include "common/public_graph_elems/edge.h"
#include "common/public_graph_elems/node_ptr.h"

namespace node_prog
{
    class read_node_props_params : public virtual Node_Parameters_Base 
    {
        public:
            std::vector<std::string> keys; // empty vector means fetch all props
            uint64_t vt_id;
            std::vector<db::element::property> node_props;

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
                    + message::size(vt_id)
                    + message::size(node_props);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, keys);
                message::pack_buffer(packer, vt_id);
                message::pack_buffer(packer, node_props);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, keys);
                message::unpack_buffer(unpacker, vt_id);
                message::unpack_buffer(unpacker, node_props);
            }
    };

    struct read_node_props_state : public virtual Node_State_Base
    {
        virtual ~read_node_props_state() { }

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

    struct read_node_props_cache_value : public virtual Cache_Value_Base 
    {
        virtual ~read_node_props_cache_value() { }

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

    std::vector<std::pair<common::node_ptr, read_node_props_params>> 
    read_node_props_node_program(
            common::node &n,
            common::node_ptr &,
            read_node_props_params &params,
            std::function<read_node_props_state&()>,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<common::node_ptr>>, uint64_t)>&,
            std::unique_ptr<db::caching::cache_response>)
    {
        for (const db::element::property &prop : *n.get_props())
        {
            bool key_match = params.keys.empty() || (std::find(params.keys.begin(), params.keys.end(), prop.key) != params.keys.end());
            if (key_match && order::clock_creat_before_del_after(*req_vclock, prop.get_creat_time(), prop.get_del_time()))
            {
                params.node_props.emplace_back(prop);
            }
        }

        return {std::make_pair(common::node_ptr(params.vt_id, 1337), std::move(params))}; // initializer list of vector
    }
}

#endif
