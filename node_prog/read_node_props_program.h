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

#ifndef weaver_node_prog_read_node_props_program_h 
#define weaver_node_prog_read_node_props_program_h 

#include <vector>
#include <string>

#include "common/message.h"
#include "common/vclock.h"
#include "node.h"
#include "edge.h"
#include "db/element/remote_node.h"

namespace node_prog
{
    class read_node_props_params : public virtual Node_Parameters_Base 
    {
        public:
            std::vector<std::string> keys; // empty vector means fetch all props
            std::vector<std::pair<std::string, std::string>> node_props;

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
                    + message::size(node_props);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, keys);
                message::pack_buffer(packer, node_props);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, keys);
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

    inline std::pair<search_type, std::vector<std::pair<db::element::remote_node, read_node_props_params>>>
    read_node_props_node_program(
            node &n,
            db::element::remote_node &,
            read_node_props_params &params,
            std::function<read_node_props_state&()>,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            cache_response<Cache_Value_Base>*)
    {
        bool fetch_all = params.keys.empty();
        for (property &prop : n.get_properties()) {
            if (fetch_all || (std::find(params.keys.begin(), params.keys.end(), prop.get_key()) != params.keys.end())) {
                params.node_props.emplace_back(prop.get_key(), prop.get_value());
            }
        }

        return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::element::remote_node, read_node_props_params>>
                (1, std::make_pair(db::element::coordinator, std::move(params)))); 
    }
}

#endif
