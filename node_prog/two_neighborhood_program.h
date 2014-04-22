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

#ifndef weaver_node_prog_two_neighborhood_program_h 
#define weaver_node_prog_two_neighborhood_program_h 

#include <vector>
#include <string>

#include "common/message.h"
#include "common/vclock.h"
#include "common/event_order.h"
#include "node.h"
#include "edge.h"
#include "db/element/remote_node.h"

namespace node_prog
{
    class two_neighborhood_params : public virtual Node_Parameters_Base 
    {
        public:
            bool _search_cache;
            std::string prop_key;
            uint32_t on_hop;
            bool outgoing;
            db::element::remote_node prev_node;
            std::vector<std::pair<uint64_t, std::string>> responses;

        public:
            virtual bool search_cache() {
                return _search_cache;
            }

            virtual uint64_t cache_key() {
                return std::hash<std::string>()(prop_key);
            }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(prop_key)
                    + message::size(on_hop)
                    + message::size(outgoing)
                    + message::size(prev_node)
                    + message::size(responses);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, prop_key);
                message::pack_buffer(packer, on_hop);
                message::pack_buffer(packer, outgoing);
                message::pack_buffer(packer, prev_node);
                message::pack_buffer(packer, responses);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, prop_key);
                message::unpack_buffer(unpacker, on_hop);
                message::unpack_buffer(unpacker, outgoing);
                message::unpack_buffer(unpacker, prev_node);
                message::unpack_buffer(unpacker, responses);
            }
    };

    struct two_neighborhood_state : public virtual Node_State_Base
    {
        bool one_hop_visited;
        bool two_hop_visited;
        uint32_t responses_left;
        db::element::remote_node prev_node;
        std::vector<std::pair<uint64_t, std::string>> responses;

        two_neighborhood_state()
            : one_hop_visited(false)
            , two_hop_visited(false)
            , responses_left(0)
            , prev_node()
        { }

        virtual ~two_neighborhood_state() { }

        virtual uint64_t size() const
        {
            uint64_t toRet = message::size(one_hop_visited)
                + message::size(two_hop_visited)
                + message::size(responses_left)
                + message::size(prev_node)
                + message::size(responses);
                return toRet;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, one_hop_visited);
            message::pack_buffer(packer, two_hop_visited);
            message::pack_buffer(packer, responses_left);
            message::pack_buffer(packer, prev_node);
            message::pack_buffer(packer, responses);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, one_hop_visited);
            message::unpack_buffer(unpacker, two_hop_visited);
            message::unpack_buffer(unpacker, responses_left);
            message::unpack_buffer(unpacker, prev_node);
            message::unpack_buffer(unpacker, responses);
        }
    };

    struct two_neighborhood_cache_value : public virtual Cache_Value_Base 
    {
        public:
        std::string props_key;
        std::vector<std::pair<uint64_t, std::string>> responses;

        two_neighborhood_cache_value(std::string &props_key, std::vector<std::pair<uint64_t, std::string>> &responses)
            : props_key(props_key)
            , responses(responses)
        { }

        virtual ~two_neighborhood_cache_value () { }

        virtual uint64_t size() const 
        {
            uint64_t toRet = message::size(props_key)
                + message::size(responses);
                return toRet;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, props_key);
            message::pack_buffer(packer, responses);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, props_key);
            message::unpack_buffer(unpacker, responses);
        }
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, two_neighborhood_params>>>
    two_neighborhood_node_program(
            node &n,
            db::element::remote_node &,
            two_neighborhood_params &params,
            std::function<two_neighborhood_state&()>,
            std::function<void(std::shared_ptr<two_neighborhood_cache_value>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)> &add_cache_func,
            cache_response<Cache_Value_Base> *cache_response);
}

#endif
