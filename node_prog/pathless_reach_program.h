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

#ifndef weaver_node_prog_pathless_reach_program_h_
#define weaver_node_prog_pathless_reach_program_h_

#include <vector>

#include "node_prog/base_classes.h"
#include "common/weaver_constants.h"
//#include "db/cache/prog_cache.h"
#include "common/message.h"
#include "common/vclock.h"
#include "common/event_order.h"
#include "db/element/edge.h"
#include "node.h"
#include "edge.h"
#include "db/element/remote_node.h"

namespace node_prog
{
    class pathless_reach_params : public virtual Node_Parameters_Base  
    {
        public:
            bool returning; // false = request, true = reply
            db::element::remote_node prev_node;
            uint64_t dest;
            std::vector<std::pair<std::string, std::string>> edge_props;
            bool reachable;

        public:
            pathless_reach_params()
                : returning(false)
                , reachable(false)
            { }
            
            virtual ~pathless_reach_params() { }

            virtual bool search_cache() {
                return false;
            }

            virtual uint64_t cache_key() {
                return 0;
            }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(returning)
                    + message::size(prev_node)
                    + message::size(dest) 
                    + message::size(edge_props)
                    + message::size(reachable);
                return toRet;
            }

            virtual void pack(e::buffer::packer &packer) const 
            {
                message::pack_buffer(packer, returning);
                message::pack_buffer(packer, prev_node);
                message::pack_buffer(packer, dest);
                message::pack_buffer(packer, edge_props);
                message::pack_buffer(packer, reachable);
            }

            virtual void unpack(e::unpacker &unpacker)
            {
                message::unpack_buffer(unpacker, returning);
                message::unpack_buffer(unpacker, prev_node);
                message::unpack_buffer(unpacker, dest);
                message::unpack_buffer(unpacker, edge_props);
                message::unpack_buffer(unpacker, reachable);
            }
    };

    struct pathless_reach_node_state : public virtual Node_State_Base 
    {
        bool visited;
        db::element::remote_node prev_node; // previous node
        uint32_t out_count; // number of requests propagated
        bool reachable;

        pathless_reach_node_state()
            : visited(false)
            , out_count(0)
            , reachable(false)
        { }

        virtual ~pathless_reach_node_state() { }

        virtual uint64_t size() const 
        {
            uint64_t toRet = message::size(visited)
                + message::size(prev_node)
                + message::size(out_count)
                + message::size(reachable);
            return toRet;
        }

        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, visited);
            message::pack_buffer(packer, prev_node);
            message::pack_buffer(packer, out_count);
            message::pack_buffer(packer, reachable);
        }

        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, visited);
            message::unpack_buffer(unpacker, prev_node);
            message::unpack_buffer(unpacker, out_count);
            message::unpack_buffer(unpacker, reachable);
        }
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, pathless_reach_params>>>
    pathless_reach_node_program(
            node &n,
            db::element::remote_node &rn,
            pathless_reach_params &params,
            std::function<pathless_reach_node_state&()> state_getter,
            std::function<void(std::shared_ptr<Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
            cache_response<Cache_Value_Base>*cache_response);
}

#endif
