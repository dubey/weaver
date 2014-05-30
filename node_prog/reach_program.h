/*
 * ===============================================================
 *    Description:  Reachability program: classes declaration.
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

#ifndef weaver_node_prog_reach_program_h_
#define weaver_node_prog_reach_program_h_

#include <vector>
#include <string>

#include "db/remote_node.h"
#include "node_prog/base_classes.h"
#include "node_prog/node.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class reach_params : public virtual Node_Parameters_Base  
    {
        public:
            bool _search_cache;
            uint64_t _cache_key;
            bool returning; // false = request, true = reply
            db::element::remote_node prev_node;
            uint64_t dest;
            std::vector<std::pair<std::string, std::string>> edge_props;
            uint16_t hops;
            bool reachable;
            std::vector<db::element::remote_node> path;

        public:
            reach_params();
            virtual ~reach_params() { }
            virtual bool search_cache() { return _search_cache; }
            virtual uint64_t cache_key() { return _cache_key; }
            virtual uint64_t size() const;
            virtual void pack(e::buffer::packer &packer) const; 
            virtual void unpack(e::unpacker &unpacker);
    };

    struct reach_node_state : public virtual Node_State_Base 
    {
        bool visited;
        db::element::remote_node prev_node; // previous node
        uint32_t out_count; // number of requests propagated
        bool reachable;
        uint16_t hops;

        reach_node_state();
        virtual ~reach_node_state() { }
        virtual uint64_t size() const; 
        virtual void pack(e::buffer::packer& packer) const ;
        virtual void unpack(e::unpacker& unpacker);
    };

    struct reach_cache_value : public virtual Cache_Value_Base 
    {
        std::vector<db::element::remote_node> path;

        reach_cache_value(std::vector<db::element::remote_node> &cpy);
        virtual ~reach_cache_value () { }
        virtual uint64_t size() const;
        virtual void pack(e::buffer::packer& packer) const;
        virtual void unpack(e::unpacker& unpacker);
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, reach_params>>>
    reach_node_program(
            node &n,
            db::element::remote_node &rn,
            reach_params &params,
            std::function<reach_node_state&()> state_getter,
            std::function<void(std::shared_ptr<reach_cache_value>, // TODO make const
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
            cache_response<reach_cache_value>*cache_response);
}

#endif
