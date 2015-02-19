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
#include <string>

#include "db/remote_node.h"
#include "node_prog/base_classes.h"
#include "node_prog/node.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class pathless_reach_params : public virtual Node_Parameters_Base  
    {
        public:
            bool returning; // false = request, true = reply
            db::remote_node prev_node;
            node_handle_t dest;
            std::vector<std::pair<std::string, std::string>> edge_props;
            bool reachable;

        public:
            pathless_reach_params();
            ~pathless_reach_params() { }
            bool search_cache();
            cache_key_t cache_key() { return cache_key_t(); }
            uint64_t size() const; 
            void pack(e::buffer::packer &packer) const;
            void unpack(e::unpacker &unpacker);
    };

    struct pathless_reach_node_state : public virtual Node_State_Base 
    {
        bool visited;
        db::remote_node prev_node; // previous node
        uint32_t out_count; // number of requests propagated
        bool reachable;

        pathless_reach_node_state();
        ~pathless_reach_node_state() { }
        uint64_t size() const;
        void pack(e::buffer::packer& packer) const;
        void unpack(e::unpacker& unpacker);
    };

    std::pair<search_type, std::vector<std::pair<db::remote_node, pathless_reach_params>>>
    pathless_reach_node_program(
            node &n,
            db::remote_node &rn,
            pathless_reach_params &params,
            std::function<pathless_reach_node_state&()> state_getter,
            std::function<void(std::shared_ptr<Cache_Value_Base>,
                std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>& add_cache_func,
            cache_response<Cache_Value_Base>*cache_response);
}

#endif
