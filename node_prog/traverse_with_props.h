/*
 * ===============================================================
 *    Description:  Program which traverses graph starting at a
 *                  user-specified node.  For each traversal hop,
 *                  user specifies a list of node and edge props 
 *                  must be satisfied.
 *
 *        Created:  2014-06-23 11:08:56
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_traverse_with_props_h_
#define weaver_node_prog_traverse_with_props_h_

#include <string>
#include <vector>
#include <deque>

#include "db/remote_node.h"
#include "node_prog/node.h"
#include "node_prog/base_classes.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    struct traverse_props_params: public virtual Node_Parameters_Base
    {
        bool returning; // false = request spreading out, true = request return
        db::remote_node prev_node;
        std::deque<std::vector<std::string>> node_aliases;
        std::deque<std::vector<std::pair<std::string, std::string>>> node_props;
        std::deque<std::vector<std::pair<std::string, std::string>>> edge_props;
        bool collect_nodes;
        bool collect_edges;
        std::unordered_set<node_handle_t> return_nodes;
        std::unordered_set<edge_handle_t> return_edges;

        traverse_props_params();
        ~traverse_props_params() { }
        uint64_t size() const;
        void pack(e::packer &packer) const;
        void unpack(e::unpacker &unpacker);

        // no caching
        bool search_cache() { return false; }
        cache_key_t cache_key() { return cache_key_t(); }
    };

    struct traverse_props_state: public virtual Node_State_Base
    {
        bool visited;
        uint32_t out_count; // number of requests propagated
        db::remote_node prev_node; // previous node
        std::unordered_set<node_handle_t> return_nodes;
        std::unordered_set<edge_handle_t> return_edges;

        traverse_props_state();
        ~traverse_props_state() { }
        uint64_t size() const; 
        void pack(e::packer& packer) const ;
        void unpack(e::unpacker& unpacker);
    };

    extern "C" {
    std::pair<search_type, std::vector<std::pair<db::remote_node, traverse_props_params>>>
    traverse_props_node_program(node &n,
        db::remote_node &rn,
        traverse_props_params &params,
        std::function<traverse_props_state&()> state_getter,
        std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
        cache_response<Cache_Value_Base>*);
    }
}

#endif
