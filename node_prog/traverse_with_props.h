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
#include "node_prog/node_prog_type.h"
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
        uint64_t size(void*) const;
        void pack(e::packer &packer, void*) const;
        void unpack(e::unpacker &unpacker, void*);

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
        uint64_t size(void*) const; 
        void pack(e::packer& packer, void*) const ;
        void unpack(e::unpacker& unpacker, void*);
    };

    extern "C" {
        std::shared_ptr<Node_Parameters_Base> ctor_prog_param();
        std::shared_ptr<Node_State_Base> ctor_prog_state();
        
        uint64_t param_size(const Node_Parameters_Base&, void*);
        void param_pack(const Node_Parameters_Base&, e::packer&, void*);
        void param_unpack(Node_Parameters_Base&, e::unpacker&, void*);

        uint64_t state_size(const Node_State_Base&, void*);
        void state_pack(const Node_State_Base&, e::packer&, void*);
        void state_unpack(Node_State_Base&, e::unpacker&, void*);

        std::pair<search_type, std::vector<std::pair<db::remote_node, std::shared_ptr<Node_Parameters_Base>>>>
        node_program(node &n,
            db::remote_node &rn,
            std::shared_ptr<Node_Parameters_Base> param_ptr,
            std::function<Node_State_Base&()> state_getter);
    }
}

#endif
