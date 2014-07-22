/*
 * ===============================================================
 *    Description:  Node program to read properties of edges on a
 *                  single node
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

#ifndef weaver_node_prog_read_edges_props_program_h_
#define weaver_node_prog_read_edges_props_program_h_

#include <vector>
#include <string>

#include "db/remote_node.h"
#include "node_prog/base_classes.h"
#include "node_prog/node.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class read_edges_props_params : public Node_Parameters_Base 
    {
        public:
            std::vector<edge_handle_t> edges; // empty vector means fetch props for all edges
            std::vector<std::string> keys; // empty vector means fetch all props
            std::vector<std::pair<edge_handle_t, std::vector<std::pair<std::string, std::string>>>> edges_props;

            // no caching needed
            bool search_cache() { return false; }
            cache_key_t cache_key() { return cache_key_t(); }
            uint64_t size() const;
            void pack(e::buffer::packer& packer) const;
            void unpack(e::unpacker& unpacker);
    };

    struct read_edges_props_state : public Node_State_Base
    {
        ~read_edges_props_state() { }
        uint64_t size() const { return 0; }
        void pack(e::buffer::packer&) const { }
        void unpack(e::unpacker&) { }
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, read_edges_props_params>>>
    read_edges_props_node_program(
            node &n,
            db::element::remote_node &,
            read_edges_props_params &params,
            std::function<read_edges_props_state&()>,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, cache_key_t)>&,
            cache_response<Cache_Value_Base>*);
}

#endif
