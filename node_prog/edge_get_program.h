/*
 * ===============================================================
 *    Description:  Get all edges from the node to a particular
 *                  vertex which have specified properties.
 *
 *        Created:  2014-04-17 11:33:57
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_edge_get_program_h_
#define weaver_node_prog_edge_get_program_h_

#include <vector>
#include <string>

#include "db/remote_node.h"
#include "node_prog/base_classes.h"
#include "node_prog/node.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class edge_get_params : public Node_Parameters_Base 
    {
        public:
            node_handle_t nbr_handle;
            std::vector<std::pair<std::string, std::string>> edges_props;
            std::vector<edge_handle_t> return_edges;

            // would never need to cache
            bool search_cache() { return false; }
            cache_key_t cache_key() { return cache_key_t(); }
            uint64_t size() const;
            void pack(e::buffer::packer& packer) const;
            void unpack(e::unpacker& unpacker);
    };

    struct edge_get_state : public Node_State_Base
    {
        ~edge_get_state() { }
        uint64_t size() const { return 0; }
        void pack(e::buffer::packer&) const { }
        void unpack(e::unpacker&) { }
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, edge_get_params>>>
    edge_get_node_program(
            node &n,
            db::element::remote_node &,
            edge_get_params &params,
            std::function<edge_get_state&()>,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, cache_key_t)>&,
            cache_response<Cache_Value_Base>*);
}

#endif
