/*
 * ===============================================================
 *    Description:  Count number of edges at a node.
 *
 *        Created:  2014-04-16 17:41:08
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_edge_count_program_h_
#define weaver_node_prog_edge_count_program_h_

#include <vector>
#include <string>

#include "db/element/remote_node.h"
#include "node_prog/base_classes.h"
#include "node_prog/node.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class edge_count_params : public virtual Node_Parameters_Base 
    {
        public:
            std::vector<std::pair<std::string, std::string>> edges_props;
            uint64_t edge_count;

        public:
            // would never need to cache 
            bool search_cache() { return false; }
            uint64_t cache_key() { return 0; }
            uint64_t size() const;
            void pack(e::buffer::packer& packer) const;
            void unpack(e::unpacker& unpacker);
    };

    struct edge_count_state : public virtual Node_State_Base
    {
        virtual ~edge_count_state() { }
        virtual uint64_t size() const { return 0; }
        virtual void pack(e::buffer::packer&) const { }
        virtual void unpack(e::unpacker&) { }
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, edge_count_params>>>
    edge_count_node_program(
            node &n,
            db::element::remote_node &,
            edge_count_params &params,
            std::function<edge_count_state&()>,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            cache_response<Cache_Value_Base>*);
}

#endif
