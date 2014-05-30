/*
 * ===============================================================
 *    Description:  Program to read the 'top' n edges, used to
 *                  simulate range queries for soc. net. workload.
 *
 *        Created:  2014-04-15 21:30:10
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_read_n_edges_program_h_
#define weaver_node_prog_read_n_edges_program_h_

#include <vector>
#include <string>

#include "db/remote_node.h"
#include "node_prog/base_classes.h"
#include "node_prog/node.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class read_n_edges_params : public Node_Parameters_Base 
    {
        public:
            uint64_t num_edges;
            std::vector<std::pair<std::string, std::string>> edges_props;
            std::vector<uint64_t> return_edges;

        public:
            // no caching needed
            bool search_cache() { return false; }
            uint64_t cache_key() { return 0; }
            uint64_t size() const;
            void pack(e::buffer::packer& packer) const;
            void unpack(e::unpacker& unpacker);
    };

    struct read_n_edges_state : public Node_State_Base
    {
        ~read_n_edges_state() { }
        uint64_t size() const { return 0; }
        void pack(e::buffer::packer&) const { }
        void unpack(e::unpacker&) { }
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, read_n_edges_params>>>
    read_n_edges_node_program(
        node &n,
        db::element::remote_node &,
        read_n_edges_params &params,
        std::function<read_n_edges_state&()>,
        std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
            std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
        cache_response<Cache_Value_Base>*);
}

#endif
