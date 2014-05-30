/*
 * ===============================================================
 *    Description:  Dijkstra shortest path program.
 *
 *        Created:  Sunday 21 April 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef weaver_node_prog_clustering_program_h_
#define weaver_node_prog_clustering_program_h_

#include <vector>

#include "db/element/remote_node.h"
#include "node_prog/node.h"
#include "node_prog/edge.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class clustering_params : public Node_Parameters_Base 
    {
        public:
            bool _search_cache;
            uint64_t _cache_key;
            bool is_center;
            db::element::remote_node center;
            bool outgoing;
            std::vector<uint64_t> neighbors;
            double clustering_coeff;

            bool search_cache() { return _search_cache; }
            uint64_t cache_key() { return _cache_key; }
            uint64_t size() const;
            void pack(e::buffer::packer& packer) const;
            void unpack(e::unpacker& unpacker);
    };

    struct clustering_node_state : public Node_State_Base
    {
        // map from a node id to the number of neighbors who are connected to it
        std::unordered_map<uint64_t, int> neighbor_counts;
        int responses_left;

        ~clustering_node_state() { }
        uint64_t size() const;
        void pack(e::buffer::packer& packer) const;
        void unpack(e::unpacker& unpacker);
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, clustering_params>>>
    clustering_node_program(
            node &n,
            db::element::remote_node &rn,
            clustering_params &params,
            std::function<clustering_node_state&()> get_state,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            cache_response<Cache_Value_Base>*);
}

#endif
