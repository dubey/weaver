/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_node_h_
#define weaver_node_prog_node_h_

#include <stdint.h>
#include <iostream>
#include <iterator>
#include <vector>
#include <unordered_map>

#include "common/types.h"
#include "common/property_predicate.h"
#include "node_prog/edge_list.h"
#include "client/datastructures.h"

namespace node_prog
{
    class node
    {
        public:
            virtual ~node() { }
            virtual const node_handle_t& get_handle() const = 0;
            virtual bool edge_exists(const edge_handle_t&) = 0;
            virtual edge& get_edge(const edge_handle_t&) = 0;
            virtual edge_list get_edges() = 0;
            virtual prop_list get_properties() = 0;
            virtual std::string get_property(const std::string &key) = 0;
            virtual bool has_property(std::pair<std::string, std::string> &p) = 0;
            virtual bool has_all_properties(std::vector<std::pair<std::string, std::string>> &props) = 0;
            virtual bool has_all_predicates(std::vector<predicate::prop_predicate> &preds) = 0;
            virtual bool is_alias(const node_handle_t &alias) const = 0;
            virtual void get_client_node(cl::node&, bool props, bool edges, bool aliases) = 0;
    };
}

#endif
