/*
 * ===============================================================
 *    Description:  Graph edge class 
 *
 *        Created:  Tuesday 16 October 2012 02:28:29  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 * 
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_edge_h_
#define weaver_db_edge_h_

#include <stdint.h>
#include <vector>

#include "node_prog/edge.h"
#include "node_prog/prop_list.h"
#include "node_prog/property.h"
#include "db/remote_node.h"
#include "db/element.h"
#include "db/shard_constants.h"
#include "client/datastructures.h"

namespace db
{
    class edge : public node_prog::edge
    {
        public:
            edge();
            edge(const edge_handle_t &handle, vclock_ptr_t &vclk, uint64_t remote_loc, const node_handle_t &remote_handle);
            edge(const edge_handle_t &handle, vclock_ptr_t &vclk, remote_node &rn);
            ~edge();

        public:
            element base;
            remote_node nbr; // out-neighbor for this edge
#ifdef WEAVER_CLDG
            uint32_t msg_count; // number of messages sent on this link
#endif
#ifdef WEAVER_NEW_CLDG
            uint32_t msg_count; // number of messages sent on this link
#endif
            void traverse(); // indicate that this edge was traversed; useful for migration statistics

            const remote_node& get_neighbor() { return nbr; }
            node_prog::prop_list get_properties();
            bool has_property(std::pair<std::string, std::string> &p);
            bool has_all_properties(std::vector<std::pair<std::string, std::string>> &props);
            bool has_all_predicates(std::vector<predicate::prop_predicate> &preds);
            const edge_handle_t& get_handle() const { return base.get_handle(); }
            void get_client_edge(const std::string &node, cl::edge &e);

            static edge empty_edge;
    };
}

#endif
