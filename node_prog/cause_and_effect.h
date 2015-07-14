/*
 * ===============================================================
 *    Description:  Discover all paths between two vertices
 *                  predicated on max path len, node properties,
 *                  edge properties.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_cause_and_effect_h_
#define weaver_node_prog_cause_and_effect_h_

#include <string>

#include "common/property_predicate.h"
#include "db/remote_node.h"
#include "node_prog/node.h"
#include "node_prog/edge.h"
#include "node_prog/base_classes.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    using edge_set = std::unordered_set<cl::edge, cl::hash_edge, cl::equals_edge>;

    /* parameter passing to a node */
    struct cause_and_effect_params: public virtual Node_Parameters_Base
    {
        node_handle_t dest;
        /* the product of confidence along the path */
        double path_confid;
        double prev_confid;
        /* the cutoff confidence */
        double cutoff_confid;
        /* node & edge predicates */
        std::vector<predicate::prop_predicate> node_preds;
        std::vector<predicate::prop_predicate> edge_preds;
        /* subgraph results */
        std::unordered_map<node_handle_t, std::vector<cl::edge>> paths;
        /* whether the search is in returning phase */
        bool returning;
        /* the node that propagates to the current node */
        db::remote_node prev_node;
        node_handle_t src;
        /* all nodes on the path to root */
        std::unordered_set<node_handle_t> path_ancestors; // prevent cycles

        cause_and_effect_params();
        ~cause_and_effect_params() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);

        // no caching
        bool search_cache() { return false; }
        cache_key_t cache_key() { return cache_key_t(); }
    };

    /* finer state given remaining path length */
    struct cdp_len_state: public virtual Node_State_Base
    {
        /* number of children that still have not returned the result */
        uint32_t outstanding_count;
        /* previous nodes */
        std::vector<db::remote_node> prev_nodes;
        std::vector<double> prev_confids;
        /* subgraph results */
        std::unordered_map<node_handle_t, edge_set> paths;

        cdp_len_state();
        ~cdp_len_state() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);
    };

    /* overall state of a node */
    struct cause_and_effect_state: public virtual Node_State_Base
    {
        /* maps remaining path len to a finer state representation */
        std::unordered_map<uint32_t, cdp_len_state> vmap;
        uint32_t max_path_len;

        cause_and_effect_state();
        ~cause_and_effect_state() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);
    };

   std::pair<search_type, std::vector<std::pair<db::remote_node, cause_and_effect_params>>>
   cause_and_effect_node_program(node &n,   /* current node */
       db::remote_node &rn,                 /* current node for access remotely */
       cause_and_effect_params &params,     /* passed parameters */
       std::function<cause_and_effect_state&()> state_getter,   /* the function to get state */
       std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,                           /* caching thing */
       cache_response<Cache_Value_Base>*);
}

#endif
