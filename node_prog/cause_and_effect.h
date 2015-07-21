/*
 * ===============================================================
 *    Description:  Cause and effect implementation.
 *
 *         Author:  Ted Yin, ted.sybil@gmail.com
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
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
    using path_res = std::vector<std::pair<double, std::vector<node_handle_t>>>;
    using anc_set = std::unordered_set<node_handle_t>;

    struct path_handle: public virtual Node_State_Base {
        std::vector<node_handle_t> nodes;
        std::vector<edge_handle_t> edges;
        void pop();
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);
        bool operator==(const path_handle &b) const;
    };

    /* parameter passing to a node */
    struct cause_and_effect_params: public virtual Node_Parameters_Base
    {
        node_handle_t dest;
        /* the cutoff confidence */
        double cutoff_confid;
        /* node & edge predicates */
        std::vector<predicate::prop_predicate> node_preds;
        std::vector<predicate::prop_predicate> edge_preds;

        double confidence;
        /* for checking cycles */
        anc_set ancestors;
        /* for identifying different path through a node */
        path_handle path_id;
        uint32_t path_hash;
        uint32_t prev_path_hash;
        uint32_t max_results;
        /* path results */
        path_res paths;

        /* whether the search is in returning phase */
        bool returning;
        /* the node that propagates to the current node */
        db::remote_node prev_node;

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
    struct cause_and_effect_substate: public virtual Node_State_Base
    {
        /* number of children that still have not returned the result */
        uint32_t outstanding_count;
        /* the product of confidence so far */
        double confidence;
        uint32_t prev_path_hash;
        uint32_t path_hash;
        /* all nodes on the path to root */
        path_handle path_id;
        /* path results */
        db::remote_node prev_node;
        path_res paths;
        void get_prev_substate_identifier(cause_and_effect_params &params);
        cause_and_effect_substate();
        ~cause_and_effect_substate() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);
    };

    /* overall state of a node */
    struct cause_and_effect_state: public virtual Node_State_Base
    {
        /* maps remaining path len to a finer state representation */
        std::unordered_map<uint32_t, std::vector<cause_and_effect_substate>> vmap;

        cause_and_effect_state();
        ~cause_and_effect_state() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);
        cause_and_effect_substate *get_substate(const cause_and_effect_params &params, bool create);
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
