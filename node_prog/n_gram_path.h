/*
 * ===============================================================
 *    Description:  N-gram path matching implementation.
 *
 *         Author:  Ted Yin, ted.sybil@gmail.com
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_n_gram_path_h_
#define weaver_node_prog_n_gram_path_h_

#include <string>

#include "common/property_predicate.h"
#include "db/remote_node.h"
#include "node_prog/node.h"
#include "node_prog/edge.h"
#include "node_prog/base_classes.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    /* parameter passing to a node */
    struct n_gram_path_params: public virtual Node_Parameters_Base
    {
        /* node & edge predicates */
        std::vector<predicate::prop_predicate> node_preds;
        std::vector<predicate::prop_predicate> edge_preds;

        std::unordered_map<uint32_t, uint32_t> progress;

        /* coordinator */
        db::remote_node coord;
        std::vector<node_handle_t> remaining_path;
        uint32_t step;
        bool unigram;

        n_gram_path_params();
        ~n_gram_path_params() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);

        // no caching
        bool search_cache() { return false; }
        cache_key_t cache_key() { return cache_key_t(); }
    };

    /* overall state of a node */
    struct n_gram_path_state: public virtual Node_State_Base
    {
        n_gram_path_state();
        ~n_gram_path_state() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);
    };

   std::pair<search_type, std::vector<std::pair<db::remote_node, n_gram_path_params>>>
   n_gram_path_node_program(node &n,   /* current node */
       db::remote_node &rn,                 /* current node for access remotely */
       n_gram_path_params &params,     /* passed parameters */
       std::function<n_gram_path_state&()> state_getter,   /* the function to get state */
       std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,                           /* caching thing */
       cache_response<Cache_Value_Base>*);
}

#endif
