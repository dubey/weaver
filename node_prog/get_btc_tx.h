/*
 * ===============================================================
 *    Description:  Node program to get a BTC tx
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_get_btc_tx_h_
#define weaver_node_prog_get_btc_tx_h_

#include <string>

#include "common/property_predicate.h"
#include "db/remote_node.h"
#include "node_prog/node.h"
#include "node_prog/edge.h"
#include "node_prog/base_classes.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    using btc_tx_t = std::pair<std::vector<cl::edge>, std::vector<cl::edge>>;
    struct get_btc_tx_params: public virtual Node_Parameters_Base
    {
        node_handle_t tx_handle;
        cl::node ret_node;

        bool returning;
        std::unordered_map<edge_handle_t, node_handle_t> consumed_map;
        db::remote_node tx_rn;

        get_btc_tx_params();
        ~get_btc_tx_params() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);

        // no caching
        bool search_cache() { return false; }
        cache_key_t cache_key() { return cache_key_t(); }
    };

    struct get_btc_tx_state: public virtual Node_State_Base
    {
        uint32_t outstanding_count;
        std::unordered_map<edge_handle_t, node_handle_t> consumed_map;

        get_btc_tx_state();
        ~get_btc_tx_state() { }
        uint64_t size() const;
        void pack(e::packer&) const;
        void unpack(e::unpacker&);
    };

   std::pair<search_type, std::vector<std::pair<db::remote_node, get_btc_tx_params>>>
   get_btc_tx_node_program(node &n,
       db::remote_node &rn,
       get_btc_tx_params &params,
       std::function<get_btc_tx_state&()> state_getter,
       std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
       cache_response<Cache_Value_Base>*);
}

#endif
