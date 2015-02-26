/*
 * ===============================================================
 *    Description:  Get node with all client-facing data.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_node_get_program_h_
#define weaver_node_prog_node_get_program_h_

#include <vector>
#include <string>

#include "db/remote_node.h"
#include "node_prog/base_classes.h"
#include "node_prog/node.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class node_get_params: public Node_Parameters_Base
    {
        public:
            bool props, edges, aliases;
            cl::node node;

            // no caching needed
            bool search_cache() { return false; }
            cache_key_t cache_key() { return cache_key_t(); }
            uint64_t size() const;
            void pack(e::buffer::packer& packer) const;
            void unpack(e::unpacker& unpacker);
    };

    struct node_get_state: public Node_State_Base
    {
        ~node_get_state() { }
        uint64_t size() const { return 0; }
        void pack(e::buffer::packer&) const { }
        void unpack(e::unpacker&) { }
    };

    std::pair<search_type, std::vector<std::pair<db::remote_node, node_get_params>>>
    node_get_node_program(
        node &n,
        db::remote_node &,
        node_get_params &params,
        std::function<node_get_state&()>,
        std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
            std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
        cache_response<Cache_Value_Base>*);
}

#endif
