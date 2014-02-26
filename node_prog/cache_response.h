/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __CACHE_RESPONSE__
#define __CACHE_RESPONSE__

#include <stdint.h>
#include <iostream>
#include <iterator>
#include <vector>
#include <unordered_map>

#include "base_classes.h"
#include "edge_list.h"

namespace node_prog
{
    class node_cache_context;

    template <typename CacheValueType>
    class cache_response
    {
        public:
           virtual std::shared_ptr<CacheValueType> get_value() = 0;
           virtual std::shared_ptr<std::vector<db::element::remote_node>> get_watch_set() = 0;
           virtual std::vector<node_prog::node_cache_context> &get_context() = 0;
           virtual void invalidate() = 0;
    };
}

#endif
