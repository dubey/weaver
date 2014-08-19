/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_cache_response_h_
#define weaver_node_prog_cache_response_h_

#include <stdint.h>
#include <iostream>
#include <iterator>
#include <vector>
#include <unordered_map>

#include "common/types.h"
#include "node_prog/base_classes.h"
#include "node_prog/property.h"
#include "db/remote_node.h"
#include "db/cache_entry.h"

namespace node_prog
{
    struct edge_cache_context
    {
        edge_handle_t edge_handle;
        db::element::remote_node nbr;

        edge_cache_context() { }
        edge_cache_context(const edge_handle_t &handle, db::element::remote_node &nbr) : edge_handle(handle), nbr(nbr) { }

        std::vector<property> props_added;
        std::vector<property> props_deleted;
    };

    struct node_cache_context
    {
        db::element::remote_node node;

        bool node_deleted;

        node_cache_context() { }
        node_cache_context(uint64_t loc, const node_handle_t &handle, bool deleted) : node(loc, handle), node_deleted(deleted) { }

        std::vector<property> props_added;
        std::vector<property> props_deleted;

        std::vector<edge_cache_context> edges_added;
        std::vector<edge_cache_context> edges_modified;
        std::vector<edge_cache_context> edges_deleted;
    };

    template <typename CacheValueType>
    class cache_response
    {
        private:
            std::unordered_map<cache_key_t, db::cache_entry> &from_cache;
            cache_key_t key;
            std::shared_ptr<CacheValueType> value;
            std::shared_ptr<std::vector<db::element::remote_node>> watch_set;
            std::vector<node_cache_context> context;

        public:
            cache_response(std::unordered_map<cache_key_t, db::cache_entry> &came_from,
                cache_key_t key_used,
                std::shared_ptr<Cache_Value_Base> val,
                std::shared_ptr<std::vector<db::element::remote_node>> watch_set_used)
                : from_cache(came_from)
                , key(key_used)
                , watch_set(watch_set_used)
          {
              value = std::shared_ptr<CacheValueType>(std::dynamic_pointer_cast<CacheValueType>(val));
          }

            // delete standard copy onstructors
            cache_response (const cache_response &) = delete;
            cache_response& operator=(cache_response const&) = delete;

            std::shared_ptr<CacheValueType> get_value() { return value; }
            std::shared_ptr<std::vector<db::element::remote_node>> get_watch_set() { return watch_set; }
            std::vector<node_cache_context> &get_context() { return context; }
            void invalidate() { from_cache.erase(key); }
    };
}

#endif
