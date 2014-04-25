/*
 * ===============================================================
 *    Description:  Graph query cachine classes
 *
 *        Created:  Tuesday 18 November 2013 02:28:29  EDT
 *
 *         Author:  Gregory D. Hill, gdh39@cornell.edu
 * 
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_program_cache_h_
#define weaver_db_program_cache_h_

#include <stdint.h>
#include <vector>

#include "common/weaver_constants.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/base_classes.h"
#include "node_prog/edge_list.h"
#include "node_prog/cache_response.h"
#include "db/element/node.h"
#include "db/element/edge.h"
#include "db/element/remote_node.h"


namespace node_prog
{
    struct edge_cache_context
    {
        uint64_t edge_handle;
        db::element::remote_node nbr;

        edge_cache_context() {};
        edge_cache_context(uint64_t handle, db::element::remote_node &nbr) : edge_handle(handle), nbr(nbr) {};

        std::vector<node_prog::property> props_added;
        std::vector<node_prog::property> props_deleted;
    };

    struct node_cache_context
    {
        db::element::remote_node node;

        bool node_deleted;

        node_cache_context() {};
        node_cache_context(uint64_t loc, uint64_t id, bool deleted) : node(loc, id), node_deleted(deleted) {};

        std::vector<node_prog::property> props_added;
        std::vector<node_prog::property> props_deleted;

        std::vector<edge_cache_context> edges_added;
        std::vector<edge_cache_context> edges_modified;
        std::vector<edge_cache_context> edges_deleted;
    };
}

namespace db
{
namespace caching
{
    class program_cache 
    {
        public:
        std::unordered_map<uint64_t, std::tuple<std::shared_ptr<node_prog::Cache_Value_Base>, std::shared_ptr<vc::vclock>, std::shared_ptr<std::vector<db::element::remote_node>>>> cache;

        void add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<vc::vclock>& vc, std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
                std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key);

        program_cache() : cache(MAX_CACHE_ENTRIES) {}; // reserve size of max cache

        // delete standard copy onstructors
        program_cache (const program_cache &) = delete;
        program_cache& operator=(program_cache const&) = delete;
    };

    inline void
    program_cache :: add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<vc::vclock>& vc, std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
            std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key)
    {
        // clear oldest entry if cache is full
        if (MAX_CACHE_ENTRIES > 0 && cache.size() >= MAX_CACHE_ENTRIES) {
            vc::vclock& oldest = *vc;
            uint64_t key_to_del = key;
            for (auto& kvpair : cache) {
                vc::vclock& to_cmp = *std::get<1>(kvpair.second);
                // don't talk to kronos just pick one to delete
                if (order::compare_two_clocks(to_cmp.clock, oldest.clock) <= 0) {
                    key_to_del = kvpair.first;
                    oldest = to_cmp;
                }
            }
            cache.erase(key_to_del);
        }
        
        UNUSED(ptype); // TODO: use prog_type
        cache.emplace(key, std::make_tuple(cache_value, vc, watch_set));
    }

    template <typename CacheValueType>
    struct cache_response : node_prog::cache_response<CacheValueType>
    {
        private:
            program_cache &from;
            uint64_t key;

        public:
            std::shared_ptr<CacheValueType> value;
            std::shared_ptr<std::vector<db::element::remote_node>> watch_set;
            std::vector<node_prog::node_cache_context> context;

            cache_response(program_cache &came_from, uint64_t key_used, std::shared_ptr<node_prog::Cache_Value_Base> &val,
                    std::shared_ptr<std::vector<db::element::remote_node>> watch_set_used)
                : from(came_from)
                  , key(key_used)
                  , watch_set(watch_set_used)
                  { value = std::shared_ptr<CacheValueType>(std::dynamic_pointer_cast<CacheValueType>(val)); };

            // delete standard copy onstructors
            cache_response (const cache_response &) = delete;
            cache_response& operator=(cache_response const&) = delete;

            void invalidate() { from.cache.erase(key); };

            std::shared_ptr<CacheValueType> get_value() { return value; }
            std::shared_ptr<std::vector<db::element::remote_node>> get_watch_set() { return watch_set; };
            std::vector<node_prog::node_cache_context> &get_context() { return context; };
    };
}
}


#endif // __CACHING__
