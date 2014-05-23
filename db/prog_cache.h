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
#include "db/element/node.h"
#include "db/element/edge.h"
#include "db/element/remote_node.h"

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
}
}

#endif // __CACHING__
