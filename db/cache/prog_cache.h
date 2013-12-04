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

#ifndef __CACHING__
#define __CACHING__

#include <stdint.h>
#include <vector>
//#include <po6/net/location.h>

#include "node_prog/node_prog_type.h"
#include "node_prog/base_classes.h"
#include "db/element/node.h"
#include "db/element/remote_node.h"
#include "db/element/edge.h"

namespace db
{
namespace caching
{

    struct node_cache_context
    {
        bool node_deleted;
        std::vector<db::element::edge> edges_added;
        std::vector<db::element::edge> edges_deleted;
    };

    class program_cache 
    {
        uint64_t uid = 0;
        public:
        std::unordered_map<uint64_t, std::tuple<std::shared_ptr<node_prog::Cache_Value_Base>, vc::vclock, std::shared_ptr<std::vector<db::element::remote_node>>>> cache;

        void add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<node_prog::Cache_Value_Base> cache_value, std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key, vc::vclock& vc); // TODO shared_ptr for vclock
        uint64_t gen_uid();
        //std::unique_ptr<cache_response> get_cached_value(node_prog::prog_type& ptype, uint64_t key, vc::vclock& vc);
    };

    inline void
    program_cache :: add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<node_prog::Cache_Value_Base> cache_value, std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key, vc::vclock& vc)
    {
        // TODO: use prog_type
        cache.emplace(key, std::make_tuple(cache_value, vc, watch_set));
        //WDEBUG << "OMG WE EMPLACED, cache size "<< cache.size() << std::endl;
        if (cache.size() > MAX_CACHE_ENTRIES){
            WDEBUG << "cache is full!" << std::endl;
            vc::vclock& oldest = vc;
            uint64_t key_to_del = key;
            for (auto& kvpair : cache) 
            {
                vc::vclock& to_cmp = std::get<1>(kvpair.second);
                if (order::compare_two_vts(to_cmp, oldest) == 0){
                    key_to_del =kvpair.first;
                    oldest = to_cmp;
                }
            }
            cache.erase(key_to_del);
        }
    }
    inline uint64_t
    program_cache :: gen_uid(){
        return ++uid;
    }

    struct cache_response
    {
        private:
            program_cache &from;
            uint64_t key;

        public:
            std::shared_ptr<node_prog::Cache_Value_Base> value;
            std::vector<std::pair<db::element::remote_node, node_cache_context>> context;
            void invalidate();
            cache_response(program_cache &came_from, uint64_t key_used) : from(came_from), key(key_used) {};
    };

    inline void
    cache_response :: invalidate(){
        from.cache.erase(key);
    }
}
}

#endif // __CACHING__
