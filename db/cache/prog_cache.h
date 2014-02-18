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
    struct node_cache_context
    {
        public:
            bool node_deleted_internal;
            std::vector<db::element::edge> edges_added_internal;
            std::vector<db::element::edge> edges_deleted_internal;
            std::shared_ptr<vc::vclock> &cur_time;

            node_cache_context(std::shared_ptr<vc::vclock> &time) : cur_time(time) {}; 

            // delete standard copy onstructors TODO
            //node_cache_context(const node_cache_context&) = delete;
            //node_cache_context& operator=(node_cache_context const&) = delete;

            bool node_deleted() { return node_deleted_internal; };

            node_prog::edge_list<std::vector<db::element::edge>, node_prog::vector_edge_iter> edges_added()
            {
                return node_prog::edge_list
                    <std::vector<db::element::edge>, node_prog::vector_edge_iter>
                    (edges_added_internal, cur_time);
            };

            node_prog::edge_list<std::vector<db::element::edge>, node_prog::vector_edge_iter> edges_deleted()
            {
                return node_prog::edge_list
                    <std::vector<db::element::edge>, node_prog::vector_edge_iter>
                    (edges_deleted_internal, cur_time);
            };
    };

}
namespace db
{
namespace caching
{
    class program_cache 
    {
        uint64_t uid = 0;
        public:
        std::unordered_map<uint64_t, std::tuple<std::shared_ptr<node_prog::Cache_Value_Base>, std::shared_ptr<vc::vclock>, std::shared_ptr<std::vector<db::element::remote_node>>>> cache;

        void add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
                std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key, std::shared_ptr<vc::vclock>& vc);
        uint64_t gen_uid();

        program_cache() : cache(MAX_CACHE_ENTRIES) {}; // reserve size of max cache

        // delete standard copy onstructors
        program_cache (const program_cache &) = delete;
        program_cache& operator=(program_cache const&) = delete;
    };

    inline void
    program_cache :: add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
            std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key, std::shared_ptr<vc::vclock>& vc)
    {
        // clear oldest entry if cache is full
        if (MAX_CACHE_ENTRIES > 0 && cache.size() >= MAX_CACHE_ENTRIES){
            vc::vclock& oldest = *vc;
            uint64_t key_to_del = key;
            for (auto& kvpair : cache) 
            {
                vc::vclock& to_cmp = *std::get<1>(kvpair.second);
                if (order::compare_two_clocks(to_cmp.clock, oldest.clock) <= 0){ // don't talk to kronos just pick one to delete
                    key_to_del = kvpair.first;
                    oldest = to_cmp;
                }
            }
            cache.erase(key_to_del);
        }
        
        UNUSED(ptype); // TODO: use prog_type
        cache.emplace(key, std::make_tuple(cache_value, vc, watch_set));
    }
    inline uint64_t
    program_cache :: gen_uid(){
        return ++uid;
    }

    struct cache_response : node_prog::cache_response
    {
        private:
            program_cache &from;
            uint64_t key;

        public:
            std::shared_ptr<node_prog::Cache_Value_Base> value;
            std::shared_ptr<std::vector<db::element::remote_node>> watch_set;
            std::vector<std::pair<db::element::remote_node, node_prog::node_cache_context>> context;

            cache_response(program_cache &came_from, uint64_t key_used, std::shared_ptr<node_prog::Cache_Value_Base> &val,
                    std::shared_ptr<std::vector<db::element::remote_node>> watch_set_used)
                : from(came_from)
                  , key(key_used)
                  , value(val)
                  , watch_set(watch_set_used) {};

            // delete standard copy onstructors
            cache_response (const cache_response &) = delete;
            cache_response& operator=(cache_response const&) = delete;

            void invalidate() { from.cache.erase(key); };

            std::shared_ptr<node_prog::Cache_Value_Base> get_cached_value() { return value; }
            std::shared_ptr<std::vector<db::element::remote_node>> get_watch_set() { return watch_set; };
            std::vector<std::pair<db::element::remote_node, node_prog::node_cache_context>> &get_cache_context() { return context; };
    };
}
}


#endif // __CACHING__
