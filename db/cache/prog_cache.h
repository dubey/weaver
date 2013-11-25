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
#include "db/element/remote_node.h"
#include "db/element/edge.h"

namespace db
{
namespace caching
{

    class node_cache_context
    {
        uint64_t node_handle;
        bool node_deleted();
        const std::vector<db::element::edge>& edges_added();
        const std::vector<db::element::edge>& edges_deleted();
        //cache_response(std::tuple<std::shared_ptr<node_prog::Packable_Deletable>, vc::vclock, std::shared_ptr<std::vector<db::element::remote_node>>> cache_val);
        //~cache_response();
    };

    class cache_response
    {
        std::vector<node_cache_context> context;
        public:
            const node_prog::Packable_Deletable& value;
            const std::vector<node_cache_context>& get_context();
            void invalidate();
            cache_response(std::tuple<std::shared_ptr<node_prog::Packable_Deletable>, vc::vclock, std::shared_ptr<std::vector<db::element::remote_node>>>& cache_val);
            ~cache_response();
    };

    cache_response :: cache_response(
            std::tuple<std::shared_ptr<node_prog::Packable_Deletable>, vc::vclock, std::shared_ptr<std::vector<db::element::remote_node>>>& cache_val)
        : value(*std::get<0>(cache_val))
    {
        vc::vclock& cache_time = std::get<1>(cache_val);
        for(auto& node : *std::get<2>(cache_val)){
            //context
        }

    }

    class program_cache 
    {
        std::unordered_map<uint64_t, std::tuple<std::shared_ptr<node_prog::Packable_Deletable>, vc::vclock, std::shared_ptr<std::vector<db::element::remote_node>>>> cache;
        public:
        void add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<node_prog::Packable_Deletable> cache_value, std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key, vc::vclock& vc); // TODO shared_ptr for vclock
        std::unique_ptr<cache_response> get_cached_value(node_prog::prog_type& ptype, uint64_t key, vc::vclock& vc);
    };

    inline void
    program_cache :: add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<node_prog::Packable_Deletable> cache_value, std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key, vc::vclock& vc)
    {
        // TODO: use prog_type
        cache.emplace(key, std::make_tuple(cache_value, vc, watch_set));
    }

    inline std::unique_ptr<cache_response>
    program_cache ::  get_cached_value(node_prog::prog_type& ptype, uint64_t key, vc::vclock& vc){
        auto ret_iterator = cache.find(key);
        if (ret_iterator == cache.end()){
            std::unique_ptr<cache_response> ret(nullptr);
            return std::move(ret);
        }
        std::unique_ptr<cache_response> ret(new cache_response(ret_iterator->second));
        return std::move(ret);
    }
}
}

#endif // __CACHING__
