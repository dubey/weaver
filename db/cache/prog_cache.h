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

    struct cache_response
    {
        public:
            std::shared_ptr<node_prog::Cache_Value_Base> value;
            std::vector<std::pair<db::element::remote_node, node_cache_context>> context;
            void invalidate();
//            ~cache_response(); // XXX mem leak?
    };


    class program_cache 
    {
        public:
        std::unordered_map<uint64_t, std::tuple<std::shared_ptr<node_prog::Cache_Value_Base>, vc::vclock, std::shared_ptr<std::vector<db::element::remote_node>>>> cache;

        void add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<node_prog::Cache_Value_Base> cache_value, std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key, vc::vclock& vc); // TODO shared_ptr for vclock
        //std::unique_ptr<cache_response> get_cached_value(node_prog::prog_type& ptype, uint64_t key, vc::vclock& vc);
    };

    inline void
    program_cache :: add_cache_value(node_prog::prog_type& ptype, std::shared_ptr<node_prog::Cache_Value_Base> cache_value, std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t key, vc::vclock& vc)
    {
        // TODO: use prog_type
        cache.emplace(key, std::make_tuple(cache_value, vc, watch_set));
        //WDEBUG << "OMG WE EMPLACED" << std::endl;
    }
}
}

#endif // __CACHING__
