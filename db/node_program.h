/*
 * ===============================================================
 *    Description:  something
 *
 *        Created:  Sunday 17 March 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill, dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __NODE_PROG__
#define __NODE_PROG__

#include <vector>
#include <unordered_map>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"
#include "element/remote_node.h"
#include "db/graph.h"

namespace db
{
    enum prog_type
    {
        REACHABILITY = 0,
        DIJKSTRA,
        CLUSTERING
    };

    template <typename params_type, typename node_state_type, typename cache_value_type>
    struct node_function_type
    {
        typedef std::vector<std::pair<element::remote_node, params_type>> (*value_type)(element::node&, params_type&, node_state_type&, cache_value_type&);
    };

    struct node_program{
        virtual void unpack_and_run(Graph *g, message::message &msg) = 0;
        virtual void destroy_cache_value(void *val) = 0;
        virtual ~node_program() { }
    }

    template <typename ParamsType, typename NodeStateType, typename CacheValueType>
    class particular_node_program : virtual node_program {
        typedef typename node_function_type<ParamsType, NodeStateType, CacheValueType>::value_type func;
        func enclosed_function;
        prog_type type;
    public:
        particular_node_program(prog_type _type, func _enclosed_function) :
            enclosed_function(_enclosed_function), type(_type)
        { }
        
        virtual void unpack_and_run(db::graph *G, message::message &msg) {
            // unpack some start params from msg:
            std::vector<std::pair<uint64_t, ParamsType>> params;
            uint64_t unpacked_req_id;
            node_program_runner(G, enclosed_function, params, type, unpacked_req_id);
        }

        virtual void destroy_cache_value(void *val) {
            CacheValueType *cvt = (CacheValueType *)val;
            delete cvt;
        }
    };

    std::map<prog_type, std::unique_ptr<node_program>> programs = {
        std::make_pair(DIJKSTRA, new particular_node_program<db::dijkstra_params, db::dijkstra_node_state, db::dijkstra_cache_value>(DIJKSTRA, db::dijkstra_node_program))

    };
} 

namespace std
{
    // used if we want a hash table with a prog type as the key
    template <>
    struct hash<db::prog_type>
    {
        public:
            size_t operator()(db::prog_type x) const throw() 
            {
                return hash<int>()(x);
            }
    };
}

#endif //__NODE_PROG__
