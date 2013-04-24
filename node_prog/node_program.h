/*
 * ===============================================================
 *    Description:  Template for a node program.
 *
 *        Created:  Sunday 17 March 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __NODE_PROG__
#define __NODE_PROG__

#include <vector>
#include <map>
#include <unordered_map>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"
#include "element/remote_node.h"

#include "node_prog_type.h"
#include "dijkstra_program.h"
#include "reach_program.h"

namespace coordinator
{
    class central;
    class pending_req;
}

namespace db
{
    class graph;
}

namespace node_prog
{
    template <typename ParamsType, typename NodeStateType, typename CacheValueType>
    void node_program_runner(db::graph *G,
            typename node_prog::node_function_type<ParamsType, NodeStateType, CacheValueType>::value_type np,
            std::vector<std::pair<uint64_t, ParamsType>> &start_node_params,
            node_prog::prog_type program,
            uint64_t request_id)
    {
    }

    class node_program
    {
        public:
            virtual void unpack_and_run_db(db::graph *g, message::message &msg) = 0;
            virtual void unpack_and_start_coord(coordinator::central *server,
                    message::message &msg,
                    std::shared_ptr<coordinator::pending_req> request) = 0;

            virtual ~node_program() { }
    };

    template <typename ParamsType, typename NodeStateType, typename CacheValueType>
    class particular_node_program : public virtual node_program 
    {
        public:
            typedef typename node_function_type<ParamsType, NodeStateType, CacheValueType>::value_type func;
            func enclosed_function;
            prog_type type;

        public:
            particular_node_program(prog_type _type, func _enclosed_function)
                : enclosed_function(_enclosed_function)
                , type(_type)
            {
            }

        public:
            virtual void unpack_and_run_db(db::graph *G, message::message &msg);
            virtual void unpack_and_start_coord(coordinator::central *server, message::message &msg, std::shared_ptr<coordinator::pending_req> request);
    };
    
    std::map<prog_type, node_program*> programs = {
        {REACHABILITY, new particular_node_program<node_prog::reach_params, node_prog::reach_node_state, node_prog::reach_cache_value>(REACHABILITY, node_prog::reach_node_program)},
        {DIJKSTRA, new particular_node_program<node_prog::dijkstra_params, node_prog::dijkstra_node_state, node_prog::dijkstra_cache_value>(DIJKSTRA, node_prog::dijkstra_node_program)} 
    };
} 

#endif //__NODE_PROG__
