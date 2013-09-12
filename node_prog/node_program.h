/*
 * ===============================================================
 *    Description:  Template for a particular node program.
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
#include "db/element/node.h"
#include "db/element/edge.h"
#include "db/element/remote_node.h"

#include "node_prog_type.h"
//#include "dijkstra_program.h"
#include "reach_program.h"
//#include "clustering_program.h"

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
    template <typename ParamsType, typename NodeStateType>
    void node_program_runner(typename node_prog::node_function_type<ParamsType,
            NodeStateType>::value_type np,
            std::vector<std::pair<uint64_t, ParamsType>> &start_node_params,
            node_prog::prog_type program,
            uint64_t request_id)
    { }

    class node_program
    {
        public:
            virtual void unpack_and_run_db(message::message &msg) = 0;
            virtual void unpack_and_start_coord(std::shared_ptr<coordinator::pending_req> request) = 0;

            virtual ~node_program() { }
    };

    template <typename ParamsType, typename NodeStateType>
    class particular_node_program : public virtual node_program 
    {
        public:
            typedef typename node_function_type<ParamsType, NodeStateType>::value_type func;
            //typedef typename deleted_node_function_type<ParamsType, NodeStateType>::value_type dfunc; TODO: NEEDED?
            func enclosed_node_prog_func;
            //dfunc enclosed_node_deleted_func;
            prog_type type;

        public:
            particular_node_program(prog_type _type, func prog_func)//, dfunc del_func)
                : enclosed_node_prog_func(prog_func)
              //  , enclosed_node_deleted_func(del_func)
                , type(_type)
            {
            }

        public:
            virtual void unpack_and_run_db(message::message &msg);
            virtual void unpack_and_start_coord(std::shared_ptr<coordinator::pending_req> request);
    };
    
    std::map<prog_type, node_program*> programs = {
        { REACHABILITY,
          new particular_node_program<node_prog::reach_params, node_prog::reach_node_state>(REACHABILITY, node_prog::reach_node_program/*,
                node_prog::reach_node_deleted_program*/) }};/*,
        { DIJKSTRA,
          new particular_node_program<node_prog::dijkstra_params, node_prog::dijkstra_node_state,
                node_prog::dijkstra_cache_value>(DIJKSTRA, node_prog::dijkstra_node_program,
                dijkstra_node_deleted_program,
                INVALIDATE_TAIL) },
        { CLUSTERING,
          new particular_node_program<node_prog::clustering_params, node_prog::clustering_node_state,
                node_prog::clustering_cache_value>(CLUSTERING, node_prog::clustering_node_program,
                clustering_node_deleted_program,
                INVALIDATE_BOTH) }
    };
    */

}
#endif //__NODE_PROG__
