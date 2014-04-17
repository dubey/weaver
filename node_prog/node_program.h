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

#ifndef weaver_node_prog_node_program_h_
#define weaver_node_prog_node_program_h_

#include <vector>
#include <map>
#include <unordered_map>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "cache_response.h"
#include "node.h"
#include "edge.h"
#include "db/element/remote_node.h"

#include "node_prog_type.h"
#include "reach_program.h"
#include "clustering_program.h"
#include "dijkstra_program.h"
#include "read_node_props_program.h"
#include "read_edges_props_program.h"
#include "read_n_edges_program.h"
#include "edge_count_program.h"
#include "edge_get_program.h"

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

    template <typename params_type, typename node_state_type, typename cache_value_type>
    struct node_function_type
    {
        public:
            typedef std::vector<std::pair<db::element::remote_node, params_type>> (*value_type)(
                node&, // this node
                db::element::remote_node&, // this remote node
                params_type&,
                std::function<node_state_type&()>,
                std::function<void(std::shared_ptr<cache_value_type>,
                    std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
                    cache_response<cache_value_type> *cache_response);

    };

    template <typename ParamsType, typename NodeStateType, typename CacheValueType>
        struct node_prog_running_state //: public virtual node_prog::Packable XXX can't get making this packable to work
    {
        private:
        /* constructs a clone without start_node_params or cache_value */
        node_prog_running_state(node_prog::prog_type _prog_type_recvd, bool _global_req, uint64_t _vt_id, std::shared_ptr<vc::vclock> _req_vclock, uint64_t _req_id, uint64_t _prev)
            : prog_type_recvd(_prog_type_recvd)
              , global_req(_global_req)
              , vt_id(_vt_id)
              , req_vclock(_req_vclock)
              , req_id(_req_id)
              , prev_server(_prev)
        { };
        public:
        node_prog::prog_type prog_type_recvd;
        bool global_req;
        uint64_t vt_id;
        std::shared_ptr<vc::vclock> req_vclock;
        uint64_t req_id;
        uint64_t prev_server;
        std::vector<std::pair<uint64_t, ParamsType>> start_node_params;
        std::unique_ptr<db::caching::cache_response<CacheValueType>> cache_value; // XXX unique ptr needed?

        node_prog_running_state() {};
        // delete standard copy onstructors
        node_prog_running_state(const node_prog_running_state &) = delete;
        node_prog_running_state& operator=(node_prog_running_state const&) = delete;

        node_prog_running_state clone_without_start_node_params() 
        {
            return node_prog_running_state(prog_type_recvd, global_req, vt_id, req_vclock, req_id, prev_server);
        }

        node_prog_running_state(node_prog_running_state&& copy_from)
            : prog_type_recvd(copy_from.prog_type_recvd)
              , global_req(copy_from.global_req)
              , vt_id(copy_from.vt_id)
              , req_vclock(copy_from.req_vclock)
              , req_id(copy_from.req_id)
              , prev_server(copy_from.prev_server)
              , start_node_params(copy_from.start_node_params)
              , cache_value(std::move(copy_from.cache_value)){};
   };

    class node_program
    {
        public:
            virtual void unpack_and_run_db(std::unique_ptr<message::message> msg) = 0;
            virtual void unpack_context_reply_db(std::unique_ptr<message::message> msg) = 0;
            virtual void unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, int tid) = 0;

            virtual ~node_program() { }
    };

    template <typename ParamsType, typename NodeStateType, typename CacheValueType>
    class particular_node_program : public virtual node_program 
    {
        public:
            typedef typename node_function_type<ParamsType, NodeStateType, CacheValueType>::value_type func;
            func enclosed_node_prog_func;
            prog_type type;

        public:
            particular_node_program(prog_type _type, func prog_func)
                : enclosed_node_prog_func(prog_func)
                , type(_type)
            {
                static_assert(std::is_base_of<Node_Parameters_Base, ParamsType>::value, "Params type must be derived from Node_Parameters_Base");
                static_assert(std::is_base_of<Node_State_Base, NodeStateType>::value, "State type must be derived from Node_State_Base");
                static_assert(std::is_base_of<Cache_Value_Base, CacheValueType>::value, "Cache value type must be derived from Cache_Value_Base");

            }

        public:
            virtual void unpack_and_run_db(std::unique_ptr<message::message> msg);
            virtual void unpack_context_reply_db(std::unique_ptr<message::message> msg);
            virtual void unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, int tid);

            // delete standard copy onstructors
            particular_node_program(const particular_node_program&) = delete;
            particular_node_program& operator=(particular_node_program const&) = delete;
    };
    
    std::map<prog_type, node_program*> programs = {
        { REACHABILITY,
            new particular_node_program<reach_params, reach_node_state, reach_cache_value>(REACHABILITY, node_prog::reach_node_program) },
        { DIJKSTRA,
            new particular_node_program<dijkstra_params, dijkstra_node_state, Cache_Value_Base>(DIJKSTRA, node_prog::dijkstra_node_program) },
        { CLUSTERING,
            new particular_node_program<clustering_params, clustering_node_state, Cache_Value_Base>(CLUSTERING, node_prog::clustering_node_program) },
        { READ_NODE_PROPS,
            new particular_node_program<read_node_props_params, read_node_props_state, Cache_Value_Base>(READ_NODE_PROPS, node_prog::read_node_props_node_program) },
        { READ_EDGES_PROPS,
            new particular_node_program<read_edges_props_params, read_edges_props_state, Cache_Value_Base>(READ_EDGES_PROPS, node_prog::read_edges_props_node_program) },
        { READ_N_EDGES,
            new particular_node_program<read_n_edges_params, read_n_edges_state, Cache_Value_Base>(READ_N_EDGES, node_prog::read_n_edges_node_program) },
        { EDGE_COUNT,
            new particular_node_program<edge_count_params, edge_count_state, Cache_Value_Base>(EDGE_COUNT, node_prog::edge_count_node_program) },
        { EDGE_GET,
            new particular_node_program<edge_get_params, edge_get_state, Cache_Value_Base>(EDGE_GET, node_prog::edge_get_node_program) },
    };
}
#endif //__NODE_PROG__
