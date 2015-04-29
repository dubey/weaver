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
#include <deque>
#include <map>
#include <unordered_map>
#include <po6/threads/mutex.h>

#include "common/message.h"
#include "common/event_order.h"
#include "db/remote_node.h"
#include "node_prog/cache_response.h"
#include "node_prog/node.h"
#include "node_prog/edge.h"

#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"
#include "node_prog/pathless_reach_program.h"
#include "node_prog/clustering_program.h"
#include "node_prog/read_node_props_program.h"
#include "node_prog/read_n_edges_program.h"
#include "node_prog/edge_count_program.h"
#include "node_prog/edge_get_program.h"
#include "node_prog/node_get_program.h"
#include "node_prog/clustering_program.h"
#include "node_prog/two_neighborhood_program.h"
#include "node_prog/traverse_with_props.h"
#include "node_prog/discover_paths.h"
#include "node_prog/get_btc_block.h"

namespace coordinator
{
    class central;
    class pending_req;
    class hyper_stub;
}

namespace node_prog
{

    template <typename params_type, typename node_state_type, typename cache_value_type>
    struct node_function_type
    {
        public:
            typedef std::pair<search_type, std::vector<std::pair<db::remote_node, params_type>>> (*value_type)(
                node&, // this node
                db::remote_node&, // this remote node
                params_type&,
                std::function<node_state_type&()>,
                std::function<void(std::shared_ptr<cache_value_type>,
                    std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)> &add_cache_func,
                    cache_response<cache_value_type> *cache_response);

    };

    template <typename ParamsType, typename NodeStateType, typename CacheValueType>
    struct node_prog_running_state
    {
        private:
            /* constructs a clone without start_node_params or cache_value */
            node_prog_running_state(node_prog::prog_type ptype, uint64_t vt, std::shared_ptr<vc::vclock> vclk, uint64_t rid, uint64_t vtptr)
                : prog_type_recvd(ptype)
                , vt_id(vt)
                , req_vclock(vclk)
                , req_id(rid)
                , vt_prog_ptr(vtptr)
            { }

        public:
            node_prog::prog_type prog_type_recvd;
            uint64_t vt_id;
            std::shared_ptr<vc::vclock> req_vclock;
            uint64_t req_id;
            uint64_t vt_prog_ptr;
            std::deque<std::pair<node_handle_t, ParamsType>> start_node_params;
            std::unique_ptr<cache_response<CacheValueType>> cache_value;

            node_prog_running_state() { }
            // delete standard copy onstructors
            node_prog_running_state(const node_prog_running_state &) = delete;
            node_prog_running_state& operator=(node_prog_running_state const&) = delete;

            node_prog_running_state clone_without_start_node_params() 
            {
                return node_prog_running_state(prog_type_recvd, vt_id, req_vclock, req_id, vt_prog_ptr);
            }

            node_prog_running_state(node_prog_running_state&& copy_from)
                : prog_type_recvd(copy_from.prog_type_recvd)
                  , vt_id(copy_from.vt_id)
                  , req_vclock(copy_from.req_vclock)
                  , req_id(copy_from.req_id)
                  , vt_prog_ptr(copy_from.vt_prog_ptr)
                  , start_node_params(copy_from.start_node_params)
                  , cache_value(std::move(copy_from.cache_value))
            { }
   };

    class node_program
    {
        public:
            virtual void unpack_and_run_db(uint64_t tid, std::unique_ptr<message::message> msg, order::oracle *time_oracle) = 0;
            virtual void unpack_context_reply_db(uint64_t tid, std::unique_ptr<message::message> msg, order::oracle *time_oracle) = 0;
            virtual void unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, coordinator::hyper_stub*) = 0;

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
            virtual void unpack_and_run_db(uint64_t tid, std::unique_ptr<message::message> msg, order::oracle *time_oracle);
            virtual void unpack_context_reply_db(uint64_t tid, std::unique_ptr<message::message> msg, order::oracle *time_oracle);
            virtual void unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, coordinator::hyper_stub*);

            // delete standard copy onstructors
            particular_node_program(const particular_node_program&) = delete;
            particular_node_program& operator=(particular_node_program const&) = delete;
    };
    
    std::map<prog_type, node_program*> programs = {
        { REACHABILITY,
            new particular_node_program<reach_params, reach_node_state, reach_cache_value>(REACHABILITY, node_prog::reach_node_program) },
        { PATHLESS_REACHABILITY,
            new particular_node_program<pathless_reach_params, pathless_reach_node_state, Cache_Value_Base>(PATHLESS_REACHABILITY, node_prog::pathless_reach_node_program) },
        { CLUSTERING,
            new particular_node_program<clustering_params, clustering_node_state, Cache_Value_Base>(CLUSTERING, node_prog::clustering_node_program) },
        { TWO_NEIGHBORHOOD,
            new particular_node_program<two_neighborhood_params, two_neighborhood_state, two_neighborhood_cache_value>(TWO_NEIGHBORHOOD, node_prog::two_neighborhood_node_program) },
        { READ_NODE_PROPS,
            new particular_node_program<read_node_props_params, read_node_props_state, Cache_Value_Base>(READ_NODE_PROPS, node_prog::read_node_props_node_program) },
        { READ_N_EDGES,
            new particular_node_program<read_n_edges_params, read_n_edges_state, Cache_Value_Base>(READ_N_EDGES, node_prog::read_n_edges_node_program) },
        { EDGE_COUNT,
            new particular_node_program<edge_count_params, edge_count_state, Cache_Value_Base>(EDGE_COUNT, node_prog::edge_count_node_program) },
        { EDGE_GET,
            new particular_node_program<edge_get_params, edge_get_state, Cache_Value_Base>(EDGE_GET, node_prog::edge_get_node_program) },
        { NODE_GET,
            new particular_node_program<node_get_params, node_get_state, Cache_Value_Base>(NODE_GET, node_prog::node_get_node_program) },
        { TRAVERSE_PROPS,
            new particular_node_program<traverse_props_params, traverse_props_state, Cache_Value_Base>(TRAVERSE_PROPS, node_prog::traverse_props_node_program) },
        { DISCOVER_PATHS,
            new particular_node_program<discover_paths_params, discover_paths_state, Cache_Value_Base>(DISCOVER_PATHS, node_prog::discover_paths_node_program) },
        { GET_BTC_BLOCK,
            new particular_node_program<get_btc_block_params, get_btc_block_state, Cache_Value_Base>(GET_BTC_BLOCK, node_prog::get_btc_block_node_program) },
    };
}

#endif
