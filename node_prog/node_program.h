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
#include "dijkstra_program.h"
#include "reach_program.h"
//#include "triangle_program.h"
//#include "n_hop_reach_program.h"
#include "clustering_program.h"

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

    template <typename params_type, typename node_state_type>
    struct node_function_type
    {
        public:
            typedef std::vector<std::pair<db::element::remote_node, params_type>> (*value_type)(uint64_t, // req_id
                db::element::node&, // this node
                db::element::remote_node&, // this remote node
                params_type&,
                std::function<node_state_type&()>,
                std::shared_ptr<vc::vclock> &req_vlock,
                std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                    std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>& add_cache_func,
                    std::unique_ptr<db::caching::cache_response> cache_response);

    };

    template <typename ParamsType, typename NodeStateType>
    void node_program_runner(typename node_prog::node_function_type<ParamsType,
            NodeStateType>::value_type np,
            std::vector<std::pair<uint64_t, ParamsType>> &start_node_params,
            node_prog::prog_type program,
            uint64_t request_id)
    { }

    template <typename ParamsType, typename NodeStateType>
        struct node_prog_running_state //: public virtual node_prog::Packable XXX can't get making this packable to work
    {
        private:
        /* constructs a clone without start_node_params or cache_value */
        node_prog_running_state(node_prog::prog_type _prog_type_recvd, bool _global_req, uint64_t _vt_id, std::shared_ptr<vc::vclock> _req_vclock, uint64_t _req_id)
            : prog_type_recvd(_prog_type_recvd)
              , global_req(_global_req)
              , vt_id(_vt_id)
              , req_vclock(_req_vclock)
              , req_id(_req_id) {};
        public:
        node_prog::prog_type prog_type_recvd;
        bool global_req;
        uint64_t vt_id;
        std::shared_ptr<vc::vclock> req_vclock;
        uint64_t req_id;
        std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> start_node_params;
        std::unique_ptr<db::caching::cache_response> cache_value;

        node_prog_running_state() {};
        // delete standard copy onstructors
        node_prog_running_state (const node_prog_running_state &) = delete;
        node_prog_running_state& operator=(node_prog_running_state const&) = delete;
        //node_prog_running_state(const node_prog_running_state &)  = delete;

        node_prog_running_state clone_without_start_node_params()
        {
            return node_prog_running_state(prog_type_recvd, global_req, vt_id, req_vclock, req_id);
        }

        node_prog_running_state(node_prog_running_state&& copy_from)
            : prog_type_recvd(copy_from.prog_type_recvd)
              , global_req(copy_from.global_req)
              , vt_id(copy_from.vt_id)
              , req_vclock(copy_from.req_vclock)
              , req_id(copy_from.req_id)
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
                static_assert(std::is_base_of<Node_Parameters_Base, ParamsType>::value, "Params type must be derived from Node_Parameters_Base");
                static_assert(std::is_base_of<Node_State_Base, NodeStateType>::value, "Params type must be derived from Node_Parameters_Base");

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
            new particular_node_program<node_prog::reach_params, node_prog::reach_node_state>(REACHABILITY, node_prog::reach_node_program) },
        /*
        { TRIANGLE_COUNT,
        new particular_node_program<node_prog::triangle_params, node_prog::triangle_node_state>(TRIANGLE_COUNT, node_prog::triangle_node_program) },
        { N_HOP_REACHABILITY,
          new particular_node_program<node_prog::n_hop_reach_params, node_prog::n_hop_reach_node_state>(N_HOP_REACHABILITY, node_prog::n_hop_reach_node_program) },
        */
        { DIJKSTRA,
            new particular_node_program<node_prog::dijkstra_params, node_prog::dijkstra_node_state>(DIJKSTRA, node_prog::dijkstra_node_program) },
        { CLUSTERING,
            new particular_node_program<node_prog::clustering_params, node_prog::clustering_node_state>(CLUSTERING, node_prog::clustering_node_program) }
    };
}
#endif //__NODE_PROG__
