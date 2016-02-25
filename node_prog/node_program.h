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
#include "db/async_nodeprog_state.h"
#include "node_prog/cache_response.h"
#include "node_prog/node.h"
#include "node_prog/edge.h"

#include "node_prog/node_prog_type.h"
//#include "node_prog/reach_program.h"
//#include "node_prog/pathless_reach_program.h"
//#include "node_prog/clustering_program.h"
//#include "node_prog/read_node_props_program.h"
//#include "node_prog/read_n_edges_program.h"
//#include "node_prog/edge_count_program.h"
//#include "node_prog/edge_get_program.h"
//#include "node_prog/node_get_program.h"
//#include "node_prog/clustering_program.h"
//#include "node_prog/two_neighborhood_program.h"
//#include "node_prog/traverse_with_props.h"
//#include "node_prog/discover_paths.h"
//#include "node_prog/cause_and_effect.h"
//#include "node_prog/n_gram_path.h"
//#include "node_prog/get_btc_block.h"
//#include "node_prog/get_btc_tx.h"
//#include "node_prog/get_btc_addr.h"

namespace coordinator
{
    class central;
    class pending_req;
    class hyper_stub;
}

namespace node_prog
{

//    template <typename params_type, typename node_state_type, typename cache_value_type>
//    struct node_function_type
//    {
//        public:
//            typedef std::pair<search_type, std::vector<std::pair<db::remote_node, params_type>>> (*value_type)(
//                node&, // this node
//                db::remote_node&, // this remote node
//                params_type&,
//                std::function<Node_State_Base&()>,
//                std::function<void(std::shared_ptr<cache_value_type>,
//                    std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)> &add_cache_func,
//                    cache_response<cache_value_type> *cache_response);
//
//    };

    struct node_prog_running_state
    {
        node_prog_running_state() : m_handle(nullptr) { }

        uint64_t m_type;
        void *m_handle;
        uint64_t vt_id;
        std::shared_ptr<vc::vclock> req_vclock;
        uint64_t req_id;
        uint64_t vt_prog_ptr;
        std::deque<std::pair<node_handle_t, np_param_ptr_t>> start_node_params;
        //std::unique_ptr<cache_response<CacheValueType>> cache_value;
        std::unordered_map<uint64_t, std::deque<std::pair<node_handle_t, np_param_ptr_t>>> batched_node_progs;
        std::vector<std::pair<node_handle_t, vclock_ptr_t>> nodes_that_created_state;
   };

    class node_program
    {
        public:
            virtual void unpack_and_run_db(uint64_t tid, std::unique_ptr<message::message> msg, order::oracle *time_oracle) = 0;
            virtual void unpack_context_reply_db(uint64_t tid, std::unique_ptr<message::message> msg, order::oracle *time_oracle) = 0;
            virtual void unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, coordinator::hyper_stub*) = 0;
            virtual void continue_execution(uint64_t tid, order::oracle*, db::async_nodeprog_state) = 0;

            virtual ~node_program() { }
    };
}

#endif
