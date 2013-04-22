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
#include <map>
#include <unordered_map>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"
#include "element/remote_node.h"
#include "db/graph.h"

#include "coordinator/central.h"
#include "db/node_prog_type.h"
#include "db/dijkstra_program.h"

namespace db
{
    template <typename ParamsType, typename NodeStateType, typename CacheValueType>
        void node_program_runner(db::graph *G,
                typename db::node_function_type<ParamsType, NodeStateType, CacheValueType>::value_type np,
                std::vector<std::pair<uint64_t, ParamsType>> &start_node_params,
                db::prog_type program,
                uint64_t request_id)
        {
        }

    struct node_program{
        public:
            virtual void unpack_and_run_db(db::graph *g, message::message &msg) = 0;
            virtual void unpack_and_start_coord(coordinator::central *server,
                    message::message &msg,
                    std::shared_ptr<coordinator::pending_req> request) = 0;

            //virtual void pack_message(e::buffer::packer&, Deletable params) = 0;
            //virtual void destroy_cache_value(void *val) = 0;
            virtual ~node_program() { }
    };

    template <typename ParamsType, typename NodeStateType, typename CacheValueType>
        class particular_node_program : public virtual node_program {
            public:
                typedef typename node_function_type<ParamsType, NodeStateType, CacheValueType>::value_type func;
                func enclosed_function;
                prog_type type;
                particular_node_program(prog_type _type, func _enclosed_function) :
                    enclosed_function(_enclosed_function), type(_type)
            { }

                virtual void unpack_and_run_db(db::graph *G, message::message &msg) {
                    // unpack some start params from msg:
                    std::vector<std::pair<uint64_t, ParamsType>> start_node_params;
                    uint64_t unpacked_request_id;
                    std::vector<uint64_t> vclocks; //needed to pass to next message
                    prog_type ignore;

                    printf("ZAAAAAAAAAAAAAAAAAA\n");
                    message::unpack_message(msg, message::NODE_PROG, ignore, vclocks, unpacked_request_id, start_node_params);


                    std::unordered_map<int, std::vector<std::pair<uint64_t, ParamsType>>> batched_node_progs;

                    while (!start_node_params.empty()){
                        for (auto &handle_params : start_node_params)
                        {
                            uint64_t node_handle = handle_params.first;
                            db::element::node* node = G->acquire_node(node_handle); // maybe use a try-lock later so forward progress can continue on other nodes in list

                            CacheValueType *cache;
                            if (G->prog_cache_exists(type, unpacked_request_id, node_handle)){
                                cache = (CacheValueType *) G->fetch_prog_cache(type, unpacked_request_id, node_handle);
                            } else {
                                cache = new CacheValueType();
                                G->insert_prog_cache(type, unpacked_request_id, node_handle, cache);
                            }

                            NodeStateType *state;
                            if (G->prog_req_state_exists(type, unpacked_request_id, node_handle)){
                                state = (NodeStateType *) G->fetch_prog_req_state(type, unpacked_request_id, node_handle);
                            } else {
                                state = new NodeStateType();
                                G->insert_prog_req_state(type, unpacked_request_id, node_handle, state);
                            }

                            auto next_node_params = enclosed_function(*node, handle_params.second, *state, *cache); // call node program
                            for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params)
                            {
                                batched_node_progs[res.first.loc].emplace_back(res.first.handle, std::move(res.second));
                            }
                        }
                        start_node_params = std::move(batched_node_progs[G->myid]); // hopefully this nicely cleans up old vector, makes sure batched nodes
                    }
                    // if done send to coordinator and call delete on all objects in the map for node state

                    // now propagate requests
                    for (auto &batch : batched_node_progs){
                        if (batch.first == G->myid){
                            // this shouldnt happen or it should be an empty vector
                            // make sure not to do anything here because the vector was moved out
                        } else {
                            // send msg to batch.first (location) with contents batch.second (start_node_params for that machine)
                        }
                    }
                }

            virtual void unpack_and_start_coord(coordinator::central *server, message::message &msg, std::shared_ptr<coordinator::pending_req> request)
            {
                db::prog_type ignore;
                printf("coordinator ZAAAAAAAAAAAAAAAAAA\n");
                std::vector<std::pair<uint64_t, ParamsType>> initial_args;

                message::unpack_message(msg, message::CLIENT_NODE_PROG_REQ, request->client->port, ignore, initial_args);

                std::unordered_map<int, std::vector<std::pair<uint64_t, ParamsType>>> initial_batches; // map from locations to a list of start_node_params to send to that shard
                server->update_mutex.lock();

                for (std::pair<uint64_t, ParamsType>& node_params_pair : initial_args)
                {
                    if (check_elem(server, node_params_pair.first, true)){
                        std::cerr << "one of the arg nodes has been deleted, cannot perform request" << std::endl;
                        /*
                           message::message msg;
                           message::prepare_message(msg, message::CLIENT_REPLY, false);
                           server->send(std::move(request->client), msg.buf);
                         */
                   return;
                    }
                    common::meta_element *me = server->nodes.at(node_params_pair.first);
                    initial_batches[me->get_loc()].emplace_back(std::make_pair(node_params_pair.first, std::move(node_params_pair.second)));
                }
                request->vector_clock.reset(new std::vector<uint64_t>(*server->vc.clocks));
                /*
                   request->out_count = server->last_del;
                   request->out_count->cnt++;
                 */

                request->req_id = ++server->request_id;

                server->pending.insert(std::make_pair(request->req_id, request));
                /*
                   std::cout << "Reachability request number " << request->req_id << " from source"
                   << " request->elem " << request->elem1 << " " << me1->get_loc() 
                   << " to destination request->elem " << request->elem2 << " " 
                   << me2->get_loc() << std::endl;
                 */

                message::message msg_to_send;
                for (auto &batch_pair : initial_batches){
                    message::prepare_message(msg_to_send, message::NODE_PROG, request->pType, *request->vector_clock, 
                            request->req_id, batch_pair.second);
                    server->send(batch_pair.first, msg_to_send.buf); // later change to send without update mutex lock
                   }
                   server->update_mutex.unlock();
            }
        };

    std::map<prog_type, node_program*> programs = {
        {DIJKSTRA, new particular_node_program<db::dijkstra_params, db::dijkstra_node_state, db::dijkstra_cache_value>(DIJKSTRA, db::dijkstra_node_program) }, 
        {REACHABILITY, NULL }
    };
} 

#endif //__NODE_PROG__
