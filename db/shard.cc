/*
 * ===============================================================
 *    Description:  Core database functionality for a shard server
 *
 *        Created:  07/25/2013 04:02:37 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *                  Greg Hill, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <iostream>
#include <signal.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#define __WEAVER_DEBUG__
#include "common/event_order.h"
#include "common/weaver_constants.h"
// TODO #include "common/message_graph_elem.h"
#include "shard.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"

// global static variables
static uint64_t shard_id;
static db::shard *S;
db::shard *db::thread::pool::S = NULL; // reinitialized in graph constructor

inline void
create_node(vc::vclock_t &t_creat, uint64_t node_handle)
{
    S->create_node(node_handle, t_creat, false);
}

inline uint64_t
create_edge(vc::vclock_t &t_creat, uint64_t edge_handle, uint64_t n1, uint64_t n2, uint64_t loc2)
{
    return S->create_edge(edge_handle, n1, n2, loc2, t_creat);
}

inline uint64_t
delete_node(vc::vclock_t &t_del, uint64_t node_handle)
{
    return S->delete_node(node_handle, t_del);
}

inline uint64_t
delete_edge(vc::vclock_t &t_del, uint64_t edge_handle)
{
    return S->delete_edge(edge_handle, t_del);
}

inline uint64_t
create_reverse_edge(vc::vclock_t &vclk, uint64_t edge_handle, uint64_t local_node, uint64_t remote_node, uint64_t remote_loc)
{
    return S->create_reverse_edge(edge_handle, local_node, remote_node, remote_loc, vclk);
}

void
unpack_update_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;
    vc::vclock_t vclk, qts;
    uint64_t handle, elem1, elem2, loc2, ret;

    switch (request->type) {
        case message::REVERSE_EDGE_CREATE:
            DEBUG << "reverse edge create" << std::endl;
            message::unpack_message(*request->msg, request->type, vclk, handle, elem1, elem2, loc2);
            ret = create_reverse_edge(vclk, handle, elem1, elem2, loc2);
            break;

        default:
            DEBUG << "unknown type" << std::endl;
    }
    if (ret == 0) {
        // update successful
    } else {
        // node being migrated, tx needs to be forwarded
        // TODO
    }
    delete request;
}

void
unpack_tx_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;
    uint64_t vt_id, tx_id, ret;
    vc::vclock_t vclk, qts;
    transaction::pending_tx tx;
    bool ack = true;
    message::unpack_message(*request->msg, message::TX_INIT, vt_id, vclk, qts, tx_id, tx.writes);
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                DEBUG << "unpacked node create" << std::endl;
                create_node(vclk, upd->handle);
                S->record_completed_transaction(vt_id, tx_id); // TODO: only do this once per transaction
                DEBUG << "done node create" << std::endl;
                ret = 0;
                break;

            case transaction::EDGE_CREATE_REQ:
                ret = create_edge(vclk, upd->handle, upd->elem1, upd->elem2, upd->loc2);
                S->record_completed_transaction(vt_id, tx_id);
                break;

            case transaction::NODE_DELETE_REQ:
                ret = delete_node(vclk, upd->elem1);
                S->record_completed_transaction(vt_id, tx_id);
                break;

            case transaction::EDGE_DELETE_REQ:
                ret = delete_edge(vclk, upd->elem1);
                S->record_completed_transaction(vt_id, tx_id);
                break;

            default:
                DEBUG << "unknown type" << std::endl;
        }
        if (ret == 0) {
            // tx subpart successful
        } else {
            // node being migrated, tx needs to be forwarded
            // TODO also need to maintain DS for tracking when to ack transaction
            ack = false;
        }
    }
    delete request;
    if (ack) {
        // send tx confirmation to coordinator
        message::message conf_msg;
        message::prepare_message(conf_msg, message::TX_DONE, tx_id);
        S->send(vt_id, conf_msg.buf);
    }
}

template <typename NodeStateType>
std::shared_ptr<NodeStateType> get_node_state(node_prog::prog_type pType,
        uint64_t req_id, uint64_t node_handle)
{
    std::shared_ptr<NodeStateType> ret;
    auto state = S->fetch_prog_req_state(pType, req_id, node_handle);
    if (state) {
        ret = std::dynamic_pointer_cast<NodeStateType>(state);
    }
    return ret;
}

template <typename NodeStateType>
NodeStateType& return_state(node_prog::prog_type pType, uint64_t req_id,
        uint64_t node_handle, std::shared_ptr<NodeStateType> toRet)
{
    if (toRet) {
        return *toRet;
    } else {
        std::shared_ptr<NodeStateType> newState(new NodeStateType());
        S->insert_prog_req_state(pType, req_id, node_handle,
                std::dynamic_pointer_cast<node_prog::Packable_Deletable>(newState));
        return *newState;
    }
}

void
unpack_node_program(void *req) {
    db::graph_request *request = (db::graph_request *) req;
    node_prog::prog_type pType;

    DEBUG << "node program got of priority queue to run!" << std::endl;
    message::unpack_message(*request->msg, message::NODE_PROG, pType);
    node_prog::programs.at(pType)->unpack_and_run_db(*request->msg); // std::move me!
    delete request;
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_run_db(message::message &msg)
{
    DEBUG << "node program runing in templated function!" << std::endl;
    // unpack some start params from msg:
    std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> start_node_params;
    uint64_t vt_id;
    uint64_t req_id;
    vc::vclock_t req_vclock;
    //std::vector<uint64_t> vclocks; //needed to pass to next message
    prog_type prog_type_recvd;
    bool done_request = false;
    /*
    // map from loc to send back to the handle that was deleted, the params it was given, and the handle to send back to
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, uint64_t>>> batched_deleted_nodes; 
    std::vector<uint64_t> dirty_cache_ids; // cache values used by user that we need to verify are good at coord
    std::unordered_set<uint64_t> invalid_cache_ids; // cache values from coordinator we know are invalid
    */

    // map from location to send to next to tuple of handle and params to send to next, and node that sent them
    // these are the node programs that will be propagated onwards
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> batched_node_progs;
    db::element::remote_node this_node(S->shard_id, 0);
    uint64_t node_handle;

    // unpack the node program
    try {
        message::unpack_message(msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id, start_node_params);
        //, dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[G->myid]);
        /*
#ifdef __WEAVER_DEBUG__
if (batched_deleted_nodes[G->myid].size() == 1 && std::get<0>(batched_deleted_nodes[G->myid].at(0)) == MAX_TIME) {
        //DEBUG << "Unpacking forwarded request in unpack_and_run for node "
        //    << std::get<0>(start_node_params.at(0)) << std::endl;
        batched_deleted_nodes[G->myid].clear();
        }
#endif
         */
    } catch (std::bad_alloc& ba) {
        DEBUG << "bad_alloc caught " << ba.what() << '\n';
        return;
    }

    // node state and cache functions
    std::function<NodeStateType&()> node_state_getter;
    //std::function<CacheValueType&()> cache_value_putter;
    //std::function<std::vector<std::shared_ptr<CacheValueType>>()> cached_values_getter;

    // check if request completed
    if (S->check_done_request(req_id)) {
        done_request = true;
    }
    while ((!start_node_params.empty() /*|| !batched_deleted_nodes[G->myid].empty()*/) && !done_request) {
        /*
        // deleted nodes loop
        for (std::tuple<uint64_t, ParamsType, uint64_t> del_node_params: batched_deleted_nodes[G->myid]) {
        uint64_t deleted_node_handle = std::get<0>(del_node_params);
        ParamsType del_node_params_given = std::get<1>(del_node_params);
        uint64_t parent_handle = std::get<2>(del_node_params);

        db::element::node *node = G->acquire_node(parent_handle);
        // parent should definitely not be deleted
        assert(node != NULL && node->get_del_time() > req_vclock);
        if (node->state == db::element::node::mode::IN_TRANSIT
        || node->state == db::element::node::mode::MOVED) {
        // queueing/forwarding delete program
        std::unique_ptr<message::message> m(new message::message());
        std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> dummy_node_params;
        std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> fwd_deleted_nodes; 
        fwd_deleted_nodes.emplace_back(del_node_params);
        message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, req_vclock,
                    req_id, dummy_node_params, dirty_cache_ids, invalid_cache_ids, fwd_deleted_nodes);
                if (node->state == db::element::node::mode::MOVED) {
                    // node set up at new shard, fwd request immediately
                    uint64_t new_loc = node->new_loc;
                    G->release_node(node);
                    G->send(new_loc, m->buf);
                    //DEBUG << "MOVED: Forwarding del prog immed, node handle " << node_handle
                    //    << ", to location " << new_loc << std::endl;
                } else {
                    // queue request for forwarding later
                    node->pending_requests.emplace_back(std::move(m));
                    G->release_node(node);
                    //DEBUG << "IN_TRANSIT: Enqueuing del prog for migr node " << node_handle << std::endl;
                }
            } else {
                DEBUG << "calling delete program" << std::endl;
                std::shared_ptr<NodeStateType> state = get_node_state<NodeStateType>(prog_type_recvd,
                        req_id, parent_handle);
                node_state_getter = std::bind(return_state<NodeStateType>, prog_type_recvd,
                        req_id, parent_handle, state);

                if (G->check_done_request(req_id)) {
                    done_request = true;
                    G->release_node(node);
                    break;
                }
                auto next_node_params = enclosed_node_deleted_func(req_id, *node,
                        deleted_node_handle, del_node_params_given, node_state_getter); 

                G->release_node(node);
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    uint64_t next_loc = res.first.loc;
                    if (next_loc == COORD_ID) {
                        // signal to send back to coordinator
                        // XXX get rid of pair, without pair it is not working for some reason
                        std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                        message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, req_id,
                            temppair, dirty_cache_ids, invalid_cache_ids);
                        G->send(COORD_ID, msg.buf);
                    } else if (next_loc == G->myid) {
                        start_node_params.emplace_back(res.first.handle, std::move(res.second), this_node);
                    } else {
                        batched_node_progs[next_loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                    }
                }
            }
        }
        batched_deleted_nodes[G->myid].clear(); // we have run programs for this list
        */

        for (auto &handle_params : start_node_params) {
            node_handle = std::get<0>(handle_params);
            ParamsType params = std::get<1>(handle_params);
            this_node.handle = node_handle;
            // XXX maybe use a try-lock later so forward progress can continue on other nodes in list
            db::element::node *node = S->acquire_node(node_handle);
            if (node == NULL || order::compare_two_vts(node->get_del_time(), req_vclock)==0) { // TODO: TIMESTAMP
                if (node != NULL) {
                    S->release_node(node);
                }

                /*
                db::element::remote_node parent = std::get<2>(handle_params);
                batched_deleted_nodes[parent.loc].emplace_back(std::make_tuple(node_handle, params, parent.handle));
                */
                DEBUG << "Node " << node_handle << " deleted, cur request num " << req_id << std::endl;
                /*
            } else if (node->state == db::element::node::mode::IN_TRANSIT
                    || node->state == db::element::node::mode::MOVED) {
                // queueing/forwarding node program
                std::unique_ptr<message::message> m(new message::message());
                std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> fwd_node_params;
                std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> dummy_deleted_nodes; 
#ifdef __WEAVER_DEBUG__
                dummy_deleted_nodes.emplace_back(std::make_tuple(MAX_TIME, ParamsType(), MAX_TIME));
#endif
                fwd_node_params.emplace_back(handle_params);
                message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, req_vclock,
                    req_id, fwd_node_params, dirty_cache_ids, invalid_cache_ids, dummy_deleted_nodes);
                if (node->state == db::element::node::mode::MOVED) {
                    // node set up at new shard, fwd request immediately
                    uint64_t new_loc = node->new_loc;
                    G->release_node(node);
                    G->send(new_loc, m->buf);
                    //DEBUG << "MOVED: Forwarding request immed, node handle " << node_handle
                    //    << ", to location " << new_loc << std::endl;
                } else {
                    // queue request for forwarding later
                    node->pending_requests.emplace_back(std::move(m));
                    G->release_node(node);
                    //DEBUG << "IN_TRANSIT: Enqueuing request for migr node " << node_handle << std::endl;
                }
                */
            } else { // node does exist
                assert(node->state == db::element::node::mode::STABLE);
                // bind cache getter and putter function variables to functions
                std::shared_ptr<NodeStateType> state = get_node_state<NodeStateType>(prog_type_recvd,
                        req_id, node_handle);
                node_state_getter = std::bind(return_state<NodeStateType>,
                        prog_type_recvd, req_id, node_handle, state);
                /*
                cache_value_putter = std::bind(put_cache_value<CacheValueType>,
                        prog_type_recvd, req_id, node_handle, node, &dirty_cache_ids);
                cached_values_getter = std::bind(get_cached_values<CacheValueType>,
                        prog_type_recvd, req_id, node_handle, &dirty_cache_ids, std::ref(invalid_cache_ids));
                */

                if (S->check_done_request(req_id)) {
                    done_request = true;
                    S->release_node(node);
                    break;
                }
                // call node program
                auto next_node_params = enclosed_node_prog_func(req_id, *node, this_node,
                        params, // actual parameters for this node program
                        node_state_getter, req_vclock);
                /*
                        cache_value_putter,
                        cached_values_getter);
                        */
                // batch the newly generated node programs for onward propagation
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    uint64_t loc = res.first.loc;
                    if (loc == COORD_ID) {
                        // signal to send back to vector timestamper that issued request
                        // TODO mark done
                        // XXX get rid of pair, without pair it is not working for some reason
                        std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                        message::prepare_message(msg, message::NODE_PROG_RETURN, req_id, temppair);
                        S->send(vt_id, msg.buf);
                    } else {
                        batched_node_progs[loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                        if (!MSG_BATCHING && (loc != S->shard_id)) {
                            message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id,
                                batched_node_progs[loc] /*,dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[loc]*/);
                            batched_node_progs[loc].clear();
                            //batched_deleted_nodes[loc].clear();
                            S->send(loc, msg.buf);
                        }
                        /*
                        S->msg_count_mutex.lock();
                        S->agg_msg_count[node_handle]++;
                        S->request_count[loc-1]++; // increment count of msges sent to loc
                        S->msg_count_mutex.unlock();
                        */
                    }
                }
                S->release_node(node);
            }
            if (MSG_BATCHING) {
                for (uint64_t next_loc = 1; next_loc <= NUM_SHARDS; next_loc++) {
                    if (((!batched_node_progs[next_loc].empty() && batched_node_progs[next_loc].size()>BATCH_MSG_SIZE)
                         /*|| (!batched_deleted_nodes[next_loc].empty())*/)
                        && next_loc != S->shard_id) {
                        message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id,
                            batched_node_progs[next_loc]/*, dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[next_loc]*/);
                        S->send(next_loc, msg.buf);
                        batched_node_progs[next_loc].clear();
                        //batched_deleted_nodes[next_loc].clear();
                    }
                }
            }
        }
        start_node_params = std::move(batched_node_progs[S->shard_id]);
        if (S->check_done_request(req_id)) {
            done_request = true;
        }
    }

    // propagate all remaining node progs
    for (uint64_t next_loc = 1; next_loc <= NUM_SHARDS && !done_request; next_loc++) {
        if (((!batched_node_progs[next_loc].empty())
          /*|| (!batched_deleted_nodes[next_loc].empty())*/)
            && next_loc != S->shard_id) {
            message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id,
                batched_node_progs[next_loc]/*, dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[next_loc]*/);
            S->send(next_loc, msg.buf);
            batched_node_progs[next_loc].clear();
            //batched_deleted_nodes[next_loc].clear();
        }
    }
}


template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID)
{
    UNUSED(msg);
    UNUSED(clientID);
}

// server msg recv loop for the shard server
void
msgrecv_loop()
{
    busybee_returncode ret;
    uint64_t sender, vt_id, req_id;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg(new message::message());
    db::thread::unstarted_thread *thr;
    db::graph_request *request;
    node_prog::prog_type pType;
    vc::vclock_t vclk, qts;

    while (true) {
        if ((ret = S->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "msg recv error: " << ret << " at shard " << S->shard_id << std::endl;
            continue;
        }
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        rec_msg->change_type(mtype);
        sender -= ID_INCR;
        vclk.clear();
        qts.clear();

        switch (mtype)
        {
            case message::TX_INIT:
                DEBUG << "got tx_init" << std::endl;
                message::unpack_message(*rec_msg, message::TX_INIT, vt_id, vclk, qts);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(qts.at(shard_id-SHARD_ID_INCR), vclk, unpack_tx_request, request);
                //DEBUG << "going to add request to threadpool" << std::endl;
                S->add_write_request(vt_id, thr);
                //DEBUG << "added request to threadpool" << std::endl;
                rec_msg.reset(new message::message());
                break;

            case message::REVERSE_EDGE_CREATE:
                //message::unpack_message(*rec_msg, mtype, vclk, handle, elem1, elem2, 
                message::unpack_message(*rec_msg, mtype, vclk);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, vclk, unpack_update_request, request);
                S->add_write_request(0, thr);
                rec_msg.reset(new message::message());
                break;

            case message::NODE_PROG:
                DEBUG << "got node_prog" << std::endl;
                message::unpack_message(*rec_msg, message::NODE_PROG, pType, vt_id, vclk, req_id);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_node_program, request);
                DEBUG << "going to add node prog to threadpool" << std::endl;
                S->add_read_request(vt_id, thr);
                DEBUG << "added node prog to threadpool" << std::endl;
                rec_msg.reset(new message::message());
                break;

            //case message::TRANSIT_NODE_DELETE_REQ:
            //case message::TRANSIT_EDGE_CREATE_REQ:
            //case message::TRANSIT_REVERSE_EDGE_CREATE:
            //case message::TRANSIT_EDGE_DELETE_REQ:
            //case message::TRANSIT_EDGE_ADD_PROP:
            //case message::TRANSIT_EDGE_DELETE_PROP:
            //    break;

            //case message::CLEAN_UP:
            //case message::MIGRATE_NODE_STEP1:
            //case message::MIGRATE_NODE_STEP2:
            //case message::COORD_NODE_MIGRATE_ACK:
            //case message::MIGRATE_NODE_STEP4:
            //case message::MIGRATE_NODE_STEP6a:
            //case message::MIGRATE_NODE_STEP6b:
            //case message::COORD_CLOCK_REPLY:
            //case message::MIGRATED_NBR_UPDATE:
            //case message::MIGRATED_NBR_ACK:
            //case message::PERMANENT_DELETE_EDGE:
            //case message::PERMANENT_DELETE_EDGE_ACK:
            //case message::REQUEST_COUNT:
            //case message::REQUEST_COUNT_ACK:
            //    request = new db::update_request(mtype, 0, std::move(rec_msg));
            //    thr = new db::thread::unstarted_thread(0, unpack_update_request, request);
            //    G->thread_pool.add_request(thr);
            //    rec_msg.reset(new message::message());
            //    break;

            //case message::NODE_PROG:
            //    vclocks.clear();
            //    message::unpack_message(*rec_msg, message::NODE_PROG, pType, vclocks);
            //    request = new db::update_request(mtype, 0, std::move(rec_msg));
            //    thr = new db::thread::unstarted_thread(vclocks[G->myid-1], unpack_and_run_node_program, request);
            //    G->thread_pool.add_request(thr);
            //    rec_msg.reset(new message::message());
            //    break;

            //case message::MIGRATION_TOKEN:
            //    DEBUG << "Now obtained migration token at shard " << G->myid << std::endl;
            //    G->migr_token_mutex.lock();
            //    G->migr_token = true;
            //    G->migrated = false;
            //    G->migr_token_mutex.unlock();
            //    DEBUG << "Ended obtaining token" << std::endl;
            //    break;

            case message::EXIT_WEAVER:
                exit(0);
                
            default:
                DEBUG << "unexpected msg type " << mtype << std::endl;
        }
    }
}

int
main(int argc, char* argv[])
{
    // TODO signal(SIGINT, end_program);
    if (argc != 2) {
        DEBUG << "Usage: " << argv[0] << " <myid>" << std::endl;
        return -1;
    }
    uint64_t id = atoi(argv[1]);
    shard_id = id;
    S = new db::shard(id);
    std::cout << "Weaver: shard instance " << S->shard_id << std::endl;

    // TODO migration methods init

    msgrecv_loop();

    return 0;
}
