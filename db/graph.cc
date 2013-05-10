/*
 * ===============================================================
 *    Description:  Core graph database functionality for a shard 
 *                  server
 *
 *        Created:  Tuesday 16 October 2012 03:03:11  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef PO6_NDEBUG_LEAKS
#define PO6_NDEBUG_LEAKS

#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <po6/net/location.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#include "common/weaver_constants.h"
#include "common/message.h"
#include "graph.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
#include "node_prog/dijkstra_program.h"
#include "node_prog/reach_program.h"
#include "node_prog/clustering_program.h"

// migration methods
void migrate_node_step1(db::graph *G, uint64_t node_handle, int new_shard);
void migrate_node_step2(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step3(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step4(db::graph *G);
void migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step6(db::graph *G);
void migrate_node_step7(db::graph *G);
void migration_wrapper(db::graph *G);

// daemon processes
void shard_daemon_begin(db::graph *G, void*);
void shard_daemon_end(db::graph *G);

// create a graph node
inline void
handle_create_node(db::graph *G, uint64_t req_id) 
{
    G->create_node(req_id);
}

// delete a graph node
inline void
handle_delete_node(db::graph *G, uint64_t req_id, uint64_t node_handle, std::unique_ptr<std::vector<uint64_t>> cache)
{
    std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_node(node_handle, req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first == 0) {
        message::prepare_message(*msg, message::NODE_DELETE_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_NODE_DELETE_REQ, success.first, req_id, *success.second);
        G->fwd_transit_node_update(std::move(msg));
    }
}

// create a graph edge
inline void
handle_create_edge(db::graph *G, uint64_t req_id, uint64_t n1, uint64_t n2, int loc2, uint64_t tc2)
{
    uint64_t ret = G->create_edge(n1, req_id, n2, loc2, tc2);
    if (ret != 0) {
        std::unique_ptr<message::message> msg(new message::message());
        message::prepare_message(*msg, message::TRANSIT_EDGE_CREATE_REQ, ret, req_id, n2, loc2, tc2);
        G->fwd_transit_node_update(std::move(msg));
    }
}

// create a back pointer for an edge
inline void
handle_create_reverse_edge(db::graph *G, uint64_t req_id, uint64_t remote_node, int remote_loc, uint64_t local_node)
{
    uint64_t ret = G->create_reverse_edge(req_id, local_node, remote_node, remote_loc);
    if (ret != 0) {
        std::unique_ptr<message::message> msg(new message::message());
        message::prepare_message(*msg, message::TRANSIT_REVERSE_EDGE_CREATE, ret, req_id, remote_node, remote_loc);
        G->fwd_transit_node_update(std::move(msg));
    }
}

// delete an edge
inline void
handle_delete_edge(db::graph *G, uint64_t req_id, uint64_t n, uint64_t e, std::unique_ptr<std::vector<uint64_t>> cache)
{
    std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_edge(n, e, req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first == 0) {
        message::prepare_message(*msg, message::EDGE_DELETE_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_EDGE_DELETE_REQ, success.first, req_id, e, *success.second);
        G->fwd_transit_node_update(std::move(msg));
    }
}

// add edge property
inline void
handle_add_edge_property(db::graph *G, uint64_t req_id, uint64_t node_addr, uint64_t edge_addr, common::property &prop)
{
    uint64_t ret = G->add_edge_property(node_addr, edge_addr, prop);
    if (ret != 0) {
        std::unique_ptr<message::message> msg(new message::message());
        message::prepare_message(*msg, message::TRANSIT_EDGE_ADD_PROP, ret, req_id, edge_addr, prop);
        G->fwd_transit_node_update(std::move(msg));
    }
}

// delete all edge properties with the given key
inline void
handle_delete_edge_property(db::graph *G, uint64_t req_id, uint64_t node_addr, uint64_t edge_addr, uint32_t key,
    std::unique_ptr<std::vector<uint64_t>> cache)
{
    std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_all_edge_property(node_addr, edge_addr, key, req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first == 0) {
        message::prepare_message(*msg, message::EDGE_DELETE_PROP_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_EDGE_DELETE_PROP, req_id, edge_addr, key, *success.second);
        G->fwd_transit_node_update(std::move(msg));
    }
}

// send node information to new shard
// mark node as "in transit" so that subsequent requests are queued up
// send migration information to coord
void
migrate_node_step1(db::graph *G, uint64_t node_handle, int shard)
{
    std::cout << "Starting step1\n";
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->acquire_node(node_handle);
    if (n->updated) {
        G->release_node(n);
        G->mrequest.mutex.unlock();
        std::cout << "Canceling migration!!!\n";
        migration_wrapper(G);
    } else {
        // mark node as "in transit"
        n->state = db::element::node::mode::IN_TRANSIT;
        n->new_loc = shard;
        // pack entire node info in a ginormous message and send to new loc
        message::message msg(message::MIGRATE_NODE_STEP1);
        G->mrequest.cur_node = node_handle;
        G->mrequest.new_loc = shard;
        std::vector<std::unique_ptr<message::message>>().swap(G->mrequest.pending_requests);
        message::prepare_message(msg, message::MIGRATE_NODE_STEP1, node_handle, G->myid, *n);
        G->send(shard, msg.buf);
        // send new loc information to coordinator
        message::prepare_message(msg, message::COORD_NODE_MIGRATE, G->mrequest.cur_node, n->new_loc);
        G->send_coord(msg.buf);
        G->release_node(n);
        G->mrequest.mutex.unlock();
        std::cout << "Ending step1\n";
    }
}

// Receive and place a node which has been migrated to this shard
    void
migrate_node_step2(db::graph *G, std::unique_ptr<message::message> msg)
{
//    std::cout << "Starting step2\n";
    int from_loc;
    uint64_t node_handle;
    db::element::node *n;
    // create a new node, unpack the message
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP1, node_handle);
    G->create_node(node_handle, true);
    n = G->acquire_node(node_handle);
    std::vector<uint64_t>().swap(n->agg_msg_count);
    // TODO change at coordinator so that user does not have to enter node for edge
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP1, node_handle, from_loc, *n);
    n->prev_loc = from_loc;
    G->release_node(n);
//    std::cout << "Ending step2\n";
}

// receive last update clock value from coord
void
migrate_node_step3(db::graph *G, std::unique_ptr<message::message> msg)
{
//    std::cout << "Starting step3\n";
    uint64_t my_clock, other_clock;
    message::unpack_message(*msg, message::COORD_NODE_MIGRATE_ACK, my_clock, other_clock);
    G->mrequest.mutex.lock();
    G->mrequest.other_clock = other_clock;
    G->mrequest.mutex.unlock();
    if (G->set_callback(my_clock, migrate_node_step4)) {
        migrate_node_step4(G);
    }
//    std::cout << "Ending step3\n";
}

// all pre-migration time updates have been received
void
migrate_node_step4(db::graph *G)
{
//    std::cout << "Starting step4\n";
    message::message msg;
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->acquire_node(G->mrequest.cur_node);
    message::prepare_message(msg, message::MIGRATE_NODE_STEP4, n->update_count, G->mrequest.other_clock);
    G->send(G->mrequest.new_loc, msg.buf);
    G->release_node(n);
    G->mrequest.mutex.unlock();
//    std::cout << "Ending step4\n";
}

// receive the number of pending updates for the newly migrated node
void
migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg)
{
//    std::cout << "Starting step5\n";
    uint64_t update_count, target_clock;
    db::element::node *n = G->acquire_node(G->migr_node);
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP4, update_count, target_clock);
    if (n->update_count == update_count) {
        // no pending updates left
        n->state = db::element::node::mode::STABLE;
        G->release_node(n);
        if (G->set_callback(target_clock, migrate_node_step6)) {
            migrate_node_step6(G);
        }
    } else {
        // some updates yet to trickle through
        G->set_update_count(update_count, n->update_count, target_clock);
        G->release_node(n);
    }
//    std::cout << "Ending step5\n";
}

// inform other shards of migration
// Caution: assuming we have acquired node lock
void
migrate_node_step6(db::graph *G)
{
//    std::cout << "Starting step6\n";
    G->increment_clock(); // let subsequent updates come through now
    db::element::node *n = G->acquire_node(G->migr_node);
    // signal previous loc to forward pending node programs
    message::message msg;
    message::prepare_message(msg, message::MIGRATE_NODE_STEP6);
    G->send(n->prev_loc, msg.buf);
    // update in-neighbors
    for (auto &nbr: n->in_edges) {
        if (nbr.second->nbr.loc == G->myid) {
            if (nbr.second->nbr.handle == G->migr_node) {
                G->update_migrated_nbr_nonlocking(n, nbr.first, G->migr_node, G->myid);
            } else {
                G->update_migrated_nbr(nbr.second->nbr.handle, nbr.first, G->migr_node, G->myid);
            }
        } else {
            message::prepare_message(msg, message::MIGRATED_NBR_UPDATE, nbr.second->nbr.handle, nbr.first, G->migr_node, G->myid);
            G->send(nbr.second->nbr.loc, msg.buf);
        }
    }
    G->release_node(n);
//    std::cout << "Ending step6\n";
}

// forward queued traversal requests to new location
void
migrate_node_step7(db::graph *G)
{
    std::cout << "Starting step7\n";
    message::message msg;
    std::vector<uint64_t> nodes;
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->acquire_node(G->mrequest.cur_node);
    n->state = db::element::node::mode::MOVED;
    for (auto &r: G->mrequest.pending_requests) {
        G->send(G->mrequest.new_loc, r->buf);
    }
    G->release_node(n);
    G->mrequest.mutex.unlock();
    // when is this safe TODO?
    G->delete_migrated_node(G->mrequest.cur_node);
    migration_wrapper(G);
    std::cout << "Ending step7\n";
}

void
migrated_nbr_update(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::cout << "Starting nbr update\n";
    uint64_t local_node, remote_node, edge;
    int orig_loc, new_loc;
    message::unpack_message(*msg, message::MIGRATED_NBR_UPDATE, local_node, edge, remote_node, new_loc);
    G->update_migrated_nbr(local_node, edge, remote_node, new_loc);
    std::cout << "Ending nbr update\n";
}

// update cache based on confirmations for transient cached values
// and invalidations for stale entries
void
handle_cache_update(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::vector<uint64_t> good, bad;
    uint64_t perm_del_id;
    message::unpack_message(*msg, message::CACHE_UPDATE, good, bad, perm_del_id);
    
    // invalidations
    for (size_t i = 0; i < bad.size(); i++) {
        G->invalidate_prog_cache(bad[i]);
    }
    
    // confirmations
    for (size_t i = 0; i < good.size(); i++) {
        G->commit_prog_cache(good[i]);
    }
    
    G->permanent_delete(perm_del_id);
    std::cout << "Permanent delete, id = " << perm_del_id << "\tNumber of nodes = " << G->nodes.size() << std::endl;
}

template <typename NodeStateType>
NodeStateType& get_node_state(db::graph *G, node_prog::prog_type pType, uint64_t req_id, uint64_t node_handle)
{
        NodeStateType *toRet = new NodeStateType();
        if (G->prog_req_state_exists(pType, req_id, node_handle)) {
            toRet = dynamic_cast<NodeStateType *>(G->fetch_prog_req_state(pType, req_id, node_handle));
            if (toRet == NULL) {
                std::cerr << "NodeStateType needs to extend Deletable" << std::endl;
            }
        } else {
            toRet = new NodeStateType();
            G->insert_prog_req_state(pType, req_id, node_handle, toRet);
        }
        return *toRet;
}

template <typename CacheValueType>
std::vector<CacheValueType *> get_cached_values(db::graph *G, node_prog::prog_type pType, uint64_t req_id, uint64_t node_handle, 
    std::vector<uint64_t> *dirty_list_ptr, std::unordered_set<uint64_t> &ignore_set)
{
    std::vector<CacheValueType *> toRet;
    CacheValueType *cache;
    for (node_prog::CacheValueBase *cval : G->fetch_prog_cache(pType, node_handle, req_id, dirty_list_ptr, ignore_set)) {
        cache = dynamic_cast<CacheValueType *>(cval);
        if (cache == NULL) {
            std::cerr << "CacheValueType needs to extend CacheValueBase" << std::endl;
        } else {
            toRet.push_back(cache);
        }
    }
    return std::move(toRet);
}

template <typename CacheValueType>
CacheValueType& put_cache_value(db::graph *G, node_prog::prog_type pType, uint64_t req_id, uint64_t node_handle, db::element::node *n)
{
    CacheValueType * toRet;
    if (G->prog_cache_exists(pType, node_handle, req_id)) {
        // XXX They wrote cache twice! delete and replace probably
        toRet = dynamic_cast<CacheValueType *>(G->fetch_prog_cache_single(pType, node_handle, req_id));
        if (toRet == NULL) {
            // dynamic_cast failed, CacheValueType needs to extend CacheValueBase
            std::cerr << "CacheValueType needs to extend CacheValueBase" << std::endl;
        }
    } else {
        toRet = new CacheValueType();
        G->insert_prog_cache(pType, req_id, node_handle, toRet, n);
    }
    return *toRet;
}

void
unpack_and_run_node_program(db::graph *G, void *req)
{
    db::update_request *request = (db::update_request *)req;
    node_prog::prog_type pType;

    message::unpack_message(*request->msg, message::NODE_PROG, pType);
    node_prog::programs.at(pType)->unpack_and_run_db(G, *request->msg);
    delete request;
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_run_db(db::graph *G, message::message &msg)
{
    // unpack some start params from msg:
    std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> start_node_params;
    uint64_t unpacked_request_id;
    std::vector<uint64_t> vclocks; //needed to pass to next message
    prog_type prog_type_recvd;
    db::element::remote_node deleted_nodes_parent;
    ParamsType deleted_nodes_param;
    std::vector<std::pair<uint64_t, db::element::remote_node>> deleted_nodes; // node that was deleted and it's parent's handle
    uint64_t cur_node;
    std::vector<uint64_t> dirty_cache_ids; // cache values used by user that we need to verify are good at coord
    std::unordered_set<uint64_t> invalid_cache_ids; // cache values from coordinator we know are invalid
    std::unordered_map<int, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> batched_node_progs; // node programs that will be propagated
    db::element::remote_node this_node(G->myid, 0);
    uint64_t node_handle;

    // unpack the node program
    message::unpack_message(msg, message::NODE_PROG, prog_type_recvd, vclocks,
            unpacked_request_id, start_node_params, dirty_cache_ids, invalid_cache_ids);
    G->migrtestmutex.lock();
//    if (!G->already_migr && unpacked_request_id>500 && G->myid==0) {
//        G->already_migr = true;
//        migrate_node_step1(G, std::get<0>(start_node_params.at(0)), (G->myid+1)%2);
//    }
    G->migrtestmutex.unlock();

    // node state and cache functions
    std::function<NodeStateType&()> node_state_getter;
    std::function<CacheValueType&()> cache_value_putter;
    std::function<std::vector<CacheValueType *>()> cached_values_getter;

    while (!start_node_params.empty() || !deleted_nodes.empty()) {
        for (std::pair<uint64_t, db::element::remote_node> del_node: deleted_nodes) {
            size_t parent_handle = del_node.second.handle;
            db::element::node *node = G->acquire_node(parent_handle); // parent should definitely exist
            node_state_getter = std::bind(get_node_state<NodeStateType>, G, prog_type_recvd, unpacked_request_id, parent_handle);
            auto next_node_params = enclosed_node_deleted_func(unpacked_request_id, *node, del_node.first, deleted_nodes_param, node_state_getter); 
            G->release_node(node);
            for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                // signal to send back to coordinator
                int next_loc = res.first.loc;
                if (next_loc == -1) {
                    // XXX get rid of pair, without pair it is not working for some reason
                    std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                    message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, unpacked_request_id, temppair, dirty_cache_ids, invalid_cache_ids);
                    G->send_coord(msg.buf);
                } else if (next_loc == G->myid) {
                    start_node_params.emplace_back(res.first.handle, std::move(res.second), this_node);
                } else {
                    batched_node_progs[next_loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                }
            }
        }
        deleted_nodes.clear();

        for (auto &handle_params : start_node_params) {
            node_handle = std::get<0>(handle_params);
            this_node.handle = node_handle;
            db::element::node *node = G->acquire_node(node_handle); // maybe use a try-lock later so forward progress can continue on other nodes in list
            if (node == NULL) {
                G->release_node(node);
                deleted_nodes.push_back(std::make_pair(node_handle, std::get<2>(handle_params)));
                std::cout << "Node deleted!\n";
            } else if (node->get_del_time() <= unpacked_request_id) {
                deleted_nodes.push_back(std::make_pair(node_handle, std::get<2>(handle_params)));
                std::cout << "Node deleted!\n";
            } else if (node->state == db::element::node::mode::IN_TRANSIT || node->state == db::element::node::mode::MOVED) {
                // queueing/forwarding node programs logic goes here
                std::unique_ptr<message::message> m(new message::message());
                std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> fwd_node_params;
                fwd_node_params.emplace_back(handle_params);
                message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, vclocks,
                    unpacked_request_id, fwd_node_params, dirty_cache_ids, invalid_cache_ids);
                G->mrequest.mutex.lock();
                G->mrequest.pending_requests.emplace_back(std::move(m));
                G->mrequest.mutex.unlock();
                G->release_node(node);
            } else { // node does exist
                // bind cache getter and putter function variables to functions
                node_state_getter = std::bind(get_node_state<NodeStateType>, G,
                        prog_type_recvd, unpacked_request_id, node_handle);
                cache_value_putter = std::bind(put_cache_value<CacheValueType>, G,
                        prog_type_recvd, unpacked_request_id, node_handle, node);
                cached_values_getter = std::bind(get_cached_values<CacheValueType>, G,
                        prog_type_recvd, unpacked_request_id, node_handle, &dirty_cache_ids, std::ref(invalid_cache_ids));
                // call node program
                auto next_node_params = enclosed_node_prog_func(unpacked_request_id, *node, this_node, 
                        std::get<1>(handle_params), // actual parameters for this node program
                        node_state_getter, 
                        cache_value_putter, 
                        cached_values_getter); 
                // batch the newly generated node programs for onward propagation
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    // signal to send back to coordinator
                    if (res.first.loc == -1) {
                        // XXX get rid of pair, without pair it is not working for some reason
                        std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                        message::prepare_message(msg, message::NODE_PROG, prog_type_recvd,
                            unpacked_request_id, dirty_cache_ids, temppair);
                        G->send_coord(msg.buf);
                    } else {
                        batched_node_progs[res.first.loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                        node->msg_count[res.first.loc]++;
                        G->msg_count_mutex.lock();
                        G->agg_msg_count.at(node_handle)++;
                        G->msg_count_mutex.unlock();
                    }
                }
                G->release_node(node);
            }
        }
        start_node_params = std::move(batched_node_progs[G->myid]);
    }

    // now propagate requests
    for (auto &batch_pair : batched_node_progs) {
        if (batch_pair.first == G->myid) {
            assert(batch_pair.second.empty());
        }
        // send msg to batch.first (location) with contents batch.second (start_node_params for that machine)
        message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, vclocks, unpacked_request_id, batch_pair.second, dirty_cache_ids, invalid_cache_ids);
        G->send(batch_pair.first, msg.buf);
    }
}


template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_start_coord(coordinator::central *server, message::message &msg, std::shared_ptr<coordinator::pending_req> request)
{
}

// unpack update request and call appropriate function
void
unpack_update_request(db::graph *G, void *req)
{
    db::update_request *request = (db::update_request *) req;
    uint64_t req_id;
    uint64_t start_time, time2;
    uint64_t n1, n2;
    uint64_t edge;
    int loc;
    common::property prop;
    uint32_t key;
    std::unique_ptr<std::vector<uint64_t>> cache;

    switch (request->type)
    {
        case message::NODE_CREATE_REQ:
            message::unpack_message(*request->msg, message::NODE_CREATE_REQ, start_time, req_id);
            handle_create_node(G, req_id);
            if (G->increment_clock()) {
                G->migr_func(G);
            }
            break;

        case message::NODE_DELETE_REQ:
            message::unpack_message(*request->msg, message::NODE_DELETE_REQ, start_time, req_id, n1);
            handle_delete_node(G, req_id, n1, std::move(cache));
            if (G->increment_clock()) {
                G->migr_func(G);
            }
            break;

        case message::EDGE_CREATE_REQ:
            message::unpack_message(*request->msg, message::EDGE_CREATE_REQ, start_time, req_id,
                n1, n2, loc, time2);
            handle_create_edge(G, req_id, n1, n2, loc, time2);
            if (G->increment_clock()) {
                G->migr_func(G);
            }
            break;

        case message::REVERSE_EDGE_CREATE:
            message::unpack_message(*request->msg, message::REVERSE_EDGE_CREATE, start_time, req_id, n2, loc, n1);
            handle_create_reverse_edge(G, req_id, n2, loc, n1);
            if (G->increment_clock()) {
                G->migr_func(G);
            }
            break;

        case message::EDGE_DELETE_REQ:
            message::unpack_message(*request->msg, message::EDGE_DELETE_REQ, start_time, req_id, n1, edge);
            handle_delete_edge(G, req_id, n1, edge, std::move(cache));
            if (G->increment_clock()) {
                G->migr_func(G);
            }
            break;

        case message::PERMANENT_DELETE_EDGE:
            message::unpack_message(*request->msg, message::PERMANENT_DELETE_EDGE, req_id, n1, edge);
            G->permanent_edge_delete(n1, edge);
            break;

        case message::EDGE_ADD_PROP:
            message::unpack_message(*request->msg, message::EDGE_ADD_PROP, start_time, req_id, n1, edge, prop);
            handle_add_edge_property(G, req_id, n1, edge, prop);
            if (G->increment_clock()) {
                G->migr_func(G);
            }
            break;

        case message::EDGE_DELETE_PROP:
            message::unpack_message(*request->msg, message::EDGE_DELETE_PROP, start_time, req_id, n1, edge, key);
            handle_delete_edge_property(G, req_id, n1, edge, key, std::move(cache));
            if (G->increment_clock()) {
                G->migr_func(G);
            }
            break;

        case message::CACHE_UPDATE:
            handle_cache_update(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP1:
            migrate_node_step2(G, std::move(request->msg));
            break;
        
        case message::COORD_NODE_MIGRATE_ACK:
            migrate_node_step3(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP4:
            migrate_node_step5(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP6:
            migrate_node_step7(G);
            break;

        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(G, std::move(request->msg));
            break;

        default:
            std::cerr << "Bad msg type in unpack_update_request" << std::endl;
    }
    delete request;
}

// assuming caller holds migration_lock
void
unpack_transit_update_request(db::graph *G, db::update_request *request)
{
    uint64_t req_id, update_count;
    uint64_t n2, edge;
    int loc;
    uint64_t time;
    common::property prop;
    uint32_t key;
    std::unique_ptr<std::vector<uint64_t>> cache;

    switch (request->type)
    {
        case message::TRANSIT_NODE_DELETE_REQ:
            cache.reset(new std::vector<uint64_t>());
            message::unpack_message(*request->msg, message::TRANSIT_NODE_DELETE_REQ, update_count, req_id, *cache);
            handle_delete_node(G, req_id, G->migr_node, std::move(cache));
            break;

        case message::TRANSIT_EDGE_CREATE_REQ:
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_CREATE_REQ, update_count, req_id, n2, loc, time);
            handle_create_edge(G, req_id, G->migr_node, n2, loc, time);
            break;

        case message::TRANSIT_REVERSE_EDGE_CREATE:
            message::unpack_message(*request->msg, message::TRANSIT_REVERSE_EDGE_CREATE, update_count, req_id, n2, loc);
            handle_create_reverse_edge(G, req_id, n2, loc, G->migr_node);
            break;
        
        case message::TRANSIT_EDGE_DELETE_REQ:
            cache.reset(new std::vector<uint64_t>());
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_DELETE_REQ, update_count, req_id, edge, *cache);
            handle_delete_edge(G, req_id, G->migr_node, edge, std::move(cache));
            break;

        case message::TRANSIT_EDGE_ADD_PROP:
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_ADD_PROP, update_count, req_id, edge, prop);
            handle_add_edge_property(G, req_id, G->migr_node, edge, prop);
            break;

        case message::TRANSIT_EDGE_DELETE_PROP:
            cache.reset(new std::vector<uint64_t>());
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_DELETE_PROP, update_count, req_id, edge, key, *cache);
            handle_delete_edge_property(G, req_id, G->migr_node, edge, key, std::move(cache));
            break;

        default:
            std::cerr << "Bad msg type in unpack_transit_update_request" << std::endl;
    }
}

void
process_pending_updates(db::graph *G, void *req)
{
    db::update_request *request = (db::update_request *)req;
    delete request;
    G->migration_mutex.lock();
    while (!G->pending_updates.empty()) {
        request = G->pending_updates.top();
        if (G->current_update_count == (request->start_time-1)) {
            G->pending_updates.pop();
            G->migration_mutex.unlock();
            unpack_transit_update_request(G, request);
            delete request;
            G->migration_mutex.lock();
            G->current_update_count++;
            if (G->pending_update_count == G->current_update_count) {
                // processed all pending updates
                db::element::node *n = G->acquire_node(G->migr_node);
                n->state = db::element::node::mode::STABLE;
                G->release_node(n);
                if (G->set_callback(G->new_shard_target_clock, migrate_node_step6)) {
                    migrate_node_step6(G);
                }
            }
        }
    }
    G->migration_mutex.unlock();
}


// server loop for the shard server
void
runner(db::graph *G)
{
    busybee_returncode ret;
    po6::net::location sender(SHARD_IPADDR, COORD_PORT);
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    message::message msg;
    db::thread::unstarted_thread *thr;
    db::update_request *request;
    uint64_t done_id;
    uint64_t start_time, update_count;
    // used for node programs
    node_prog::prog_type pType;
    std::vector<uint64_t> vclocks;

    while (true) {
        if ((ret = G->bb_recv.recv(&sender, &msg.buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg.reset(new message::message(msg));
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        rec_msg->change_type(mtype);
        switch (mtype)
        {
            case message::NODE_CREATE_REQ:
            case message::NODE_DELETE_REQ:
            case message::EDGE_CREATE_REQ:
            case message::REVERSE_EDGE_CREATE:
            case message::EDGE_DELETE_REQ:
            case message::EDGE_ADD_PROP:
            case message::EDGE_DELETE_PROP:
                rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(mtype)) >> start_time;
                request = new db::update_request(mtype, start_time - 1, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(start_time - 1, unpack_update_request, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::TRANSIT_NODE_DELETE_REQ:
            case message::TRANSIT_EDGE_CREATE_REQ:
            case message::TRANSIT_REVERSE_EDGE_CREATE:
            case message::TRANSIT_EDGE_DELETE_REQ:
            case message::TRANSIT_EDGE_ADD_PROP:
            case message::TRANSIT_EDGE_DELETE_PROP:
                rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(mtype)) >> update_count >> start_time;
                request = new db::update_request(mtype, update_count, std::move(rec_msg));
                G->migration_mutex.lock();
                G->pending_updates.push(request);
                G->migration_mutex.unlock();
                request = new db::update_request(mtype, 0);
                thr = new db::thread::unstarted_thread(0, process_pending_updates, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::CACHE_UPDATE:
            case message::MIGRATE_NODE_STEP1:
            case message::COORD_NODE_MIGRATE_ACK:
            case message::MIGRATE_NODE_STEP4:
            case message::MIGRATE_NODE_STEP6:
            case message::MIGRATED_NBR_UPDATE:
            case message::PERMANENT_DELETE_EDGE:
            case message::PERMANENT_DELETE_EDGE_ACK:
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, unpack_update_request, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::NODE_PROG:
                vclocks.clear();
                message::unpack_message(*rec_msg, message::NODE_PROG, pType, vclocks);
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(vclocks[G->myid], unpack_and_run_node_program, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::MIGRATION_TOKEN:
                std::cout << "Now obtained migration token\n";
                thr = new db::thread::unstarted_thread(0, shard_daemon_begin, G, NULL);
                G->thread_pool.add_request(thr);
                break;

            default:
                std::cerr << "unexpected msg type " << (message::CLIENT_REPLY == code) << std::endl;
        }
    }
}

void
migration_wrapper(db::graph *G)
{
//    std::cout << "\tMigration wrapper begin\n";
    bool no_migr = true;
    while (!G->sorted_nodes.empty()) {
        db::element::node *n;
        size_t max_pos;
        uint64_t migr_node = G->sorted_nodes.front().first;
        n = G->acquire_node(migr_node);
        if (n == NULL || n->get_del_time() < MAX_TIME || n->dependent_del > 0) {
            G->release_node(n);
            continue;
        }
        n->updated = false;
        for (size_t j = 0; j < NUM_SHARDS; j++) {
            n->agg_msg_count[j] += (uint64_t)(0.8 * (double)(n->msg_count[j]));
        }
        max_pos = 0;
        for (size_t j = 1; j < n->agg_msg_count.size(); j++) {
            if (n->agg_msg_count.at(max_pos) < n->agg_msg_count.at(j)) {
                max_pos = j;
            }
        }
        for (uint32_t &cnt: n->msg_count) {
            cnt = 0;
        }
        G->release_node(n);
        G->sorted_nodes.pop_front();
        if (max_pos != G->myid) {
            migrate_node_step1(G, migr_node, max_pos);
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
//        std::cout << "Migration wrapper end\n";
        shard_daemon_end(G);
    }
}

bool agg_count_compare(std::pair<uint64_t, uint32_t> p1, std::pair<uint64_t, uint32_t> p2)
{
    return (p1.second < p2.second);
}

void
shard_daemon_begin(db::graph *G, void *dummy)
{
//    std::cout << "Starting shard daemon\n";
    std::deque<std::pair<uint64_t, uint32_t>> &sn = G->sorted_nodes;
    std::chrono::seconds duration(10); // execution frequency in seconds
    std::this_thread::sleep_for(duration);
//    std::cout << "Shard daemon woken up, going to start migration process\n";
    sn.clear();
    G->msg_count_mutex.lock();
    for (auto &p: G->agg_msg_count) {
        sn.emplace_back(p);
    }
    G->msg_count_mutex.unlock();
    std::sort(sn.begin(), sn.end(), agg_count_compare);
    size_t num_nodes = sn.size();
    sn.erase(sn.begin() + num_nodes/2, sn.end());
//    std::cout << "Migr nodes size = " << sn.size() << std::endl;
    migration_wrapper(G);
}

void
shard_daemon_end(db::graph *G)
{
    message::message msg;
    message::prepare_message(msg, message::MIGRATION_TOKEN);
    G->send((G->myid+1) % NUM_SHARDS, msg.buf);
}

int
main(int argc, char* argv[])
{
    std::thread *t;
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <myid> <ipaddr> <port> " << std::endl;
        return -1;
    }
    db::graph G(atoi(argv[1]) - 1, argv[2], atoi(argv[3]));
    std::cout << "Weaver: shard instance " << G.myid << std::endl;

    void *dummy = NULL;
    if (G.myid == 1) {
        t = new std::thread(&shard_daemon_begin, &G, dummy);
        t->detach();
    }
    runner(&G);

    return 0;
}

#endif
