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
void migrate_node_step4(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step4_1(db::graph *G);
void migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step6(db::graph *G);


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
    std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_node(node_handle, req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first) {
        message::prepare_message(*msg, message::NODE_DELETE_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_NODE_DELETE_REQ, req_id, *success.second);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// create a graph edge
inline void
handle_create_edge(db::graph *G, uint64_t req_id, uint64_t n1, uint64_t n2, int loc2, uint64_t tc2)
{
    if (!G->create_edge(n1, req_id, n2, loc2, tc2)) {
        std::unique_ptr<message::message> msg(new message::message());
        message::prepare_message(*msg, message::TRANSIT_EDGE_CREATE_REQ, req_id, n2, loc2, tc2);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// create a back pointer for an edge
inline void
handle_create_reverse_edge(db::graph *G, uint64_t req_id, uint64_t remote_node, int remote_loc, uint64_t local_node)
{
    if (!G->create_reverse_edge(req_id, local_node, remote_node, remote_loc)) {
        std::unique_ptr<message::message> msg(new message::message());
        message::prepare_message(*msg, message::TRANSIT_REVERSE_EDGE_CREATE, req_id, remote_node, remote_loc);
        G->queue_transit_node_update(0, std::move(msg));
    }
}

// delete an edge
inline void
handle_delete_edge(db::graph *G, uint64_t req_id, uint64_t n, uint64_t e, std::unique_ptr<std::vector<uint64_t>> cache)
{
    std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_edge(n, e, req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first) {
        message::prepare_message(*msg, message::EDGE_DELETE_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_EDGE_DELETE_REQ, req_id, e, *success.second);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// add edge property
inline void
handle_add_edge_property(db::graph *G, uint64_t req_id, uint64_t node_addr, uint64_t edge_addr, common::property &prop)
{
    if (!G->add_edge_property(node_addr, edge_addr, prop)) {
        std::unique_ptr<message::message> msg(new message::message());
        message::prepare_message(*msg, message::TRANSIT_EDGE_ADD_PROP, req_id, edge_addr, prop);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// delete all edge properties with the given key
inline void
handle_delete_edge_property(db::graph *G, uint64_t req_id, uint64_t node_addr, uint64_t edge_addr, uint32_t key,
    std::unique_ptr<std::vector<uint64_t>> cache)
{
    std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_all_edge_property(node_addr, edge_addr, key, req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first) {
        message::prepare_message(*msg, message::EDGE_DELETE_PROP_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_EDGE_DELETE_PROP, req_id, edge_addr, key, *success.second);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// send node information to new shard
// mark node as "in_transit" so that subsequent requests are queued up
void
migrate_node_step1(db::graph *G, uint64_t node_handle, int shard)
{
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->acquire_node(node_handle);
    n->state = db::element::node::mode::IN_TRANSIT;
    n->new_loc = shard;
    // pack entire node info in a ginormous message
    message::message msg(message::MIGRATE_NODE_STEP1);
    G->mrequest.cur_node = node_handle;
    G->mrequest.new_loc = shard;
    G->mrequest.num_pending_updates = 0;
    G->mrequest.my_clock = 0;
    std::vector<std::unique_ptr<message::message>>().swap(G->mrequest.pending_updates);
    std::vector<uint64_t>().swap(G->mrequest.pending_update_ids);
    std::vector<std::shared_ptr<db::batch_request>>().swap(G->mrequest.pending_requests);
    message::prepare_message(msg, message::MIGRATE_NODE_STEP1, n->get_creat_time(), *n, G->myid);
    G->release_node(n);
    G->mrequest.mutex.unlock();
    G->send(shard, msg.buf);
}

// Receive and place a node which has been migrated to this shard
void
migrate_node_step2(db::graph *G, std::unique_ptr<message::message> msg)
{
    int from_loc;
    uint64_t node_handle;
    db::element::node *n;
    // create a new node, unpack the message
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP1, node_handle);
    G->create_node(node_handle, true);
    n = G->acquire_node(node_handle);
    // TODO change at coordinator so that user does not have to enter node for edge
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP1, node_handle, *n, from_loc);
    n->prev_loc = from_loc;
    G->release_node(n);
    message::prepare_message(*msg, message::MIGRATE_NODE_STEP2);
    G->send(from_loc, msg->buf);
}

// send migration information to coord
void
migrate_node_step3(db::graph *G, std::unique_ptr<message::message> msg)
{
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->acquire_node(G->mrequest.cur_node);
    message::prepare_message(*msg, message::COORD_NODE_MIGRATE, G->mrequest.cur_node, n->new_loc, G->myid);
    G->release_node(n);
    G->send_coord(msg->buf);
    G->mrequest.mutex.unlock();
}

// wait for updates till the received clock value, which are being forwarded to new shard
void
migrate_node_step4(db::graph *G, std::unique_ptr<message::message> msg)
{
    uint64_t my_clock;
    message::unpack_message(*msg, message::COORD_NODE_MIGRATE_ACK, my_clock);
    if (G->set_target_clock(my_clock)) {
        migrate_node_step4_1(G);
    }
}

// forward queued update requests
void
migrate_node_step4_1(db::graph *G)
{
    message::message msg;
    G->mrequest.mutex.lock();
    // TODO check this logic. Won't msg reordering affect this?
    message::prepare_message(msg, message::MIGRATE_NODE_STEP4, G->mrequest.pending_update_ids);
    G->send(G->mrequest.new_loc, msg.buf);
    for (auto &m: G->mrequest.pending_updates) {
        G->send(G->mrequest.new_loc, m->buf);
    }
    G->mrequest.mutex.unlock();
}

// receive the number of pending updates for the newly migrated node
void
migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::vector<uint64_t> update_ids;
    db::element::node *n = G->acquire_node(G->migr_node);
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP4, update_ids);
    if (update_ids.empty()) {
        n->state = db::element::node::mode::STABLE;
        message::prepare_message(*msg, message::MIGRATE_NODE_STEP5);
        G->send(n->prev_loc, msg->buf);
        G->release_node(n);
    } else {
        G->release_node(n);
        G->set_update_ids(update_ids);
    }
}

// inform other shards of migration
// forward queued traversal requests to new location
void
migrate_node_step6(db::graph *G)
{
    message::message msg;
    std::vector<uint64_t> nodes;
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->acquire_node(G->mrequest.cur_node);
    n->state = db::element::node::mode::MOVED;
    // inform all in-nbrs of new location
    for (auto &nbr: n->in_edges) {
        message::prepare_message(msg, message::MIGRATED_NBR_UPDATE, nbr.second->nbr.handle, 
            G->mrequest.cur_node, G->myid, G->mrequest.migr_node, G->mrequest.new_loc);
        G->send(nbr.second->nbr.loc, msg.buf);
    }
    nodes.push_back(G->mrequest.migr_node);
    for (auto &r: G->mrequest.pending_requests) {
        r->lock();
        //G->propagate_request(nodes, r, G->mrequest.new_loc); TODO
        r->unlock();
    }
    G->release_node(n);
    G->mrequest.mutex.unlock();
}

void
migrated_nbr_update(db::graph *G, std::unique_ptr<message::message> msg)
{
    uint64_t local_node, orig_node, new_node;
    int orig_loc, new_loc;
    message::unpack_message(*msg, message::MIGRATED_NBR_UPDATE, local_node,
        orig_node, orig_loc, new_node, new_loc);
    G->update_migrated_nbr(local_node, orig_node, orig_loc, new_node, new_loc);
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
    std::cout << "Permanent delete, id = " << perm_del_id << std::endl;
}

template <typename NodeStateType>
NodeStateType& get_node_state(db::graph *G, node_prog::prog_type pType, uint64_t req_id, uint64_t node_handle){
        NodeStateType *toRet = new NodeStateType();
        if (G->prog_req_state_exists(pType, req_id, node_handle)) {
            //std::cout << "geting existing NodeStateType handle" << node_handle << std::endl;
            toRet = dynamic_cast<NodeStateType *>(G->fetch_prog_req_state(pType, req_id, node_handle));
            if (toRet == NULL) {
                std::cerr << "NodeStateType needs to extend Deletable" << std::endl;
            }
        } else {
            //std::cout << "making new NodeStateType hanlde "<< node_handle << std::endl;
            toRet = new NodeStateType();
            G->insert_prog_req_state(pType, req_id, node_handle, toRet);
        }
        //std::cout << "get node state " << toRet << std::endl;
        return *toRet;
}

template <typename CacheValueType>
std::vector<CacheValueType *> get_cached_values(db::graph *G, node_prog::prog_type pType, uint64_t req_id, uint64_t node_handle, std::vector<uint64_t> *dirty_list_ptr, std::unordered_set<uint64_t>& ignore_set)
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
    std::vector<std::pair<uint64_t, uint64_t>> deleted_nodes; // node that was deleted and it's handle

    // map from loc to send back to to handle that was deleted and the handle to send back to
    std::unordered_map<int, std::vector<std::pair<uint64_t, uint64_t>>> batched_deleted_nodes; 
    uint64_t cur_node;
    std::vector<uint64_t> dirty_cache_ids; // cache values used by user that we need to verify are good at coord
    std::unordered_set<uint64_t> invalid_cache_ids; // cache values from coordinator we know are invalid
    // map from location to send to next to tuple of handle and params to send to next, and node that sent them
    std::unordered_map<int, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> batched_node_progs; // node programs that will be propagated
    db::element::remote_node this_node(G->myid, 0);
    uint64_t node_handle;

    // unpack the node program
    message::unpack_message(msg, message::NODE_PROG, prog_type_recvd, vclocks,
            unpacked_request_id, start_node_params, dirty_cache_ids, invalid_cache_ids, deleted_nodes);

    // node state and cache functions
    std::function<NodeStateType&()> node_state_getter;
    std::function<CacheValueType&()> cache_value_putter;
    std::function<std::vector<CacheValueType *>()> cached_values_getter;

    while (!start_node_params.empty() || !deleted_nodes.empty()) {
        //for (std::pair<uint64_t, db::element::remote_node> del_node: deleted_nodes) {
        for (std::pair<uint64_t, uint64_t> del_node: deleted_nodes) {
            //std::cout << "in del nodes "<< del_node << std::endl;

            size_t parent_handle = del_node.second;

            db::element::node *node = G->acquire_node(parent_handle); // parent should definately exist
            std::cout << " going back to parent " << parent_handle << " after deleted" << std::endl;
            //this_node.handle = handle;
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

        // std::cout << "^^^ about to loop over local programs size " << start_node_params.size() << std::endl;
        for (auto &handle_params : start_node_params) {
            node_handle = std::get<0>(handle_params);
            this_node.handle = node_handle;

            // std::cout << "not stuck here 1" << std::endl;
            // std::cout << "about %%% to acquire node" << std::endl;

            db::element::node *node = G->acquire_node(node_handle); // maybe use a try-lock later so forward progress can continue on other nodes in list

            // std::cout << "@@@@aquiriring node " << node_handle << " has address " << node << std::endl;

            if (node == NULL) {
                // std::cout << "FOUND A DEL NODE:" << node_handle << std::endl;
                deleted_nodes.push_back(std::make_pair(node_handle, std::get<2>(handle_params)));

            } else if (node->get_del_time() <= unpacked_request_id) {
                G->release_node(node);
                // std::cout << "FOUND A DEL NODE:" << node_handle << std::endl;
                db::element::remote_node parent = std::get<2>(handle_params);
                deleted_nodes.push_back(std::make_pair(node_handle, std::get<2>(handle_params)));

            } else { // node does exist
                // bind cache getter and putter function variables to functions
                // std::cout << "&&& making getter functions" << std::endl;
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
                G->release_node(node);
                std::cout << "node program propagating to " << next_node_params.size() << " more nodes" << std::endl;

                // batch the newly generated node programs for onward propagation
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    // signal to send back to coordinator
                    if (res.first.loc == -1) {
                        // std::cout << "not stuck here coord" << std::endl;
                        // XXX get rid of pair, without pair it is not working for some reason
                        std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                        message::prepare_message(msg, message::NODE_PROG, prog_type_recvd,
                                unpacked_request_id, dirty_cache_ids, temppair);
                        G->send_coord(msg.buf);
                    } else {
                        batched_node_progs[res.first.loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                        // std::cout << "not stuck here batch of size "<<  batched_node_progs[res.first.loc].size() << std::endl;
                    }
                }
            }
        }
        start_node_params = std::move(batched_node_progs[G->myid]);
    }

    // std::cout << "not stuck here 2" << std::endl;
    // std::cout << "### about to propagate to other shards size " << batched_node_progs.size() << std::endl;

    // now propagate requests
    for (auto &batch_pair : batched_node_progs) {
        if (batch_pair.first == G->myid) {
            assert(batch_pair.second.empty());
        }
        // send msg to batch.first (location) with contents batch.second (start_node_params for that machine)
        message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, vclocks, unpacked_request_id, batch_pair.second, dirty_cache_ids, invalid_cache_ids);
        // std::cout << "not stuck here mids" << std::endl;
        G->send(batch_pair.first, msg.buf);
    }
    // std::cout << "not stuck here 3" << std::endl;
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
                migrate_node_step4_1(G);
            }
            break;

        case message::NODE_DELETE_REQ:
            message::unpack_message(*request->msg, message::NODE_DELETE_REQ, start_time, req_id, n1);
            handle_delete_node(G, req_id, n1, std::move(cache));
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::EDGE_CREATE_REQ:
            message::unpack_message(*request->msg, message::EDGE_CREATE_REQ, start_time, req_id,
                n1, n2, loc, time2);
            handle_create_edge(G, req_id, n1, n2, loc, time2);
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::REVERSE_EDGE_CREATE:
            message::unpack_message(*request->msg, message::REVERSE_EDGE_CREATE, start_time, req_id, n2, loc, n1);
            handle_create_reverse_edge(G, req_id, n2, loc, n1);
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::EDGE_DELETE_REQ:
            message::unpack_message(*request->msg, message::EDGE_DELETE_REQ, start_time, req_id, n1, edge);
            handle_delete_edge(G, req_id, n1, edge, std::move(cache));
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
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
                migrate_node_step4_1(G);
            }
            break;

        case message::EDGE_DELETE_PROP:
            message::unpack_message(*request->msg, message::EDGE_DELETE_PROP, start_time, req_id, n1, edge, key);
            handle_delete_edge_property(G, req_id, n1, edge, key, std::move(cache));
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::CACHE_UPDATE:
            handle_cache_update(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP1:
            migrate_node_step2(G, std::move(request->msg));
            break;
        
        case message::MIGRATE_NODE_STEP2:
            migrate_node_step3(G, std::move(request->msg));
            break;

        case message::COORD_NODE_MIGRATE_ACK:
            migrate_node_step4(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP4:
            migrate_node_step5(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP5:
            migrate_node_step6(G);
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
    uint64_t req_id;
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
            message::unpack_message(*request->msg, message::TRANSIT_NODE_DELETE_REQ, req_id, *cache);
            handle_delete_node(G, req_id, G->migr_node, std::move(cache));
            break;

        case message::TRANSIT_EDGE_CREATE_REQ:
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_CREATE_REQ, req_id, n2, loc, time);
            handle_create_edge(G, req_id, G->migr_node, n2, loc, time);
            break;

        case message::TRANSIT_REVERSE_EDGE_CREATE:
            message::unpack_message(*request->msg, message::TRANSIT_REVERSE_EDGE_CREATE, req_id, n2, loc);
            handle_create_reverse_edge(G, req_id, n2, loc, G->migr_node);
            break;
        
        case message::TRANSIT_EDGE_DELETE_REQ:
            cache.reset(new std::vector<uint64_t>());
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_DELETE_REQ, req_id, edge, *cache);
            handle_delete_edge(G, req_id, G->migr_node, edge, std::move(cache));
            break;

        case message::TRANSIT_EDGE_ADD_PROP:
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_ADD_PROP, req_id, edge, prop);
            handle_add_edge_property(G, req_id, G->migr_node, edge, prop);
            break;

        case message::TRANSIT_EDGE_DELETE_PROP:
            cache.reset(new std::vector<uint64_t>());
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_DELETE_PROP, req_id, edge, key, *cache);
            handle_delete_edge_property(G, req_id, G->migr_node, edge, key, std::move(cache));
            break;

        default:
            std::cerr << "Bad msg type in unpack_transit_update_request" << std::endl;
    }
}

void
process_pending_updates(db::graph *G, void *req)
{
    db::update_request *request = (db::update_request *) req;
    uint64_t req_id;
    request->msg->buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum message::msg_type)) >> req_id;
    db::update_request *r = new db::update_request(request->type, req_id, std::move(request->msg));
    G->migration_mutex.lock();
    G->pending_updates.push(r);
    while (G->pending_update_ids.front() == G->pending_updates.top()->start_time) {
        unpack_transit_update_request(G, G->pending_updates.top());
        G->pending_updates.pop();
        G->pending_update_ids.pop_front();
        if (G->pending_update_ids.empty()) {
            message::message msg;
            db::element::node *n = G->acquire_node(G->migr_node);
            n->state = db::element::node::mode::STABLE;
            message::prepare_message(msg, message::MIGRATE_NODE_STEP5);
            G->send(n->prev_loc, msg.buf);
            G->release_node(n);
            break;
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
    uint64_t start_time;
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
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, process_pending_updates, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::CACHE_UPDATE:
            case message::MIGRATE_NODE_STEP1:
            case message::MIGRATE_NODE_STEP2:
            case message::COORD_NODE_MIGRATE_ACK:
            case message::MIGRATE_NODE_STEP4:
            case message::MIGRATE_NODE_STEP5:
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

            default:
                std::cerr << "unexpected msg type " << (message::CLIENT_REPLY ==
                code) << std::endl;
        }
    }
}

void
shard_daemon(db::graph *G)
{
    std::unordered_map<uint64_t, std::vector<uint64_t>> *next_map;
    while (true) {
        std::chrono::seconds duration(40); // execution frequency in seconds
        std::this_thread::sleep_for(duration);
    }
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

    t = new std::thread(&shard_daemon, &G);
    t->detach();
    runner(&G);

    return 0;
} 

#endif
