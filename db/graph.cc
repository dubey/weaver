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
#include <signal.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#include "common/weaver_constants.h"
#include "common/message_graph_elem.h"
#include "graph.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
#include "node_prog/dijkstra_program.h"
#include "node_prog/reach_program.h"
#include "node_prog/clustering_program.h"

// migration methods
void migrate_node_step1(db::graph *G, uint64_t node_handle, uint64_t new_shard);
void migrate_node_step2(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step3(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step4(db::graph *G);
void migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step6(db::graph *G);
void migrate_node_step7a(db::graph *G);
void migrate_node_step7b(db::graph *G);
void process_pending_updates(db::graph *G, void *req);
void migration_wrapper(db::graph *G);
void continue_migration(db::graph *G, uint64_t migr_node);
timespec diff(timespec start, timespec end);

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
        G->send(COORD_ID, msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_NODE_DELETE_REQ, success.first, req_id, *success.second);
        G->fwd_transit_node_update(std::move(msg));
    }
}

// create a graph edge
inline void
handle_create_edge(db::graph *G, uint64_t req_id, uint64_t n1, uint64_t n2, uint64_t loc2, uint64_t tc2)
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
handle_create_reverse_edge(db::graph *G, uint64_t req_id, uint64_t remote_node, uint64_t remote_loc, uint64_t local_node)
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
        G->send(COORD_ID, msg->buf);
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
        G->send(COORD_ID, msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_EDGE_DELETE_PROP, req_id, edge_addr, key, *success.second);
        G->fwd_transit_node_update(std::move(msg));
    }
}

// send node information to new shard
// mark node as "in transit" so that subsequent requests are queued up
// send migration information to coord
void
migrate_node_step1(db::graph *G, uint64_t node_handle, uint64_t shard)
{
    //DEBUG << "Starting step1, node handle = ";
    //DEBUG << node_handle << std::endl;
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->acquire_node(node_handle);
    if (n->updated) {
        G->release_node(n);
        G->mrequest.mutex.unlock();
        DEBUG << "Canceling migration!!!\n";
        migration_wrapper(G);
    } else {
        // mark node as "in transit"
        n->state = db::element::node::mode::IN_TRANSIT;
        n->new_loc = shard;
        // pack entire node info in a ginormous message and send to new loc
        message::message msg(message::MIGRATE_NODE_STEP1);
        G->mrequest.prev_node = G->mrequest.cur_node; // TODO corner case may prevent deletion
        G->mrequest.cur_node = node_handle;
        G->mrequest.new_loc = shard;
        G->mrequest.migr_clock = 0;
        G->mrequest.other_clock = 0;
        G->mrequest.start_step4 = 0;
        G->mrequest.start_next_round = 0;
        std::vector<std::unique_ptr<message::message>>().swap(G->mrequest.pending_requests);
        message::prepare_message(msg, message::MIGRATE_NODE_STEP1, node_handle, G->myid, *n);
        G->send(shard, msg.buf);
        // invalidating cache
        // TODO can we avoid cache invalidation?
        std::unique_ptr<std::vector<uint64_t>> cached_ids = n->purge_cache();
        for (auto rid: *cached_ids) {
            G->invalidate_prog_cache(rid);
        }
        // send new loc information to coordinator
        message::prepare_message(msg, message::COORD_NODE_MIGRATE, G->mrequest.cur_node, n->new_loc, *cached_ids);
        G->send(COORD_ID, msg.buf);
        G->release_node(n);
        G->mrequest.mutex.unlock();
        //DEBUG << "Ending step1\n";
    }
}

// Receive and place a node which has been migrated to this shard
void
migrate_node_step2(db::graph *G, std::unique_ptr<message::message> msg)
{
//    DEBUG << "Starting step2\n";
    uint64_t from_loc;
    uint64_t node_handle;
    db::element::node *n;
    // create a new node, unpack the message
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP1, node_handle);
    G->create_node(node_handle, true);
    n = G->acquire_node(node_handle);
    std::vector<uint64_t>().swap(n->agg_msg_count);
    // TODO change at coordinator so that user does not have to enter node for edge
    try {
        message::unpack_message(*msg, message::MIGRATE_NODE_STEP1, node_handle, from_loc, *n);
    } catch (std::bad_alloc& ba) {
        DEBUG << "bad_alloc caught " << ba.what() << std::endl;
        while(1);
    }
    n->prev_loc = from_loc;
//    DEBUG << "Unpacked node, got update count = " << n->update_count << std::endl;
    G->release_node(n);
    message::prepare_message(*msg, message::MIGRATE_NODE_STEP2, 0);
    G->send(from_loc, msg->buf);
//    DEBUG << "Ending step2\n";
}

// receive last update clock value from coord
void
migrate_node_step3(db::graph *G, std::unique_ptr<message::message> msg)
{
//    DEBUG << "Starting step3\n";
    uint64_t my_clock, other_clock, global_clock;
    message::unpack_message(*msg, message::COORD_NODE_MIGRATE_ACK, my_clock, other_clock, global_clock);
    G->mrequest.mutex.lock();
    G->mrequest.migr_clock = my_clock;
    G->mrequest.other_clock = other_clock;
    if (G->mrequest.prev_node != 0) {
        G->migrated_nodes.emplace_back(std::make_pair(global_clock, G->mrequest.prev_node));
    }
    G->mrequest.mutex.unlock();
//    DEBUG << "Ending step3\n";
    if (G->set_callback(my_clock, migrate_node_step4)) {
        migrate_node_step4(G);
    }
}

// all pre-migration time updates have been received
void
migrate_node_step4(db::graph *G)
{
    message::message msg;
    db::element::node *n;
    G->mrequest.mutex.lock();
    if (++G->mrequest.start_step4 < 2) {
        // cannot start step 4 yet, waiting for another ack
        // from either coordinator or new shard
//        DEBUG << "Step4 start deferred for another ack\n";
        G->mrequest.mutex.unlock();
        return;
    } else {
//        DEBUG << "Starting step4\n";
        n = G->acquire_node(G->mrequest.cur_node);
        message::prepare_message(msg, message::MIGRATE_NODE_STEP4, n->update_count, G->mrequest.other_clock);
        G->send(G->mrequest.new_loc, msg.buf);
        G->release_node(n);
        G->mrequest.mutex.unlock();
//        DEBUG << "Ending step4\n";
    }
}

// receive the number of pending updates for the newly migrated node
void
migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg)
{
//    DEBUG << "Starting step5\n";
    uint64_t update_count, cur_update_count, target_clock;
    G->migration_mutex.lock();
    db::element::node *n = G->acquire_node(G->migr_node);
    G->migration_mutex.unlock();
//    DEBUG << "Acquired node in step 5\n";
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP4, update_count, target_clock);
    if (n->update_count == update_count) {
        // no pending updates left
        n->state = db::element::node::mode::STABLE;
//        DEBUG << "No pending updates, yay!\n";
//        DEBUG << "Ending step5\n";
        G->release_node(n);
        if (G->set_callback(target_clock, migrate_node_step6)) {
            migrate_node_step6(G);
        }
    } else {
        // some updates yet to trickle through
        cur_update_count = n->update_count;
//        DEBUG << "Pending updates, boo! Target count = " << update_count << ", current count " << n->update_count << std::endl;
//        DEBUG << "Ending step5\n";
        G->release_node(n);
        G->set_update_count(update_count, cur_update_count, target_clock);
        process_pending_updates(G, NULL);
    }
}

void
initiate_nbr_update(db::graph *G, db::element::node *n, std::pair<const uint64_t, db::element::edge*> &nbr,
    uint64_t migr_node, std::vector<std::pair<uint64_t, uint64_t>> &local_nbr_updates, uint64_t &pending_edge_updates, message::message &msg)
{
    if (nbr.second->nbr.loc == G->myid) {
        //DEBUG << "Local nbr update\n";
        local_nbr_updates.emplace_back(std::make_pair(nbr.second->nbr.handle, nbr.first));
        //if (nbr.second->nbr.handle == migr_node) {
        //    // self-edges
        //    G->update_migrated_nbr_nonlocking(n, nbr.first, migr_node, G->myid);
        //} else {
        //    // edge to local node
        //    G->update_migrated_nbr(nbr.second->nbr.handle, nbr.first, migr_node, G->myid);
        //}
    } else {
        // remote node, need to send msg
        //DEBUG << "Remote nbr update, sending msg\n";
        message::prepare_message(msg, message::MIGRATED_NBR_UPDATE, nbr.second->nbr.handle, nbr.first, migr_node, G->myid);
        G->send(nbr.second->nbr.loc, msg.buf);
        pending_edge_updates++;
    }
}

// inform other shards of migration
// Caution: assuming we have acquired node lock
void
migrate_node_step6(db::graph *G)
{
//    DEBUG << "Starting step6 for node handle ";
    uint64_t migr_node;
    uint64_t pending_edge_updates = 0;
    uint64_t prev_loc;
    std::vector<std::pair<uint64_t, uint64_t>> local_nbr_updates;
    G->increment_clock(); // let subsequent updates come through now
    G->migration_mutex.lock();
    migr_node = G->migr_node;
    //DEBUG << migr_node << std::endl;
    assert(G->pending_edge_updates == 0);
    G->migration_mutex.unlock();
    db::element::node *n = G->acquire_node(migr_node);
    // signal previous loc to forward pending node programs
    message::message msg;
    message::prepare_message(msg, message::MIGRATE_NODE_STEP6a);
    G->send(n->prev_loc, msg.buf);
    // update in-neighbors
//    DEBUG << "No. of edges = " << n->in_edges.size() << std::endl;
    int edge_cnt = 0;
    for (auto &nbr: n->in_edges) {
        initiate_nbr_update(G, n, nbr, migr_node, local_nbr_updates, pending_edge_updates, msg);
        edge_cnt++;
    }
    for (auto &nbr: n->out_edges) {
        initiate_nbr_update(G, n, nbr, migr_node, local_nbr_updates, pending_edge_updates, msg);
        edge_cnt++;
    }
    prev_loc = n->prev_loc;
    G->release_node(n);
    for (auto &u: local_nbr_updates) {
        G->update_migrated_nbr(u.first, u.second, migr_node, G->myid);
    }
    G->migration_mutex.lock();
    G->pending_edge_updates += pending_edge_updates;
    if (G->pending_edge_updates == 0) {
        //DEBUG << "No pending nbr updates\n";
        message::prepare_message(msg, message::MIGRATE_NODE_STEP6b);
        G->send(prev_loc, msg.buf);
    } else {
//        DEBUG << "Set pending edge updates to " << G->pending_edge_updates << std::endl;
    }
    G->migration_mutex.unlock();
//    DEBUG << "Ending step6\n";
}

// forward queued traversal requests to new location
void
migrate_node_step7a(db::graph *G)
{
//    DEBUG << "Starting step7a\n";
    message::message msg;
    std::vector<uint64_t> nodes;
    db::element::node *n;
    uint64_t migr_clock, migr_node;
    uint64_t req_cnt = 0;
    bool call_wrapper = false;
    G->mrequest.mutex.lock();
    migr_clock = G->mrequest.migr_clock;
    migr_node = G->mrequest.cur_node;
    n = G->acquire_node(migr_node);
    n->state = db::element::node::mode::MOVED;
    for (auto &r: G->mrequest.pending_requests) {
        G->send(G->mrequest.new_loc, r->buf);
//        DEBUG << "Forwarded request " << (++req_cnt) << std::endl;
    }
    G->release_node(n);
    if (++G->mrequest.start_next_round == 2) {
        call_wrapper = true;
    } else {
//        DEBUG << "STEP 7A: Call wrapper deferred, current count = " << (int)G->mrequest.start_next_round << std::endl;
    }
    G->mrequest.mutex.unlock();
    //G->migrated_nodes.emplace_back(std::make_pair(migr_clock, migr_node));
//    DEBUG << "Ending step7a\n";
    if (call_wrapper) {
        continue_migration(G, migr_node);
    }
}

void
migrate_node_step7b(db::graph *G)
{
    bool call_wrapper = false;
    uint64_t migr_node;
    G->mrequest.mutex.lock();
//    DEBUG << "Starting step7b\n";
    if (++G->mrequest.start_next_round == 2) {
        call_wrapper = true;
    } else {
//        DEBUG << "STEP 7B: Call wrapper deferred, current count = " << (int)G->mrequest.start_next_round << std::endl;
    }
    migr_node = G->mrequest.cur_node;
    G->mrequest.mutex.unlock();
//    DEBUG << "Ending step7b\n";
    if (call_wrapper) {
        continue_migration(G, migr_node);
    }
}

void
continue_migration(db::graph *G, uint64_t migr_node)
{
//    G->delete_migrated_node(migr_node);
    migration_wrapper(G);
}

timespec diff(timespec start, timespec end)
{
        timespec temp;
        if ((end.tv_nsec-start.tv_nsec)<0) {
            temp.tv_sec = end.tv_sec-start.tv_sec-1;
            temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
        } else {
            temp.tv_sec = end.tv_sec-start.tv_sec;
            temp.tv_nsec = end.tv_nsec-start.tv_nsec;
        }
        return temp;
}

void
migrated_nbr_update(db::graph *G, std::unique_ptr<message::message> msg)
{
//    DEBUG << "Starting nbr update, ";
    uint64_t local_node, remote_node, edge;
    uint64_t new_loc;
    message::unpack_message(*msg, message::MIGRATED_NBR_UPDATE, local_node, edge, remote_node, new_loc);
//    DEBUG << "for migrated node = " << remote_node << std::endl;
    G->update_migrated_nbr(local_node, edge, remote_node, new_loc);
    message::prepare_message(*msg, message::MIGRATED_NBR_ACK);
    G->send(new_loc, msg->buf);
//    DEBUG << "Ending nbr update\n";
}

// update cache based on confirmations for transient cached values
// and invalidations for stale entries
void
handle_cache_update(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::vector<uint64_t> good, bad;
    uint64_t perm_del_id, migr_del_id;
    message::unpack_message(*msg, message::CACHE_UPDATE, good, bad, perm_del_id, migr_del_id);
    
    // invalidations
    for (size_t i = 0; i < bad.size(); i++) {
        G->invalidate_prog_cache(bad[i]);
    }
    
    // confirmations
    for (size_t i = 0; i < good.size(); i++) {
        G->commit_prog_cache(good[i]);
    }
    
    G->permanent_delete(perm_del_id, migr_del_id);
    DEBUG << "Permanent delete, id = " << perm_del_id << "\tNumber of nodes = " << G->nodes.size() << std::endl;
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
    bool forwarded_req = false;
    // map from loc to send back to the handle that was deleted, the params it was given, and the handle to send back to
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, uint64_t>>> batched_deleted_nodes; 
    uint64_t cur_node;
    std::vector<uint64_t> dirty_cache_ids; // cache values used by user that we need to verify are good at coord
    std::unordered_set<uint64_t> invalid_cache_ids; // cache values from coordinator we know are invalid
    // map from location to send to next to tuple of handle and params to send to next, and node that sent them
    // these are the node programs that will be propagated onwards
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> batched_node_progs;
    db::element::remote_node this_node(G->myid, 0);
    uint64_t node_handle;

    // unpack the node program
    try {
        message::unpack_message(msg, message::NODE_PROG, prog_type_recvd, vclocks,
                unpacked_request_id, start_node_params, dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[G->myid]);
#ifdef __WEAVER_DEBUG__
        if (batched_deleted_nodes[G->myid].size() == 1 && std::get<0>(batched_deleted_nodes[G->myid].at(0)) == MAX_TIME) {
            //DEBUG << "Unpacking forwarded request in unpack_and_run\n";
            batched_deleted_nodes[G->myid].clear();
            forwarded_req = true;
        }
#endif
    } catch (std::bad_alloc& ba) {
        DEBUG << "bad_alloc caught " << ba.what() << '\n';
        while(1);
    }

    // node state and cache functions
    std::function<NodeStateType&()> node_state_getter;
    std::function<CacheValueType&()> cache_value_putter;
    std::function<std::vector<CacheValueType *>()> cached_values_getter;

    while (!start_node_params.empty() || !batched_deleted_nodes[G->myid].empty()) {
        // deleted nodes loop
        for (std::tuple<uint64_t, ParamsType, uint64_t> del_node_params: batched_deleted_nodes[G->myid]) {
            uint64_t deleted_node_handle = std::get<0>(del_node_params);
            ParamsType del_node_params_given = std::get<1>(del_node_params);
            uint64_t parent_handle = std::get<2>(del_node_params);

            db::element::node *node = G->acquire_node(parent_handle); // parent should definately exist
            assert(node != NULL);
            DEBUG << "calling delete program" << std::endl;
            node_state_getter = std::bind(get_node_state<NodeStateType>, G, prog_type_recvd, unpacked_request_id, parent_handle);

            auto next_node_params = enclosed_node_deleted_func(unpacked_request_id, *node,
                    deleted_node_handle, del_node_params_given, node_state_getter); 

            G->release_node(node);
            for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                // signal to send back to coordinator
                uint64_t next_loc = res.first.loc;
                if (next_loc == -1) {
                    // XXX get rid of pair, without pair it is not working for some reason
                    std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                    message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, unpacked_request_id,
                        temppair, dirty_cache_ids, invalid_cache_ids);
                    G->send(COORD_ID, msg.buf);
                } else if (next_loc == G->myid) {
                    start_node_params.emplace_back(res.first.handle, std::move(res.second), this_node);
                } else {
                    batched_node_progs[next_loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                }
            }
        }
        batched_deleted_nodes[G->myid].clear(); // we have run programs for this list

        for (auto &handle_params : start_node_params) {
            node_handle = std::get<0>(handle_params);
            ParamsType params = std::get<1>(handle_params);
            this_node.handle = node_handle;
            // XXX maybe use a try-lock later so forward progress can continue on other nodes in list
            db::element::node *node = G->acquire_node(node_handle);
            if (node == NULL || node->get_del_time() <= unpacked_request_id) {
                if (node != NULL) {
                    G->release_node(node);
                }
                db::element::remote_node parent = std::get<2>(handle_params);
                batched_deleted_nodes[parent.loc].emplace_back(std::make_tuple(node_handle, params, parent.handle));
                DEBUG << "Node " << node_handle << " deleted" << std::endl;
            } else if (node->state == db::element::node::mode::IN_TRANSIT || node->state == db::element::node::mode::MOVED) {
                // queueing/forwarding node programs logic goes here
                std::unique_ptr<message::message> m(new message::message());
                std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> fwd_node_params;
                std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> dummy_deleted_nodes; 
#ifdef __WEAVER_DEBUG__
                dummy_deleted_nodes.emplace_back(std::make_tuple(MAX_TIME, ParamsType(), MAX_TIME));
#endif
                fwd_node_params.emplace_back(handle_params);
                message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, vclocks,
                    unpacked_request_id, fwd_node_params, dirty_cache_ids, invalid_cache_ids, dummy_deleted_nodes);
                if (node->state == db::element::node::mode::MOVED) {
                    // node set up at new shard, fwd request immediately
                    G->release_node(node);
                    G->send(node->new_loc, m->buf);
                    //DEBUG << "MOVED: Forwarding request immed, node handle " << node_handle << std::endl;
                } else {
                    // queue request for forwarding later
                    G->release_node(node);
                    G->mrequest.mutex.lock();
                    G->mrequest.pending_requests.emplace_back(std::move(m));
                    G->mrequest.mutex.unlock();
                    //DEBUG << "IN_TRANSIT: Enqueuing request" << std::endl;
                }
            } else { // node does exist
                assert(node->state == db::element::node::mode::STABLE);
                // bind cache getter and putter function variables to functions
                node_state_getter = std::bind(get_node_state<NodeStateType>, G,
                        prog_type_recvd, unpacked_request_id, node_handle);
                cache_value_putter = std::bind(put_cache_value<CacheValueType>, G,
                        prog_type_recvd, unpacked_request_id, node_handle, node);
                cached_values_getter = std::bind(get_cached_values<CacheValueType>, G,
                        prog_type_recvd, unpacked_request_id, node_handle, &dirty_cache_ids, std::ref(invalid_cache_ids));

                // call node program
                auto next_node_params = enclosed_node_prog_func(unpacked_request_id, *node, this_node,
                        params, // actual parameters for this node program
                        node_state_getter,
                        cache_value_putter,
                        cached_values_getter);
                // batch the newly generated node programs for onward propagation
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    uint64_t loc = res.first.loc;
                    // signal to send back to coordinator
                    if (loc == -1) {
                        // XXX get rid of pair, without pair it is not working for some reason
                        std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                        message::prepare_message(msg, message::NODE_PROG, prog_type_recvd,
                            unpacked_request_id, dirty_cache_ids, temppair);
                        G->send(COORD_ID, msg.buf);
                    } else {
                        batched_node_progs[loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                        if (!MSG_BATCHING && (loc != G->myid)) {
                            message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, vclocks, unpacked_request_id,
                                batched_node_progs[loc], dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[loc]);
                            batched_node_progs[loc].clear();
                            batched_deleted_nodes[loc].clear();
                            G->send(loc, msg.buf);
                        }
                        node->msg_count[loc-1]++;
                        G->msg_count_mutex.lock();
                        G->agg_msg_count.at(node_handle)++;
                        G->request_count[G->myid-1]++;
                        G->msg_count_mutex.unlock();
                    }
                }
                G->release_node(node);
            }
            if (MSG_BATCHING) {
                for (uint64_t next_loc = 1; next_loc <= NUM_SHARDS; next_loc++) {
                    if ((   (!batched_node_progs[next_loc].empty() && batched_node_progs[next_loc].size()>BATCH_MSG_SIZE)
                         || (!batched_deleted_nodes[next_loc].empty()))
                        && next_loc != G->myid) {
                        message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, vclocks, unpacked_request_id,
                            batched_node_progs[next_loc], dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[next_loc]);
                        G->send(next_loc, msg.buf);
                        batched_node_progs[next_loc].clear();
                        batched_deleted_nodes[next_loc].clear();
                    }
                }
            }
        }
        start_node_params = std::move(batched_node_progs[G->myid]);
    }

    // propagate all remaining node progs
    for (uint64_t next_loc = 1; next_loc <= NUM_SHARDS; next_loc++) {
        if ((   (!batched_node_progs[next_loc].empty())
             || (!batched_deleted_nodes[next_loc].empty()))
            && next_loc != G->myid) {
            message::prepare_message(msg, message::NODE_PROG, prog_type_recvd, vclocks, unpacked_request_id,
                batched_node_progs[next_loc], dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[next_loc]);
            G->send(next_loc, msg.buf);
            batched_node_progs[next_loc].clear();
            batched_deleted_nodes[next_loc].clear();
        }
    }
    // check if migration needs to be initiated
    timespec t, dif;
    bool to_migrate = false;
    G->migration_mutex.lock();
    clock_gettime(CLOCK_MONOTONIC, &t);
    if (!G->migrated && G->migr_token) {
        dif = diff(G->migr_time, t);
        if (dif.tv_sec > MIGR_FREQ) {
            G->migrated = true;
            to_migrate = true;
        }
    }
    G->migration_mutex.unlock();
    if (to_migrate) {
        shard_daemon_begin(G, NULL);
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
    db::update_request *request = (db::update_request *)req;
    uint64_t req_id;
    uint64_t start_time, time2;
    uint64_t n1, n2;
    uint64_t edge;
    uint64_t loc;
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
            message::unpack_message(*request->msg, message::PERMANENT_DELETE_EDGE, n1, edge);
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

        case message::MIGRATE_NODE_STEP2:
            migrate_node_step4(G);
            break;
        
        case message::COORD_NODE_MIGRATE_ACK:
            migrate_node_step3(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP4:
            migrate_node_step5(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP6a:
            migrate_node_step7a(G);
            break;

        case message::MIGRATE_NODE_STEP6b:
            migrate_node_step7b(G);
            break;

        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(G, std::move(request->msg));
            break;

        case message::MIGRATED_NBR_ACK: {
            G->migration_mutex.lock();
            if (--G->pending_edge_updates == 0) {
                //DEBUG << "Processed last nbr update\n";
                db::element::node *n;
                message::message msg;
                n = G->acquire_node(G->migr_node);
                message::prepare_message(msg, message::MIGRATE_NODE_STEP6b);
                G->send(n->prev_loc, msg.buf);
                G->release_node(n);
            }
            G->migration_mutex.unlock();
            break;
        }

        default:
            std::cerr << "Bad msg type in unpack_update_request " << request->type << std::endl;
    }
    delete request;
}

// assuming caller holds migration_lock
void
unpack_transit_update_request(db::graph *G, db::update_request *request)
{
    uint64_t req_id, update_count;
    uint64_t n2, edge;
    uint64_t loc;
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
    if (request != NULL) {
        // TODO ugly, improve this.
        delete request;
    }
    G->migration_mutex.lock();
    //DEBUG << "Called process pending updates\n";
    while (!G->pending_updates.empty()) {
        request = G->pending_updates.top();
        if (G->current_update_count == (request->start_time-1)) {
            //DEBUG << "Going to processing update no. " << G->current_update_count << std::endl;
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
                G->pending_update_count = MAX_TIME;
                G->current_update_count = MAX_TIME;
                if (G->set_callback(G->new_shard_target_clock, migrate_node_step6)) {
                    migrate_node_step6(G);
                }
            }
        } else {
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
    uint64_t sender;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg(new message::message());
    db::thread::unstarted_thread *thr;
    db::update_request *request;
    uint64_t done_id;
    uint64_t start_time, update_count;
    // used for node programs
    node_prog::prog_type pType;
    std::vector<uint64_t> vclocks;

    while (true) {
        if ((ret = G->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
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
                rec_msg.reset(new message::message());
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
                rec_msg.reset(new message::message());
                break;

            case message::CACHE_UPDATE:
            case message::MIGRATE_NODE_STEP1:
            case message::MIGRATE_NODE_STEP2:
            case message::COORD_NODE_MIGRATE_ACK:
            case message::MIGRATE_NODE_STEP4:
            case message::MIGRATE_NODE_STEP6a:
            case message::MIGRATE_NODE_STEP6b:
            case message::MIGRATED_NBR_UPDATE:
            case message::MIGRATED_NBR_ACK:
            case message::PERMANENT_DELETE_EDGE:
            case message::PERMANENT_DELETE_EDGE_ACK:
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, unpack_update_request, G, request);
                G->thread_pool.add_request(thr);
                rec_msg.reset(new message::message());
                break;

            case message::NODE_PROG:
                vclocks.clear();
                message::unpack_message(*rec_msg, message::NODE_PROG, pType, vclocks);
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(vclocks[G->myid-1], unpack_and_run_node_program, G, request);
                G->thread_pool.add_request(thr);
                rec_msg.reset(new message::message());
                break;

            case message::MIGRATION_TOKEN:
                DEBUG << "Now obtained migration token" << std::endl;
                G->migration_mutex.lock();
                G->migr_token = true;
                G->migrated = false;
                clock_gettime(CLOCK_MONOTONIC, &G->migr_time);
                G->migration_mutex.unlock();
                break;

            //case message::REQUEST_COUNT:
            //    G->msg_count_mutex.lock(();
            //    G->msg_count_mutex.unlock(();
            //    break;

            case message::EXIT_WEAVER:
                exit(0);
                
            default:
                std::cerr << "unexpected msg type " << mtype << std::endl;
        }
    }
}

void
migration_wrapper(db::graph *G)
{
//    DEBUG << "\tMigration wrapper begin\n";
    bool no_migr = true;
    while (!G->sorted_nodes.empty()) {
        db::element::node *n;
        uint64_t max_pos, migr_pos;
        uint64_t prev_loc;
        uint64_t migr_node = G->sorted_nodes.front().first;
        n = G->acquire_node(migr_node);
        if (n == NULL || n->get_del_time() < MAX_TIME || n->dependent_del > 0 ||
            n->state == db::element::node::mode::IN_TRANSIT || n->state == db::element::node::mode::MOVED) {
            if (n != NULL) {
                G->release_node(n);
            }
            G->sorted_nodes.pop_front();
            continue;
        }
        n->updated = false;
        for (size_t j = 0; j < NUM_SHARDS; j++) {
            n->agg_msg_count[j] += (uint64_t)(0.8 * (double)(n->msg_count[j]));
        }
        max_pos = 0;
        for (uint64_t j = 0; j < n->agg_msg_count.size(); j++) {
            if (n->agg_msg_count.at(max_pos) < n->agg_msg_count.at(j)) {
                max_pos = j;
            }
        }
        migr_pos = max_pos+1;
        for (uint32_t &cnt: n->msg_count) {
            cnt = 0;
        }
        prev_loc = n->prev_loc;
        G->release_node(n);
        G->sorted_nodes.pop_front();
        if (migr_pos != G->myid && migr_pos != prev_loc) { // no migration to self or previous location
            migrate_node_step1(G, migr_node, max_pos+1);
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
//        DEBUG << "Migration wrapper end\n";
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
    G->msg_count_mutex.lock();
    //if (G->request_reply_count == 0) {
    //    message::message msg;
    //    for (uint64_t i = 1; i <= NUM_SHARDS; i++) {
    //        message::prepare_message(msg, message::REQUEST_COUNT);
    //        G->send(i, msg.buf);
    //    }
    //    G->msg_count_mutex.unlock();
    //    return;
    //} else if (G->request_reply_count < NUM_SHARDS) {
    //    // still awaiting some counter updates
    //    G->msg_count_mutex.unlock();
    //    return;
    //} else {
        DEBUG << "Starting shard daemon" << std::endl;
        std::deque<std::pair<uint64_t, uint32_t>> &sn = G->sorted_nodes;
        sn.clear();
        for (auto &p: G->agg_msg_count) {
            sn.emplace_back(p);
        }
        G->msg_count_mutex.unlock();
        std::sort(sn.begin(), sn.end(), agg_count_compare);
        size_t num_nodes = sn.size();
        sn.erase(sn.begin() + num_nodes/2, sn.end());
        DEBUG << "Migr nodes size = " << sn.size() << std::endl;
        migration_wrapper(G);
    //}
}

void
shard_daemon_end(db::graph *G)
{
    message::message msg;
    message::prepare_message(msg, message::MIGRATION_TOKEN);
    G->migration_mutex.lock();
    G->migr_token = false;
    G->migration_mutex.unlock();
    uint64_t next_id = (G->myid + 1) > NUM_SHARDS ? 1 : (G->myid + 1);
    G->send(next_id, msg.buf);
}

void
first_shard_daemon_begin(db::graph *G)
{
    DEBUG << "Going to sleep in sd begin\n";
    std::chrono::seconds duration(30); // initial delay for graph to set up
    std::this_thread::sleep_for(duration);
    void *dummy = NULL;
    DEBUG << "Starting sd begin\n";
    shard_daemon_begin(G, dummy);
}

void
end_program(int param)
{
    std::cerr << "Caught SIGINT, ending program, param = " << param << std::endl;
    exit(1);
}
int
main(int argc, char* argv[])
{
    std::thread *t;
    signal(SIGINT, end_program);
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <myid>" << std::endl;
        return -1;
    }
    uint64_t id = atoi(argv[1]);
    db::graph G(id);
    std::cout << "Weaver: shard instance " << G.myid << std::endl;

    void *dummy = NULL;
    if (G.myid == 1 && MIGRATION) {
        t = new std::thread(&first_shard_daemon_begin, &G);
        t->detach();
    }
    runner(&G);

    return 0;
}

#endif
