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

//#ifndef PO6_NDEBUG_LEAKS
//#define PO6_NDEBUG_LEAKS

#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <signal.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#define __WEAVER_DEBUG__
#include "common/weaver_constants.h"
#include "common/message_graph_elem.h"
#include "graph.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
#include "node_prog/dijkstra_program.h"
#include "node_prog/reach_program.h"
#include "node_prog/clustering_program.h"

// global static variables
static uint64_t myid;

// migration methods
void migrate_node_step1(db::graph *G, uint64_t node_handle, uint64_t new_shard);
void migrate_node_step2(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step3(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step4(db::graph *G);
void migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg);
void migrate_node_step6(db::graph *G);
void migrate_node_step7a(db::graph *G);
void migrate_node_step7b(db::graph *G);
void migrate_node_step7c(db::graph *G, std::unique_ptr<message::message> msg);
void process_pending_updates(db::graph *G, void *req);
void migration_wrapper(db::graph *G);
void continue_migration(db::graph *G);
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
        for (uint64_t id: *success.second) {
            cache->emplace_back(id);
        }
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
        for (uint64_t id: *success.second) {
            cache->emplace_back(id);
        }
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
        for (uint64_t id: *success.second) {
            cache->emplace_back(id);
        }
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
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->acquire_node(node_handle);
    if (n->updated) {
        G->release_node(n);
        G->mrequest.mutex.unlock();
        DEBUG << "canceling migration" << std::endl;
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
        //std::vector<std::unique_ptr<message::message>>().swap(G->mrequest.pending_requests);
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
    }
}

// Receive and place a node which has been migrated to this shard
void
migrate_node_step2(db::graph *G, std::unique_ptr<message::message> msg)
{
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
        return;
    }
    n->prev_loc = from_loc; // record shard from which we just migrated this node
    n->prev_locs.at(G->myid-1) = 1; // mark this shard as one of the previous locations
    G->release_node(n);
    message::prepare_message(*msg, message::MIGRATE_NODE_STEP2, 0);
    G->send(from_loc, msg->buf);
}

// receive last update clock value from coord
void
migrate_node_step3(db::graph *G, std::unique_ptr<message::message> msg)
{
    uint64_t my_clock, other_clock, global_clock;
    message::unpack_message(*msg, message::COORD_NODE_MIGRATE_ACK, my_clock, other_clock, global_clock);
    G->mrequest.mutex.lock();
    G->mrequest.migr_clock = my_clock;
    G->mrequest.other_clock = other_clock;
    //if (G->mrequest.prev_node != 0) {
    //    G->migrated_nodes.emplace_back(std::unique_ptr<std::pair<uint64_t, uint64_t>>(
    //        new std::pair<uint64_t, uint64_t>(global_clock, G->mrequest.prev_node)));
    //}
    G->mrequest.mutex.unlock();
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
        G->mrequest.mutex.unlock();
        return;
    } else {
        n = G->acquire_node(G->mrequest.cur_node);
        message::prepare_message(msg, message::MIGRATE_NODE_STEP4, n->update_count, G->mrequest.other_clock);
        G->send(G->mrequest.new_loc, msg.buf);
        G->release_node(n);
        G->mrequest.mutex.unlock();
    }
}

// receive the number of pending updates for the newly migrated node
void
migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg)
{
    uint64_t update_count, cur_update_count, target_clock;
    G->migration_mutex.lock();
    db::element::node *n = G->acquire_node(G->migr_node);
    G->migration_mutex.unlock();
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
        cur_update_count = n->update_count;
        G->release_node(n);
        G->set_update_count(update_count, cur_update_count, target_clock);
        process_pending_updates(G, NULL);
    }
}

void
initiate_nbr_update(db::graph *G, std::pair<const uint64_t, db::element::edge*> &nbr,
    uint64_t migr_node, std::vector<std::pair<uint64_t, uint64_t>> &local_nbr_updates,
    uint64_t &pending_edge_updates, message::message &msg)
{
    if (nbr.second->nbr.loc == G->myid) {
        local_nbr_updates.emplace_back(std::make_pair(nbr.second->nbr.handle, nbr.first));
    } else {
        // remote node, need to send msg
        message::prepare_message(msg, message::MIGRATED_NBR_UPDATE,
            nbr.second->nbr.handle, nbr.first, migr_node, G->myid);
        G->send(nbr.second->nbr.loc, msg.buf);
        pending_edge_updates++;
    }
}

// inform other shards of migration
// Caution: assuming we have acquired node lock
void
migrate_node_step6(db::graph *G)
{
    uint64_t migr_node;
    uint64_t pending_edge_updates = 0;
    uint64_t prev_loc;
    std::vector<std::pair<uint64_t, uint64_t>> local_nbr_updates;
    G->increment_clock(); // let subsequent updates come through now
    G->migration_mutex.lock();
    migr_node = G->migr_node;
    assert(G->pending_edge_updates == 0);
    G->migration_mutex.unlock();
    db::element::node *n = G->acquire_node(migr_node);
    // signal previous loc to forward pending node programs
    message::message msg;
    message::prepare_message(msg, message::MIGRATE_NODE_STEP6a);
    G->send(n->prev_loc, msg.buf);
    // update in-neighbors
    int edge_cnt = 0;
    for (auto &nbr: n->in_edges) {
        initiate_nbr_update(G, nbr, migr_node, local_nbr_updates, pending_edge_updates, msg);
        edge_cnt++;
    }
    for (auto &nbr: n->out_edges) {
        initiate_nbr_update(G, nbr, migr_node, local_nbr_updates, pending_edge_updates, msg);
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
        message::prepare_message(msg, message::MIGRATE_NODE_STEP6b);
        G->send(prev_loc, msg.buf);
    }
    G->migration_mutex.unlock();
}

// forward queued traversal requests to new location
void
migrate_node_step7a(db::graph *G)
{
    message::message msg;
    std::vector<uint64_t> nodes;
    db::element::node *n;
    uint64_t migr_node;
    bool call_wrapper = false;
    G->mrequest.mutex.lock();
    migr_node = G->mrequest.cur_node;
    n = G->acquire_node(migr_node);
    n->state = db::element::node::mode::MOVED;
    for (auto &r: n->pending_requests) {
        //DEBUG << "Now forwarding queued request for migr node " << migr_node
        //    << " to loc " << G->mrequest.new_loc << std::endl;
        G->send(G->mrequest.new_loc, r->buf);
    }
    n->pending_requests.clear();
    G->release_node(n);
    if (++G->mrequest.start_next_round == 3) {
        call_wrapper = true;
    }
    G->mrequest.mutex.unlock();
    if (call_wrapper) {
        continue_migration(G);
    }
}

void
migrate_node_step7b(db::graph *G)
{
    bool call_wrapper = false;
    message::message msg;
    message::prepare_message(msg, message::COORD_CLOCK_REQ, G->myid);
    G->send(COORD_ID, msg.buf);
    G->mrequest.mutex.lock();
    if (++G->mrequest.start_next_round == 3) {
        call_wrapper = true;
    }
    G->mrequest.mutex.unlock();
    if (call_wrapper) {
        continue_migration(G);
    }
}

void
migrate_node_step7c(db::graph *G, std::unique_ptr<message::message> msg)
{
    bool call_wrapper = false;
    uint64_t global_clock;
    message::unpack_message(*msg, message::COORD_CLOCK_REPLY, global_clock);
    G->mrequest.mutex.lock();
    G->migrated_nodes.emplace_back(std::unique_ptr<std::pair<uint64_t, uint64_t>>(
        new std::pair<uint64_t, uint64_t>(global_clock, G->mrequest.cur_node)));
    if (++G->mrequest.start_next_round == 3) {
        call_wrapper = true;
    }
    G->mrequest.mutex.unlock();
    if (call_wrapper) {
        continue_migration(G);
    }
}

void
continue_migration(db::graph *G)
{
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
    uint64_t local_node, remote_node, edge;
    uint64_t new_loc;
    message::unpack_message(*msg, message::MIGRATED_NBR_UPDATE, local_node, edge, remote_node, new_loc);
    G->update_migrated_nbr(local_node, edge, remote_node, new_loc);
    message::prepare_message(*msg, message::MIGRATED_NBR_ACK);
    G->send(new_loc, msg->buf);
}

// 1. update cache based on confirmations for transient cached values
//    and invalidations for stale entries
// 2. clean up old state for completed node programs
// 3. permanent deletion of migrated and deleted nodes
void
handle_clean_up(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::unordered_set<uint64_t> good, bad;
    uint64_t perm_del_id, migr_del_id;
    std::vector<std::pair<uint64_t, node_prog::prog_type>> completed_requests;
    message::unpack_message(*msg, message::CLEAN_UP, good, bad,
        perm_del_id, migr_del_id, completed_requests);
    
    // invalidations
    for (uint64_t bad_id: bad) {
        try {
            G->invalidate_prog_cache(bad_id);
        } catch (std::exception &e) {
            DEBUG << "caught exception here, shard = " << G->myid << ", exception " << e.what() << std::endl;
            return;
        }
    }
    
    // confirmations
    for (uint64_t good_id: good) {
        try {
            G->commit_prog_cache(good_id);
        } catch (std::exception &e) {
            DEBUG << "caught exception here, shard = " << G->myid << ", exception " << e.what() << std::endl;
            return;
        }
    }
    //G->print_cache_size();
    
    // remove state corresponding to completed node programs
    try {
        G->add_done_request(completed_requests);
    } catch (std::exception &e) {
        DEBUG << "caught exception here, shard = " << G->myid << ", exception " << e.what() << std::endl;
        return;
    }

    // permanent deletion of deleted and migrated nodes
    try {
        G->permanent_delete(perm_del_id, migr_del_id);
    } catch (std::exception &e) {
        DEBUG << "caught exception here, shard = " << G->myid << ", exception " << e.what() << std::endl;
        return;
    }
    //DEBUG << "Permanent delete, id = " << perm_del_id
    //    << "\tMigr del id = " << migr_del_id 
    //    << "\tNumber of nodes = " << G->nodes.size()
    //    << "\tGood size = " << good.size() << ", bad size = " << bad.size() << std::endl;

    // check if migration needs to be initiated
    timespec t, dif;
    bool to_migrate = false;
    G->migr_token_mutex.lock();
    clock_gettime(CLOCK_MONOTONIC, &t);
    if (!G->migrated && G->migr_token) {
        dif = diff(G->migr_time, t);
        if (dif.tv_sec > MIGR_FREQ) {
            G->migrated = true;
            to_migrate = true;
            DEBUG << "Going to start migration at shard " << G->myid << " at time " << t.tv_sec << std::endl;
        }
    }
    G->migr_token_mutex.unlock();
    if (to_migrate) {
        shard_daemon_begin(G, NULL);
    }
}

template <typename NodeStateType>
NodeStateType& get_node_state(db::graph *G, node_prog::prog_type pType, uint64_t req_id, uint64_t node_handle)
{
    NodeStateType *toRet = new NodeStateType();
    if (G->prog_req_state_exists(pType, req_id, node_handle)) {
        toRet = dynamic_cast<NodeStateType *>(G->fetch_prog_req_state(pType, req_id, node_handle));
        if (toRet == NULL) {
            DEBUG << "NodeStateType needs to extend Deletable" << std::endl;
        }
    } else {
        toRet = new NodeStateType();
        G->insert_prog_req_state(pType, req_id, node_handle, toRet);
    }
    return *toRet;
}

template <typename CacheValueType>
std::vector<std::shared_ptr<CacheValueType>> get_cached_values(db::graph *G, node_prog::prog_type pType, uint64_t req_id,
    uint64_t node_handle, std::vector<uint64_t> *dirty_list_ptr, std::unordered_set<uint64_t> &ignore_set)
{
    std::vector<std::shared_ptr<CacheValueType>> toRet;
    std::shared_ptr<CacheValueType> cache;
    try {
        for (std::shared_ptr<node_prog::CacheValueBase> cval : G->fetch_prog_cache(pType, node_handle, req_id, dirty_list_ptr, ignore_set)) {
            cache = std::dynamic_pointer_cast<CacheValueType>(cval);
            if (!cache) {
                DEBUG << "CacheValueType needs to extend CacheValueBase" << std::endl;
            } else {
                toRet.emplace_back(cache);
            }
        }
    } catch (const std::out_of_range &e) {
        DEBUG << "caught error get cache " << e.what() << std::endl;
        return std::move(toRet);
    }
    return std::move(toRet);
}

template <typename CacheValueType>
CacheValueType& put_cache_value(db::graph *G, node_prog::prog_type pType, uint64_t req_id, uint64_t node_handle,
    db::element::node *n, std::vector<uint64_t> *dirty_list_ptr)
{
    std::shared_ptr<CacheValueType> toRet;
    if (G->prog_cache_exists(pType, node_handle, req_id)) {
        // XXX They wrote cache twice! delete and replace probably
        toRet = std::dynamic_pointer_cast<CacheValueType>(G->fetch_prog_cache_single(pType, node_handle, req_id));
        if (toRet == NULL) {
            // dynamic_cast failed, CacheValueType needs to extend CacheValueBase
            DEBUG << "CacheValueType needs to extend CacheValueBase" << std::endl;
        }
    } else {
        toRet = std::make_shared<CacheValueType>();
        G->insert_prog_cache(pType, req_id, node_handle, toRet, n);
    }
    bool dirty = false;
    for (uint64_t id: *dirty_list_ptr) {
        if (id == req_id) {
            dirty = true;
            break;
        }
    }
    if (!dirty) {
        dirty_list_ptr->emplace_back(req_id);
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
    std::ofstream reqfile;
    timespec cur;
    clock_gettime(CLOCK_MONOTONIC, &cur);
    reqfile.open(std::string("graph_req") + std::to_string(G->myid) + ".rec", std::ios::app);
    G->test_mutex.lock();
    reqfile << cur.tv_sec << "." << cur.tv_nsec << std::endl;
    G->test_mutex.unlock();
    reqfile.close();
    // unpack some start params from msg:
    std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> start_node_params;
    uint64_t unpacked_request_id;
    std::vector<uint64_t> vclocks; //needed to pass to next message
    prog_type prog_type_recvd;
    bool done_request = false;
    // map from loc to send back to the handle that was deleted, the params it was given, and the handle to send back to
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, uint64_t>>> batched_deleted_nodes; 
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
            //DEBUG << "Unpacking forwarded request in unpack_and_run for node "
            //    << std::get<0>(start_node_params.at(0)) << std::endl;
            batched_deleted_nodes[G->myid].clear();
        }
#endif
    } catch (std::bad_alloc& ba) {
        DEBUG << "bad_alloc caught " << ba.what() << '\n';
        return;
    }

    // node state and cache functions
    std::function<NodeStateType&()> node_state_getter;
    std::function<CacheValueType&()> cache_value_putter;
    std::function<std::vector<std::shared_ptr<CacheValueType>>()> cached_values_getter;

    // check if request completed
    if (G->check_done_request(unpacked_request_id)) {
        done_request = true;
    }
    while ((!start_node_params.empty() || !batched_deleted_nodes[G->myid].empty()) && !done_request) {
        // deleted nodes loop
        for (std::tuple<uint64_t, ParamsType, uint64_t> del_node_params: batched_deleted_nodes[G->myid]) {
            uint64_t deleted_node_handle = std::get<0>(del_node_params);
            ParamsType del_node_params_given = std::get<1>(del_node_params);
            uint64_t parent_handle = std::get<2>(del_node_params);

            db::element::node *node = G->acquire_node(parent_handle);
            // parent should definitely not be deleted
            assert(node != NULL && node->get_del_time() > unpacked_request_id);
            if (node->state == db::element::node::mode::IN_TRANSIT
             || node->state == db::element::node::mode::MOVED) {
                // queueing/forwarding delete program
                std::unique_ptr<message::message> m(new message::message());
                std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> dummy_node_params;
                std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> fwd_deleted_nodes; 
                fwd_deleted_nodes.emplace_back(del_node_params);
                message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, vclocks,
                    unpacked_request_id, dummy_node_params, dirty_cache_ids, invalid_cache_ids, fwd_deleted_nodes);
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
                node_state_getter = std::bind(get_node_state<NodeStateType>, G, prog_type_recvd,
                        unpacked_request_id, parent_handle);

                auto next_node_params = enclosed_node_deleted_func(unpacked_request_id, *node,
                        deleted_node_handle, del_node_params_given, node_state_getter); 

                G->release_node(node);
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    uint64_t next_loc = res.first.loc;
                    if (next_loc == COORD_ID) {
                        // signal to send back to coordinator
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
                DEBUG << "Node " << node_handle << " deleted, cur request num " << unpacked_request_id << std::endl;
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
                message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, vclocks,
                    unpacked_request_id, fwd_node_params, dirty_cache_ids, invalid_cache_ids, dummy_deleted_nodes);
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
            } else { // node does exist
                assert(node->state == db::element::node::mode::STABLE);
                // bind cache getter and putter function variables to functions
                node_state_getter = std::bind(get_node_state<NodeStateType>, G,
                        prog_type_recvd, unpacked_request_id, node_handle);
                cache_value_putter = std::bind(put_cache_value<CacheValueType>, G,
                        prog_type_recvd, unpacked_request_id, node_handle, node, &dirty_cache_ids);
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
                    if (loc == COORD_ID) {
                        // signal to send back to coordinator
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
                        G->msg_count_mutex.lock();
                        G->agg_msg_count[node_handle]++;
                        G->request_count[loc-1]++; // increment count of msges sent to loc
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

    if (!done_request) { // mark request state as not in use
        G->clear_req_use(unpacked_request_id);
    }

    // propagate all remaining node progs
    for (uint64_t next_loc = 1; next_loc <= NUM_SHARDS && !done_request; next_loc++) {
        if (((!batched_node_progs[next_loc].empty())
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


template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_start_coord(coordinator::central*, std::shared_ptr<coordinator::pending_req>)
{ }

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
    uint64_t request_count, node_count, sender;

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

        case message::CLEAN_UP:
            handle_clean_up(G, std::move(request->msg));
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

        case message::COORD_CLOCK_REPLY:
            migrate_node_step7c(G, std::move(request->msg));
            break;

        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(G, std::move(request->msg));
            break;

        case message::MIGRATED_NBR_ACK: {
            G->migration_mutex.lock();
            if (--G->pending_edge_updates == 0) {
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

        case message::REQUEST_COUNT: {
            message::unpack_message(*request->msg, message::REQUEST_COUNT, sender);
            message::message msg;
            uint64_t my_node_count = G->get_node_count();
            G->msg_count_mutex.lock();
            message::prepare_message(msg, message::REQUEST_COUNT_ACK, G->request_count[sender-1], my_node_count, G->myid);
            for (auto &c: G->request_count) {
                c = 0;
            }
            G->msg_count_mutex.unlock();
            G->send(sender, msg.buf);
            break;
        }

        case message::REQUEST_COUNT_ACK: {
            message::unpack_message(*request->msg, message::REQUEST_COUNT_ACK, request_count, node_count, sender);
            G->msg_count_mutex.lock();
            G->request_count[sender-1] = request_count;
            G->node_count[sender-1] = node_count;
            uint64_t reply_cnt = ++G->request_reply_count;
            G->msg_count_mutex.unlock();
            if (reply_cnt == (NUM_SHARDS-1)) {
                shard_daemon_begin(G, NULL);
            }
            break;
        }

        default:
            DEBUG << "Bad msg type in unpack_update_request " << request->type << std::endl;
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
            DEBUG << "Bad msg type in unpack_transit_update_request" << std::endl;
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
    uint64_t start_time, update_count;
    // used for node programs
    node_prog::prog_type pType;
    std::vector<uint64_t> vclocks;

    while (true) {
        if ((ret = G->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "msg recv error: " << ret << " at shard " << G->myid << std::endl;
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
                G->pending_updates.emplace(request);
                G->migration_mutex.unlock();
                request = new db::update_request(mtype, 0);
                thr = new db::thread::unstarted_thread(0, process_pending_updates, G, request);
                G->thread_pool.add_request(thr);
                rec_msg.reset(new message::message());
                break;

            case message::CLEAN_UP:
            case message::MIGRATE_NODE_STEP1:
            case message::MIGRATE_NODE_STEP2:
            case message::COORD_NODE_MIGRATE_ACK:
            case message::MIGRATE_NODE_STEP4:
            case message::MIGRATE_NODE_STEP6a:
            case message::MIGRATE_NODE_STEP6b:
            case message::COORD_CLOCK_REPLY:
            case message::MIGRATED_NBR_UPDATE:
            case message::MIGRATED_NBR_ACK:
            case message::PERMANENT_DELETE_EDGE:
            case message::PERMANENT_DELETE_EDGE_ACK:
            case message::REQUEST_COUNT:
            case message::REQUEST_COUNT_ACK:
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
                DEBUG << "Now obtained migration token at shard " << G->myid << std::endl;
                G->migr_token_mutex.lock();
                G->migr_token = true;
                G->migrated = false;
                clock_gettime(CLOCK_MONOTONIC, &G->migr_time);
                G->migr_token_mutex.unlock();
                break;

            case message::EXIT_WEAVER:
                exit(0);
                
            default:
                DEBUG << "unexpected msg type " << mtype << std::endl;
        }
    }
}

void
migration_wrapper(db::graph *G)
{
    bool no_migr = true;
    while (!G->sorted_nodes.empty()) {
        db::element::node *n;
        uint64_t max_pos, migr_pos;
        uint64_t migr_node = G->sorted_nodes.front().first;
        uint32_t msg_count = G->sorted_nodes.front().second;
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
        db::element::edge *e;
        for (auto &e_iter: n->out_edges) {
            e = e_iter.second;
            n->msg_count.at(e->nbr.loc-1) += e->msg_count;
            e->msg_count = 0;
        }
        for (auto &e_iter: n->in_edges) {
            e = e_iter.second;
            n->msg_count.at(e->nbr.loc-1) += e->msg_count;
            e->msg_count = 0;
        }
        for (size_t j = 0; j < NUM_SHARDS; j++) {
            n->agg_msg_count.at(j) += (uint64_t)(0.8 * (double)(n->msg_count.at(j)));
        }
        max_pos = 0;
        for (uint64_t j = 0; j < n->agg_msg_count.size(); j++) {
            if (n->agg_msg_count.at(max_pos) < n->agg_msg_count.at(j)) {
                max_pos = j;
            }
        }
        migr_pos = max_pos;
        //if ((G->node_count.at(G->myid-1) >= MAX_NODES) && (migr_pos == (G->myid-1))) {
        //    // if this shard is full to capacity, try to offload to a less populated shard
        //    double max_count = n->agg_msg_count.at(migr_pos);
        //    for (uint64_t j = 0; j < NUM_SHARDS; j++) {
        //        if (n->agg_msg_count.at(j) > (0.8 * max_count) // should be within acceptable limit
        //         && j != migr_pos
        //         && G->node_count.at(j) < MAX_NODES) { // capacity != full
        //            migr_pos = j;
        //            break;
        //        }
        //    }
        //}
        migr_pos++; // fixing off-by-one
        for (uint32_t &cnt: n->msg_count) {
            cnt = 0;
        }
        G->release_node(n);
        G->sorted_nodes.pop_front();
        double reverse_force = 0;//((double)G->request_count_const.at(migr_pos-1))/G->node_count.at(migr_pos-1);
        //DEBUG << "reverse force " << reverse_force << ", forward force " << msg_count << std::endl;
        if (migr_pos != G->myid // no migration to self
         &&(reverse_force < msg_count))
         //&&(G->node_count.at(migr_pos-1) < MAX_NODES)) // good from load balancing point of view
        {
            DEBUG << "migrating node " << migr_node << " to " << migr_pos << std::endl;
            migrate_node_step1(G, migr_node, migr_pos);
            G->node_count.at(migr_pos-1)++;
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
        shard_daemon_end(G);
    }
}

bool agg_count_compare(std::pair<uint64_t, uint32_t> p1, std::pair<uint64_t, uint32_t> p2)
{
    return (p1.second > p2.second);
}

// sort nodes in order of number of requests propagated
// and (implicitly) pass sorted deque to migration wrapper
// also, first update global request statistics from each shard
void
shard_daemon_begin(db::graph *G, void*)
{
    if (G->request_reply_count == 0) {
        G->node_count[G->myid] = G->get_node_count();
        message::message msg;
        for (uint64_t i = 1; i <= NUM_SHARDS; i++) {
            if (i == G->myid) {
                continue;
            }
            message::prepare_message(msg, message::REQUEST_COUNT, G->myid);
            G->send(i, msg.buf);
        }
        return;
    } else {
        DEBUG << "Starting shard daemon" << std::endl;
        G->msg_count_mutex.lock();
        assert(G->request_reply_count == (NUM_SHARDS-1));
        G->request_reply_count = 0;
        G->request_count_const = G->request_count;
        auto agg_msg_count = std::move(G->agg_msg_count);
        assert(G->agg_msg_count.empty());
        G->msg_count_mutex.unlock();
        std::deque<std::pair<uint64_t, uint32_t>> sn;
        for (auto &p: agg_msg_count) {
            sn.emplace_back(p);
        }
        std::sort(sn.begin(), sn.end(), agg_count_compare);
        G->sorted_nodes = std::move(sn);
        DEBUG << "sorted nodes size " << sn.size() << std::endl;
        migration_wrapper(G);
    }
}

void
shard_daemon_end(db::graph *G)
{
    message::message msg;
    message::prepare_message(msg, message::MIGRATION_TOKEN);
    G->migr_token_mutex.lock();
    G->migr_token = false;
    G->migr_token_mutex.unlock();
    uint64_t next_id = (G->myid + 1) > NUM_SHARDS ? 1 : (G->myid + 1);
    G->send(next_id, msg.buf);
}

void
end_program(int param)
{
    DEBUG << "Caught SIGINT, ending program, param = " << param << std::endl;
    exit(1);
}

void
myterminate()
{
    DEBUG << "In my terminate, for shard " << myid << std::endl;
    while(1);
}

int
main(int argc, char* argv[])
{
    signal(SIGINT, end_program);
    //std::set_terminate(&myterminate);
    if (argc != 2) {
        DEBUG << "Usage: " << argv[0] << " <myid>" << std::endl;
        return -1;
    }
    uint64_t id = atoi(argv[1]);
    myid = id;
    db::graph G(id);
    std::cout << "Weaver: shard instance " << G.myid << std::endl;

    // initialize migration at shard 1
    if (G.myid == START_MIGR_ID && MIGRATION) {
        G.migr_token_mutex.lock();
        G.migr_token = true;
        G.migrated = false;
        clock_gettime(CLOCK_MONOTONIC, &G.migr_time);
        G.migr_time.tv_sec += INITIAL_MIGR_DELAY;
        G.migr_token_mutex.unlock();
    }
    runner(&G);

    return 0;
}

//#endif
