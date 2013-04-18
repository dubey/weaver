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
#include "request_objects.h"
#include "graph.h"
#include "node_program.h"
#include "dijkstra_program.h"

void handle_reachable_request(db::graph *G, void *request);
void handle_dijkstra_prop(db::graph *G, void *request);
void migrate_node_step1(db::graph *G, uint64_t node, int new_loc);
void migrate_node_step4_1(db::graph *G);

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
        G->propagate_request(nodes, r, G->mrequest.new_loc);
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

            /*case message::NODE_PROG:
            message::unpack_messag
            */
/*
        case message::REACHABLE_PROP:
            unpack_traversal_request(G, std::move(request->msg));
            break;

        case message::REACHABLE_REPLY:
            handle_reachable_reply(G, std::move(request->msg));
            break;

        case message::CACHE_UPDATE:
            handle_cache_update(G, std::move(request->msg));
            break;
            */

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

            case message::REACHABLE_REPLY:
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

/*
            case message::NODE_PROG:
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(message::msg_type) >> code;
                db::prog_type ptype = (db::prog_type) code;
                node_program * toRun = programs.at(pType);
                toRun.unpack_and_run(G, msg);

                thr = new db::thread::unstarted_thread(0, unpack_update_request, G, request);
                G->thread_pool.add_request(thr);
                break;
                */
/*

            case message::REACHABLE_PROP:
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, unpack_update_request, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::REACHABLE_DONE:
                message::unpack_message(*rec_msg, message::REACHABLE_DONE, done_id);
                G->add_done_request(done_id);
                break;

            case message::DIJKSTRA_REQ:
                unpack_dijkstra_request(G, std::move(rec_msg));
                break;

            case message::DIJKSTRA_PROP:
                unpack_dijkstra_prop(G, std::move(rec_msg));
                break;

            case message::DIJKSTRA_PROP_REPLY:
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, handle_dijkstra_prop_reply, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::CLUSTERING_REQ:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_request, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::CLUSTERING_PROP:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_prop, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::CLUSTERING_PROP_REPLY:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_prop_reply, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::NODE_REFRESH_REQ:
                thr.reset(new
                db::thread::unstarted_thread(handle_refresh_request, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::NODE_REFRESH_REPLY:
                thr.reset(new
                db::thread::unstarted_thread(handle_refresh_response, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;
*/
           default:
                std::cerr << "unexpected msg type " << (message::CLIENT_REPLY ==
                code) << std::endl;
        }
    }
}

// delete old visited properties
// TODO delete old done_requests
void
shard_daemon(db::graph *G)
{
    std::unordered_map<uint64_t, std::vector<uint64_t>> *next_map;
    while (true) {
        std::chrono::seconds duration(40); // execution frequency in seconds
        std::this_thread::sleep_for(duration);
        // deleting visited props
        G->visited_mutex.lock();
        if (G->visit_map) {
            next_map = &G->visit_map_odd;
            G->visit_map = false;
        } else {
            next_map = &G->visit_map_even;
            G->visit_map = true;
        }
        for (auto it = next_map->begin(); it != next_map->end(); it++) {
            uint64_t req_id = it->first;
            for (auto vec_it: it->second) {
                db::element::node *n = G->acquire_node(vec_it);
                G->remove_visited(n, req_id);
                G->release_node(n);
            }
        }
        next_map->clear();
        G->visited_mutex.unlock();
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
