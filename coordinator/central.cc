/*
 * ===============================================================
 *    Description:  Coordinator server loop
 *
 *        Created:  11/06/2012 11:47:04 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <unistd.h>
#include <cstdlib>
#include <iostream>
#include <time.h>
#include <chrono>
#include <thread>
#include "e/buffer.h"
#include "busybee_constants.h"

#include "central.h"
#include "common/meta_element.h"
#include "common/message.h"
#include "common/debug.h"

void coord_daemon_end(coordinator::central *server);

// caution: assuming caller holds server->mutex
bool
check_elem(coordinator::central *server, uint64_t handle, bool node_or_edge)
{
    common::meta_element *elem;
    if (node_or_edge) {
        // check for node
        if (server->nodes.find(handle) != server->nodes.end()) {
            return false;
        }
        elem = server->nodes.at(handle);
    } else {
        // check for edge
        if (server->edges.find(handle) != server->edges.end()) {
            return false;
        }
        elem = server->edges.at(handle);
    }
    if (elem->get_del_time() < MAX_TIME) {
        return false;
    } else {
        return true;
    }
}

// create a node
void
create_node(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    message::message msg;
    int loc;
    uint64_t clock;
    uint64_t req_id;
    
    server->update_mutex.lock();
    server->port_ctr = (server->port_ctr + 1) % NUM_SHARDS;
    loc = server->port_ctr; // node will be placed on this shard server
    clock = ++server->vc.clocks->at(loc);
    req_id = ++server->request_id;
    server->update_mutex.unlock();
    
    message::prepare_message(msg, message::NODE_CREATE_REQ, clock, req_id);
    server->send(loc, msg.buf);
    server->add_node(new common::meta_element(loc), req_id);
    message::prepare_message(msg, message::CLIENT_REPLY, req_id);
    server->send(std::move(request->client), msg.buf);
}

// create an edge
void
create_edge(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    message::message msg;
    uint64_t req_id;
    uint64_t clock1, clock2;
    common::meta_element *me1, *me2;
    int loc1, loc2;

    server->update_mutex.lock();
    // checks for given node_handles
    if (check_elem(server, request->elem1, true) || check_elem(server, request->elem2, true)) {
        std::cerr << "node(s) not found or deleted, cannot create edge" << std::endl;
        message::prepare_message(msg, message::CLIENT_REPLY, 0);
        server->send(std::move(request->client), msg.buf);
        server->update_mutex.unlock();
        return;
    }
    // good to go
    me1 = server->nodes.at(request->elem1);
    me2 = server->nodes.at(request->elem2);
    clock1 = ++server->vc.clocks->at(me1->get_loc());
    clock2 = ++server->vc.clocks->at(me2->get_loc());
    req_id = ++server->request_id;
    server->update_mutex.unlock();

    loc1 = me1->get_loc();
    loc2 = me2->get_loc();
    message::prepare_message(msg, message::EDGE_CREATE_REQ, clock1, req_id, request->elem1, request->elem2, loc2, clock2);
    server->send(loc1, msg.buf);
    server->add_edge(new common::meta_element(loc1), req_id);
    message::prepare_message(msg, message::CLIENT_REPLY, req_id);
    server->send(std::move(request->client), msg.buf);
}

// delete a node
void
delete_node_initiate(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    message::message msg;
    common::meta_element *me;

    server->update_mutex.lock();
    if (check_elem(server, request->elem1, true)) {
        std::cerr << "node(s) not found or deleted, cannot create edge" << std::endl;
        server->update_mutex.unlock();
    } else {
        me = server->nodes.at(request->elem1);
        request->clock1 = ++server->vc.clocks->at(me->get_loc());
        me->update_del_time(request->clock1);
        request->req_id = ++server->request_id;
        server->pending[request->req_id] = request;
        message::prepare_message(msg, message::NODE_DELETE_REQ, request->clock1, request->req_id, request->elem1);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(std::move(request->client), msg.buf);
}

// delete an edge
void
delete_edge_initiate(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    message::message msg;
    common::meta_element *me1, *me2;

    server->update_mutex.lock();
    if (check_elem(server, request->elem1, true) || check_elem(server, request->elem2, false)) {
        std::cerr << "node(s) not found or deleted, cannot create edge" << std::endl;
        server->update_mutex.unlock();
    } else {
        me1 = server->nodes.at(request->elem1);
        me2 = server->edges.at(request->elem2);
        request->clock1 = ++server->vc.clocks->at(me1->get_loc());
        me2->update_del_time(request->clock1);
        request->req_id = ++server->request_id;
        server->pending[request->req_id] = request;
        message::prepare_message(msg, message::EDGE_DELETE_REQ, request->clock1, request->req_id, request->elem1, request->elem2);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(std::move(request->client), msg.buf);
}

// add a property, i.e. a key-value pair to an edge
void 
add_edge_property(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    message::message msg;
    common::meta_element *me1;

    server->update_mutex.lock();
    if (check_elem(server, request->elem1, true) || check_elem(server, request->elem2, false)) {
        std::cerr << "node(s) not found or deleted, cannot create edge" << std::endl;
        server->update_mutex.unlock();
    } else {
        me1 = server->nodes.at(request->elem1);
        request->clock1 = ++server->vc.clocks->at(me1->get_loc());
        request->req_id = ++server->request_id;
        server->update_mutex.unlock();
        common::property prop(request->key, request->value, request->req_id);
        message::prepare_message(msg, message::EDGE_ADD_PROP, request->clock1, request->req_id, request->elem1, request->elem2, prop);
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(std::move(request->client), msg.buf);
}

void
delete_edge_property_initiate(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    message::message msg;
    common::meta_element *me1;

    server->update_mutex.lock();
    if (check_elem(server, request->elem1, true) || check_elem(server, request->elem2, false)) {
        std::cerr << "node(s) not found or deleted, cannot create edge" << std::endl;
        server->update_mutex.unlock();
    } else {
        me1 = server->nodes.at(request->elem1);
        request->clock1 = ++server->vc.clocks->at(me1->get_loc());
        request->req_id = ++server->request_id;
        server->pending[request->req_id] = request;
        message::prepare_message(msg, message::EDGE_DELETE_PROP, request->clock1, request->req_id, request->elem1, request->elem2, request->key);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(std::move(request->client), msg.buf);
}

void
delete_end(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    server->update_mutex.lock();
    server->add_deleted_cache(request, *request->cached_req_ids);
    server->pending.erase(request->req_id);
    server->update_mutex.unlock();
    request->done = true;
}


// is node1 reachable from node2 by only traversing edges with properties given
// by edge_props?
void
reachability_request_initiate(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    server->update_mutex.lock();
    if (check_elem(server, request->elem1, true) || check_elem(server, request->elem2, true)) {
        std::cerr << "one of the nodes has been deleted, cannot perform request"
            << std::endl;
        server->update_mutex.unlock();
        message::message msg;
        message::prepare_message(msg, message::CLIENT_REPLY, false);
        server->send(std::move(request->client), msg.buf);
        return;
    }
    request->src_node.reset(new std::vector<size_t>(1, request->elem1));
    request->vector_clock.reset(new std::vector<uint64_t>(*server->vc.clocks));
    request->del_request = server->get_last_del_req(request);
    request->out_count = server->last_del;
    request->out_count->cnt++;

    reachability_request_propagate(server, request);
}

// caution: assuming we hold server->mutex
void
reachability_request_propagate(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request)
{
    message::message msg;
    common::meta_element *me1, *me2;
    me1 = server->nodes.at(request->elem1);
    me2 = server->nodes.at(request->elem2);
    request->req_id = ++server->request_id;
    for (auto &p: request->edge_props) {
        p.creat_time = request->req_id;
        p.del_time = MAX_TIME;
    }
    server->pending.insert(std::make_pair(request->req_id, request));
    std::cout << "Reachability request number " << request->req_id << " from source"
              << " request->elem " << request->elem1 << " " << me1->get_loc() 
              << " to destination request->elem " << request->elem2 << " " 
              << me2->get_loc() << std::endl;
    message::prepare_message(msg, message::REACHABLE_PROP, *request->vector_clock, 
        *request->src_node, -1, request->elem2, me2->get_loc(), 
        request->req_id, request->req_id, request->edge_props, request->ignore_cache);
    server->update_mutex.unlock();
    server->send(me1->get_loc(), msg.buf);
}

// caution: assuming we hold server->mutex
void
reachability_request_end(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request)
{       
    bool done = false;
    uint64_t req_id = request->req_id;
    if (request->cached_req_id == req_id) {
        done = true;
    } else {
        if (!server->is_deleted_cache_id(request->cached_req_id)) {
            done = true;
            server->add_good_cache_id(request->cached_req_id);
        } else {
            // request was served based on cache value that should be
            // invalidated; restarting request
            request->ignore_cache.emplace_back(request->cached_req_id);
            server->add_bad_cache_id(request->cached_req_id);
            reachability_request_propagate(server, request);
        }
    }
    server->pending.erase(req_id);

    if (done) {
        message::message msg;
        request->out_count->cnt--;
        server->update_mutex.unlock();
        std::cout << "Reachable reply is " << request->reachable << " for " << "request " 
                  << request->req_id << ", cached id " << request->cached_req_id << std::endl;
        message::prepare_message(msg, message::CLIENT_REPLY, request->reachable);
        server->send(std::move(request->client), msg.buf);
    }
}

void
dijkstra_request_initiate(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    server->update_mutex.lock();
    if (check_elem(server, request->elem1, true) || check_elem(server, request->elem2, true)) {
        std::cerr << "one of the nodes has been deleted, cannot perform request"
            << std::endl;
        server->update_mutex.unlock();
        message::message msg;
        std::vector<size_t> temp;
        message::prepare_message(msg, message::CLIENT_REPLY, (size_t) 0, temp);
        server->send(std::move(request->client), msg.buf);
    }
    request->vector_clock.reset(new std::vector<uint64_t>(*server->vc.clocks));
    /* maybe do this stuff for caching later 
    request->del_request = server->get_last_del_req(request);
    request->out_count = server->last_del;
    request->out_count->cnt++;
    */
/*
    message::message msg;
    common::meta_element *me1, *me2;
    me1 = server->nodes.at(request->elem1);
    me2 = server->nodes.at(request->elem2);
    request->req_id = ++server->request_id;
    for (auto &p: request->edge_props) {
        p.creat_time = request->req_id;
        p.del_time = MAX_TIME;
    }
    server->pending[request->req_id] = request;
    std::cout << "Dijkstra request number " << request->req_id << " from source"
<<<<<<< Updated upstream
              << " request->elem " << request->elem1 << " " << me1->get_loc() 
              << " to destination request->elem " << request->elem2 << " " 
              << me2->get_loc() 
              << " with is_widest_path = " << request->is_widest << std::endl;
    message::prepare_message(msg, message::DIJKSTRA_REQ, *request->vector_clock, 
        request->elem1, request->elem2, me2->get_loc(), 
        request->req_id, request->key, request->edge_props, request->is_widest, request->elem1, request->elem2);
=======
              << " request->elem " << request->elem1->get_node_handle() << " " << request->elem1->get_loc() 
              << " but request->elem creat time is " << request->elem1->get_creat_time() 
              << " to destination request->elem " << request->elem2->get_node_handle() << " " 
              << request->elem2->get_loc() 
              << " with is_widest_path = " << request->is_widest << std::endl;
    message::prepare_message(msg, message::DIJKSTRA_REQ, *request->vector_clock, 
        request->elem1->get_node_handle(), request->elem2->get_node_handle(), request->elem2->get_loc(), 
        request->req_id, request->key, request->edge_props, request->is_widest, request->elem1->get_node_handle(), request->elem2->get_node_handle());
>>>>>>> Stashed changes
    server->update_mutex.unlock();
    server->send(me1->get_loc(), msg.buf);
    */
}

void
dijkstra_request_end(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request)
{       
    /* caching stuff for later
    if (request->cached_req_id == request->req_id) {
        done = true;
    } else {
        if (!server->is_deleted_cache_id(request->cached_req_id)) {
            done = true;
            server->add_good_cache_id(request->cached_req_id);
        } else {
            // request was served based on cache value that should be
            // invalidated; restarting request
            request->ignore_cache.emplace_back(request->cached_req_id);
            server->add_bad_cache_id(request->cached_req_id);
            reachability_request_propagate(server, request);
        }
    }
    */
    server->pending.erase(request->req_id);

    message::message msg;
    //request->out_count->cnt--; // XXX do I need this
    server->update_mutex.unlock();
    if (request->is_widest){
        std::cout << "Widest path reply is a path of size " << request->path->size() << " and max cost " << request->cost << " for " << "request " 
            << request->req_id << std::endl;
    } else {
        std::cout << "Shortest path reply is a path of size " << request->path->size() << " and cost " << request->cost << " for " << "request " 
            << request->req_id << std::endl;
    }
    message::prepare_message(msg, message::CLIENT_DIJKSTRA_REPLY, request->cost, *request->path);
    server->send(std::move(request->client), msg.buf);
}

/*
// find the shortest or widest path from one node to another given a key for edge weights and only using edges with properties in edge_props
void
path_request(common::meta_element *node1, common::meta_element *node2, uint32_t edge_weight_key, bool is_widest_path,
        std::shared_ptr<std::vector<common::property>> edge_props, coordinator::central *server, bool &reachable, size_t &cost)
{
    coordinator::pending_req *request;
    message::message msg(message::DIJKSTRA_REQ);
    size_t req_id;
    
    server->update_mutex.lock();
    if (node1->get_del_time() < MAX_TIME || node2->get_del_time() < MAX_TIME)
    {
        std::cerr << "one of the nodes has been deleted, cannot perform request"
            << std::endl;
        server->update_mutex.unlock();
        reachable = false;
        return;
    }
    request = new coordinator::pending_req(&server->update_mutex);
    req_id = ++server->request_id;
    server->pending[req_id] = request;
#ifdef DEBUG
    std::cout << "Path request number " << req_id << " from node " << node1->get_addr() << node1->get_loc() 
        << " to node " << node1->get_addr() << " " << node2->get_loc() << std::endl;
#endif
    request->mutex.lock();
    message::prepare_message(msg, message::DIJKSTRA_REQ, (size_t) node1->get_addr(), (size_t) node2->get_addr(),
            node2->get_loc(), edge_weight_key, req_id, is_widest_path, *edge_props, *(server->vc.clocks));
    server->update_mutex.unlock();
    server->send(node1->get_loc(), msg.buf);
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    reachable = request->reachable;
    cost = request->cost;
#ifdef DEBUG
    std::cout << "Path found is " << reachable << " with cost " << cost
        << " for request " << req_id << std::endl;
#endif
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    server->pending.erase(req_id);
    server->update_mutex.unlock();
}
*/

// compute the local clustering coefficient for a node
void
clustering_request_initiate(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request)
{
    message::message msg;
    common::meta_element *me;
    
    server->update_mutex.lock();
    if (check_elem(server, request->elem1, true)) {
        std::cerr << "node has been deleted, cannot perform request" << std::endl;
        server->update_mutex.unlock();
        message::prepare_message(msg, message::CLIENT_CLUSTERING_REPLY, 0, 0);
        server->send(std::move(request->client), msg.buf);
    }
    me = server->nodes.at(request->elem1);
    request->req_id = ++server->request_id;
    server->pending[request->req_id] = request;
#ifdef DEBUG
    std::cout << "Clustering request number " << request->req_id << " for node "
        << request->elem1 << " " << me->get_loc() << std::endl;
#endif
    message::prepare_message(msg, message::CLUSTERING_REQ, request->elem1,
        request->req_id, request->edge_props, *(server->vc.clocks));
    request->out_count = server->last_del;
    request->out_count->cnt++;
    server->update_mutex.unlock();
    server->send(me->get_loc(), msg.buf);
}

void
clustering_request_end(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request)
{
    message::message msg;
#ifdef DEBUG
    std::cout << "Clustering reply is " << request->numerator << " over " << request->denominator
        << " for request " << request->req_id << std::endl;
#endif
    server->update_mutex.lock();
    server->pending.erase(request->req_id);
    request->out_count->cnt--;
    server->update_mutex.unlock();
    message::prepare_message(msg, message::CLIENT_CLUSTERING_REPLY, request->clustering_numerator, request->clustering_denominator);
    server->send(std::move(request->client), msg.buf);
}

// wake up thread waiting on the received message
void
handle_pending_req(coordinator::central *server, std::unique_ptr<message::message> msg,
    enum message::msg_type m_type, std::unique_ptr<po6::net::location> dummy)
{
    uint64_t req_id, cached_req_id;
    std::shared_ptr<coordinator::pending_req>request;
    bool is_reachable; // for reply
    size_t src_node; // for reply
    int src_loc; // for reply
    std::unique_ptr<std::vector<size_t>> del_nodes(new std::vector<size_t>()); // for reply
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>()); // for reply
    std::unique_ptr<std::vector<size_t>> cached_req_ids; // for reply
    size_t clustering_numerator; //for reply
    size_t clustering_denominator; //for reply
    common::meta_element *lnode; // for migration
    size_t coord_handle; // for migration
    int new_loc, from_loc; // for migration
    std::unique_ptr<std::vector<std::pair<size_t, size_t>>> found_path(new std::vector<std::pair<size_t, size_t>>); // for dijkstra requests
    size_t cost; //for reply
    
    switch(m_type) {
        /*
        case message::NODE_CREATE_ACK:
            message::unpack_message(*msg, m_type, req_id, node_handle);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->node_handle = node_handle;
            create_end(server, request);
            break;
        case message::EDGE_CREATE_ACK:
            message::unpack_message(*msg, m_type, req_id, edge_handle);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->edge_handle = edge_handle;
            create_end(server, request);
            break;
        */
        
        case message::NODE_DELETE_ACK:
        case message::EDGE_DELETE_ACK:
        case message::EDGE_DELETE_PROP_ACK:
            cached_req_ids.reset(new std::vector<size_t>());
            message::unpack_message(*msg, m_type, req_id, *cached_req_ids);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->cached_req_ids = std::move(cached_req_ids);
            delete_end(server, request);
            break;
        
        case message::REACHABLE_REPLY:
            message::unpack_message(*msg, message::REACHABLE_REPLY, req_id, is_reachable,
                src_node, src_loc, *del_nodes, *del_times, cached_req_id);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            request->reachable = is_reachable;
            request->cached_req_id = cached_req_id;
            if (request->del_request) {
                if (request->del_request->done) {
                    reachability_request_end(server, request);
                } else {
                    request->done = true;
                    server->update_mutex.unlock();
                }
            } else {
                reachability_request_end(server, request);
            }
            break;

        case message::CACHE_UPDATE_ACK:
            server->update_mutex.lock();
            if ((++server->cache_acks) == server->num_shards) {
                coord_daemon_end(server);
            } else {
                server->update_mutex.unlock();
            }
            break;

        case message::DIJKSTRA_REPLY:
            message::unpack_message(*msg, message::DIJKSTRA_REPLY, req_id, cost, *found_path);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            request->cost = cost;
            request->path = std::move(found_path);
            dijkstra_request_end(server, request);
            break;

        case message::CLUSTERING_REPLY:
            message::unpack_message(*msg, message::CLUSTERING_REPLY, req_id, clustering_numerator, clustering_denominator);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->clustering_numerator = clustering_numerator;
            request->clustering_denominator = clustering_denominator;
            clustering_request_end(server, request);
            break;

        case message::COORD_NODE_MIGRATE:
            message::unpack_message(*msg, message::COORD_NODE_MIGRATE, coord_handle, new_loc, from_loc);
            server->update_mutex.lock();
            lnode = server->nodes.at(coord_handle);
            lnode->update_loc(new_loc);
            message::prepare_message(*msg, message::COORD_NODE_MIGRATE_ACK, server->vc.clocks->at(from_loc));
            server->update_mutex.unlock();
            server->send(from_loc, msg->buf);
            break;
        
        default:
            std::cerr << "unexpected msg type " << m_type << std::endl;
    }
}

// handle responses from shards
void
shard_msg_handler(coordinator::central *server)
{
    busybee_returncode ret;
    po6::net::location sender(COORD_IPADDR, COORD_PORT);
    message::message msg(message::ERROR);
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    std::unique_ptr<coordinator::thread::unstarted_thread> thr;
    
    while (1) {
        if ((ret = server->rec_bb.recv(&sender, &msg.buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg.reset(new message::message(msg));
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        thr.reset(new coordinator::thread::unstarted_thread(handle_pending_req,
            server, std::move(rec_msg), mtype, NULL));
        server->thread_pool.add_request(std::move(thr), false);
    }
}

// call appropriate function based on msg from client
void
handle_client_req(coordinator::central *server, std::unique_ptr<message::message> msg,
    enum message::msg_type m_type, std::unique_ptr<po6::net::location> client_loc)
{
    auto request = std::make_shared<coordinator::pending_req>(m_type);
    request->client = std::move(client_loc);

    switch (m_type)
    {
        case message::CLIENT_NODE_CREATE_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_CREATE_REQ, request->client->port);
            create_node(server, request);
            break;

        case message::CLIENT_EDGE_CREATE_REQ: 
            message::unpack_message(*msg, message::CLIENT_EDGE_CREATE_REQ,
                    request->client->port, request->elem1, request->elem2);
            create_edge(server, request);
            break;

        case message::CLIENT_NODE_DELETE_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_DELETE_REQ,
                    request->client->port, request->elem1);
            delete_node_initiate(server, request);
            break;

        case message::CLIENT_EDGE_DELETE_REQ: 
            message::unpack_message(*msg, message::CLIENT_EDGE_DELETE_REQ,
                    request->client->port, request->elem1, request->elem2);
            delete_edge_initiate(server, request);
            break;

        case message::CLIENT_ADD_EDGE_PROP:
            message::unpack_message(*msg, message::CLIENT_ADD_EDGE_PROP, 
                    request->client->port, request->elem1, request->elem2, request->key, request->value);
            add_edge_property(server, request);
            break;

        case message::CLIENT_DEL_EDGE_PROP:
            message::unpack_message(*msg, message::CLIENT_DEL_EDGE_PROP, 
                    request->client->port, request->elem1, request->elem2, request->key);
            delete_edge_property_initiate(server, request);
            break;

        case message::CLIENT_REACHABLE_REQ:
            message::unpack_message(*msg, message::CLIENT_REACHABLE_REQ, 
                    request->client->port, request->elem1, request->elem2, request->edge_props);
            reachability_request_initiate(server, request);
            break;

        case message::CLIENT_DIJKSTRA_REQ:
            message::unpack_message(*msg, message::CLIENT_DIJKSTRA_REQ,
                    request->client->port, request->elem1, request->elem2, request->key, request->is_widest, request->edge_props);
            dijkstra_request_initiate(server, request);
            break;

/*
        case message::CLIENT_CLUSTERING_REQ: 
            message::unpack_message(*msg, message::CLIENT_CLUSTERING_REQ, 
                    client_port, elem1, *edge_props);
            client_loc->port = client_port;
            request.reset(new coordinator::pending_req(m_type, (common::meta_element*)elem1, NULL, edge_props, std::move(client_loc)));
            clustering_request_initiate(server, request);
            break;
*/
        default:
            std::cerr << "invalid client msg code" << m_type << std::endl;
    }
}

// handle requests from client
void
client_msg_handler(coordinator::central *server)
{
    busybee_returncode ret;
    po6::net::location sender(COORD_IPADDR, COORD_PORT);
    message::message msg(message::ERROR);
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    std::unique_ptr<po6::net::location> client_loc;
    std::unique_ptr<coordinator::thread::unstarted_thread> thr;

    while (1)
    {
        if ((ret = server->client_rec_bb.recv(&sender, &msg.buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg.reset(new message::message(msg));
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        client_loc.reset(new po6::net::location(sender));
        thr.reset(new coordinator::thread::unstarted_thread(handle_client_req, server, std::move(rec_msg), mtype, std::move(client_loc)));
        server->thread_pool.add_request(std::move(thr), true);
    }
}

// periodically update cache at all shards
void
coord_daemon_initiate(coordinator::central *server)
{
    std::vector<uint64_t> good, bad;
    uint64_t perm_del_id = 0;
    message::message msg;
    std::chrono::seconds duration(DAEMON_PERIOD); // execute every DAEMON_PERIOD seconds
    std::this_thread::sleep_for(duration);
    server->update_mutex.lock();
    while (server->first_del->cnt == 0 && server->first_del != server->last_del) {
        perm_del_id = server->first_del->req_id;
        if (server->first_del->next) {
            server->first_del = server->first_del->next;
        } else {
            break;
        }
    }
    if ((good.size() != 0) || (bad.size() != 0)) {
        std::copy(server->good_cache_ids->begin(), server->good_cache_ids->end(), std::back_inserter(good));
        std::copy(server->bad_cache_ids->begin(), server->bad_cache_ids->end(), std::back_inserter(bad));
        server->transient_bad_cache_ids = std::move(server->bad_cache_ids);
        server->bad_cache_ids.reset(new std::unordered_set<uint64_t>());
        server->good_cache_ids.reset(new std::unordered_set<uint64_t>());
        server->update_mutex.unlock();
    } else {
        server->update_mutex.unlock();
    }
    for (uint32_t i = 0; i < server->num_shards; i++) {
        message::prepare_message(msg, message::CACHE_UPDATE, good, bad, perm_del_id);
        server->send(i, msg.buf);
    }

}

// caution: assuming we hold server->update_mutex
void
coord_daemon_end(coordinator::central *server)
{
    assert(server->cache_acks == server->num_shards);
    server->cache_acks = 0; 
    server->update_mutex.unlock();
    coord_daemon_initiate(server);
}

int
main()
{
    coordinator::central server;
    std::thread *t;
    //std::set_terminate(debug_terminate);
    
    std::cout << "Weaver: coordinator" << std::endl;

    // initialize shard msg receiving thread
    t = new std::thread(shard_msg_handler, &server);
    t->detach();

    // initialize client msg receiving thread
    t = new std::thread(coord_daemon_initiate, &server);
    t->detach();

    // call periodic cache update function
    client_msg_handler(&server);
}
