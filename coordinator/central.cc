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

// TODO convert from blocking calls to state machine
// create a node
void*
create_node(coordinator::central *server)
{
    coordinator::pending_req *request;
    common::meta_element *new_node;
    uint64_t creat_time;
    message::message msg(message::NODE_CREATE_REQ);
    int shard_id;
    size_t req_id;

    server->update_mutex.lock();
    server->port_ctr = (server->port_ctr + 1) % NUM_SHARDS;
    shard_id = server->port_ctr; // node will be placed on this shard server
    creat_time = ++server->vc.clocks->at(shard_id); // incrementing vector clock
    request = new coordinator::pending_req(&server->update_mutex);
    request->mutex.lock();
    req_id = ++server->request_id;
    server->pending[req_id] = request;
    message::prepare_message(msg, message::NODE_CREATE_REQ, creat_time, req_id);
    server->update_mutex.unlock();
    server->send(shard_id, msg.buf);
    
    // waiting for reply from shard
    while (request->waiting)
    {
        request->reply.wait();
    }
    new_node = new common::meta_element(shard_id, creat_time, MAX_TIME, request->addr);
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    server->pending.erase(req_id);
    server->add_node(new_node, req_id);
    server->update_mutex.unlock();
#ifdef DEBUG
    std::cout << "Node id is " << (void *)new_node << " " <<
        new_node->get_addr() << " " << new_node->get_loc() << std::endl;
#endif
    return (void *)new_node;
}

// create an edge
void*
create_edge(common::meta_element *node1, common::meta_element *node2, coordinator::central *server)
{
    coordinator::pending_req *request;
    common::meta_element *new_edge;
    uint64_t creat_time1, creat_time2;
    message::message msg(message::EDGE_CREATE_REQ);
    size_t req_id;

    //TODO need checks for given node_handles
    //TODO need to increment clock on both shards?
    server->update_mutex.lock();
    creat_time1 = ++server->vc.clocks->at(node1->get_loc());
    creat_time2 = ++server->vc.clocks->at(node2->get_loc());
    request = new coordinator::pending_req(&server->update_mutex);
    request->mutex.lock();
    req_id = ++server->request_id;
    server->pending[req_id] = request;
    size_t node1_addr = (size_t)node1->get_addr();
    size_t node2_addr = (size_t)node2->get_addr();
    int loc1 = node1->get_loc();
    int loc2 = node2->get_loc();
    server->update_mutex.unlock();
    message::prepare_message(msg, message::EDGE_CREATE_REQ, creat_time1, req_id, node1_addr, node2_addr,
        loc2, creat_time2);
    server->send(loc1, msg.buf);
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    new_edge = new common::meta_element(node1->get_loc(), creat_time1, MAX_TIME, request->addr);
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    server->pending.erase(req_id);
    server->add_edge(new_edge);
    server->update_mutex.unlock();
#ifdef DEBUG
    std::cout << "Edge id is " << (void *)new_edge << " " <<
        new_edge->get_addr() << std::endl;
#endif
    return (void *)new_edge;
}

// delete a node
void
delete_node(common::meta_element *node, coordinator::central *server)
{
    coordinator::pending_req *request;
    uint64_t del_time;
    message::message msg(message::NODE_DELETE_REQ);
    size_t req_id;

    server->update_mutex.lock();
    if (node->get_del_time() < MAX_TIME)
    {
        std::cerr << "cannot delete node twice" << std::endl;
        server->update_mutex.unlock();
        return;
    }
    del_time = ++server->vc.clocks->at(node->get_loc());
    node->update_del_time(del_time);
    request = new coordinator::pending_req(&server->update_mutex);
    request->mutex.lock();
    req_id = ++server->request_id;
    server->pending[req_id] = request;
    size_t node_addr = (size_t)node->get_addr();
    message::prepare_message(msg, message::NODE_DELETE_REQ, del_time, req_id, node_addr);
    server->add_pending_del_req(req_id);
    server->update_mutex.unlock();
    server->send(node->get_loc(), msg.buf);

    // waiting for reply from shard
    while (request->waiting)
    {
        request->reply.wait();
    }
    request->mutex.unlock(); // okay because no more concurrency for this request
    server->update_mutex.lock();
    server->add_deleted_cache(req_id, *request->cached_req_ids);
    delete request;
    server->pending.erase(req_id);
    server->update_mutex.unlock();
}

// delete an edge
void
delete_edge(common::meta_element *node, common::meta_element *edge, coordinator::central *server)
{
    coordinator::pending_req *request;
    uint64_t del_time;
    message::message msg(message::EDGE_DELETE_REQ);
    size_t req_id;

    server->update_mutex.lock();
    if (edge->get_del_time() < MAX_TIME || node->get_del_time() < MAX_TIME)
    {
        std::cerr << "cannot delete edge/node twice" << std::endl;
        server->update_mutex.unlock();
        return;
    }
    del_time = ++server->vc.clocks->at(node->get_loc());
    edge->update_del_time(del_time);
    request = new coordinator::pending_req(&server->update_mutex);
    request->mutex.lock();
    req_id = ++server->request_id;
    server->pending[req_id] = request;
    size_t node_addr = (size_t)node->get_addr();
    size_t edge_addr = (size_t)edge->get_addr();
    message::prepare_message(msg, message::EDGE_DELETE_REQ, del_time, req_id, node_addr, edge_addr);
    server->add_pending_del_req(req_id);
    server->update_mutex.unlock();
    server->send(node->get_loc(), msg.buf);
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    request->mutex.unlock(); // okay because no more concurrency for this request
    server->update_mutex.lock();
    server->add_deleted_cache(req_id, *request->cached_req_ids);
    delete request;
    server->pending.erase(req_id);
    server->update_mutex.unlock();
}

// add a property, i.e. a key-value pair to an edge
void 
add_edge_property(common::meta_element *node, common::meta_element *edge, uint32_t key, size_t value, coordinator::central *server)
{
    uint64_t time;
    message::message msg(message::EDGE_ADD_PROP);
    size_t req_id;

    server->update_mutex.lock();
    if (edge->get_del_time() < MAX_TIME || node->get_del_time() < MAX_TIME)
    {
        std::cerr << "cannot add property to deleted edge/node" << std::endl;
        server->update_mutex.unlock();
        return;
    }
    time = ++server->vc.clocks->at(node->get_loc());
    req_id = ++server->request_id;
    size_t node_addr = (size_t)node->get_addr();
    size_t edge_addr = (size_t)edge->get_addr();
    common::property prop(key, value, req_id);
    message::prepare_message(msg, message::EDGE_ADD_PROP, time, req_id, node_addr, edge_addr, prop);
    server->update_mutex.unlock();
    server->send(node->get_loc(), msg.buf);
}

void
delete_edge_property(common::meta_element *node, common::meta_element *edge, 
    uint32_t key, coordinator::central *server)
{
    coordinator::pending_req *request;
    uint64_t time;
    message::message msg(message::EDGE_DELETE_PROP);
    size_t req_id;

    server->update_mutex.lock();
    if (edge->get_del_time() < MAX_TIME || node->get_del_time() < MAX_TIME)
    {
        std::cerr << "cannot delete property of deleted edge/node" << std::endl;
        server->update_mutex.unlock();
        return;
    }
    time = ++server->vc.clocks->at(node->get_loc());
    request = new coordinator::pending_req(&server->update_mutex);
    request->mutex.lock();
    req_id = ++server->request_id;
    server->pending[req_id] = request;
    size_t node_addr = (size_t)node->get_addr();
    size_t edge_addr = (size_t)edge->get_addr();
    message::prepare_message(msg, message::EDGE_DELETE_PROP, time, req_id, node_addr, edge_addr, key);
    server->add_pending_del_req(req_id);
    server->update_mutex.unlock();
    server->send(node->get_loc(), msg.buf);
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    request->mutex.unlock(); // okay because no more concurrency for this request
    server->update_mutex.lock();
    server->add_deleted_cache(req_id, *request->cached_req_ids);
    delete request;
    server->pending.erase(req_id);
    server->update_mutex.unlock();
}

// is node1 reachable from node2 by only traversing edges with properties given
// by edge_props?
bool
reachability_request(common::meta_element *node1, common::meta_element *node2, 
    std::shared_ptr<std::vector<common::property>> edge_props, coordinator::central *server)
{
    coordinator::pending_req *request;
    message::message msg(message::REACHABLE_PROP);
    std::vector<size_t> src; // vector to hold src node
    src.push_back((size_t)node1->get_addr());
    size_t req_id, cached_id;
    bool ret;
    std::shared_ptr<std::vector<uint64_t>> vector_clock;
    size_t last_del_req;
    bool done_loop = false;
    std::vector<size_t> ignore_cache;
    
   
    server->update_mutex.lock();
    if (node1->get_del_time() < MAX_TIME || node2->get_del_time() < MAX_TIME)
    {
        std::cerr << "one of the nodes has been deleted, cannot perform request"
            << std::endl;
        server->update_mutex.unlock();
        return false;
    }
    vector_clock.reset(new std::vector<uint64_t>(*server->vc.clocks));
    request = new coordinator::pending_req(&server->update_mutex);
    last_del_req = server->get_last_del_req((size_t)request);

    while (!done_loop)
    {
        req_id = ++server->request_id;
        for (auto &p: *edge_props)
        {
            p.creat_time = req_id;
            p.del_time = MAX_TIME;
        }
        server->pending[req_id] = request;
        std::cout << "Reachability request number " << req_id << " from source"
                  << " node " << node1->get_addr() << " " << node1->get_loc() << " to destination node "
                  << node2->get_addr() << " " << node2->get_loc() << std::endl;
        request->mutex.lock();
        message::prepare_message(msg, message::REACHABLE_PROP, *vector_clock, src, -1, node2->get_addr(), node2->get_loc(),
            req_id, req_id, *edge_props, ignore_cache);
        server->update_mutex.unlock();
        server->send(node1->get_loc(), msg.buf);
        
        while (request->waiting)
        {
            request->reply.wait();
        }
        request->mutex.unlock(); // no more concurrency for this request
        server->update_mutex.lock();
        if (last_del_req != 0)
        {
            while (server->still_pending_del_req(last_del_req))
            {
                request->del_reply.wait();
            }
            last_del_req = 0; // no need to check for future iterations
        }
        if (request->cached_req_id == req_id) {
            done_loop = true;
        } else {
            //std::cout << "cached req id " << request->cached_req_id << std::endl;
            if (!server->is_deleted_cache_id(request->cached_req_id)) {
                done_loop = true;
                server->add_good_cache_id(request->cached_req_id);
                cached_id = request->cached_req_id;
            } else {
                // request was served based on cache value that should be
                // invalidated; restarting request
                ignore_cache.push_back(request->cached_req_id);
                server->add_bad_cache_id(request->cached_req_id);
                delete request;
                request = new coordinator::pending_req(&server->update_mutex);
            }
        }
        server->pending.erase(req_id);
    }

    server->update_mutex.unlock();
    ret = request->reachable;
    std::cout << "Reachable reply is " << ret << " for " << "request " << req_id << ", cached id " << cached_id << std::endl;
    delete request;
    return ret;
}

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

// compute the local clustering coefficient for a node
void
clustering_request(common::meta_element *node,
    std::shared_ptr<std::vector<common::property>> edge_props,
    coordinator::central *server, size_t &numerator, size_t &denominator)
{
    coordinator::pending_req *request;
    message::message msg(message::CLUSTERING_REQ);
    size_t req_id;
    
    server->update_mutex.lock();
    if (node->get_del_time() < MAX_TIME)
    {
        std::cerr << "node has been deleted, cannot perform request"
            << std::endl;
        server->update_mutex.unlock();
        return; // or NaN?
    }
    request = new coordinator::pending_req(&server->update_mutex);
    req_id = ++server->request_id;
    server->pending[req_id] = request;
#ifdef DEBUG
    std::cout << "Clustering request number " << req_id << " for node "
              << node->get_addr() << " " << node->get_loc() << std::endl;
#endif
    request->mutex.lock();
    message::prepare_message(msg, message::CLUSTERING_REQ, (size_t) node->get_addr(),
            req_id, *edge_props, *(server->vc.clocks));
    server->update_mutex.unlock();
    server->send(node->get_loc(), msg.buf);
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    numerator = request->clustering_numerator;
    denominator = request->clustering_denominator;
#ifdef DEBUG
    std::cout << "Clustering reply is " << numerator << " over " << denominator
        << " for request " << req_id << std::endl;
#endif
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    server->pending.erase(req_id);
    server->update_mutex.unlock();
}

// wake up thread waiting on the received message
void
handle_pending_req(coordinator::central *server, std::unique_ptr<message::message> msg,
    enum message::msg_type m_type, std::unique_ptr<po6::net::location> dummy)
{
    size_t req_id, cached_req_id;
    coordinator::pending_req *request;
    size_t mem_addr;
    bool is_reachable; // for reply
    size_t src_node; // for reply
    int src_loc; // for reply
    std::unique_ptr<std::vector<size_t>> del_nodes(new std::vector<size_t>()); // for reply
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>()); // for reply
    std::unique_ptr<std::vector<size_t>> cached_req_ids; // for reply
    size_t clustering_numerator; //for reply
    size_t clustering_denominator; //for reply
    common::meta_element *lnode; // for migration
    size_t coord_handle, node_handle; // for migration
    int new_loc, from_loc; // for migration
    size_t cost; //for reply
    
    switch(m_type)
    {
        case message::NODE_CREATE_ACK:
        case message::EDGE_CREATE_ACK:
            message::unpack_message(*msg, m_type, req_id, mem_addr);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->mutex.lock();
            request->addr = mem_addr;
            request->waiting = false;
            request->reply.signal();
            request->mutex.unlock();
            break;
        
        case message::NODE_DELETE_ACK:
        case message::EDGE_DELETE_ACK:
        case message::EDGE_DELETE_PROP_ACK:
            cached_req_ids.reset(new std::vector<size_t>());
            message::unpack_message(*msg, m_type, req_id, *cached_req_ids);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->mutex.lock();
            request->waiting = false;
            request->cached_req_ids = std::move(cached_req_ids);
            request->reply.signal();
            request->mutex.unlock();
            break;
        
        case message::REACHABLE_REPLY:
            message::unpack_message(*msg, message::REACHABLE_REPLY, req_id, is_reachable,
                src_node, src_loc, *del_nodes, *del_times, cached_req_id);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->mutex.lock();
            request->reachable = is_reachable;
            request->waiting = false;
            request->cached_req_id = cached_req_id;
            request->reply.signal();
            request->mutex.unlock();
            break;

        case message::CACHE_UPDATE_ACK:
            server->update_mutex.lock();
            if ((++server->cache_acks) == server->num_shards) {
                server->cache_cond.signal();
            }
            server->update_mutex.unlock();
            break;

        case message::DIJKSTRA_REPLY:
            message::unpack_message(*msg, message::DIJKSTRA_REPLY, req_id, is_reachable, cost);
            server->update_mutex.lock();
            request = server->pending[req_id];
            server->update_mutex.unlock();
            request->mutex.lock();
            request->reachable = is_reachable;
            request->cost = cost;
            request->waiting = false;
            request->reply.signal();
            request->mutex.unlock();
            break;

        case message::CLUSTERING_REPLY:
            message::unpack_message(*msg, message::CLUSTERING_REPLY, req_id, clustering_numerator, clustering_denominator);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->mutex.lock();
            request->clustering_numerator = clustering_numerator;
            request->clustering_denominator = clustering_denominator;
            request->waiting = false;
            request->reply.signal();
            request->mutex.unlock();
            break;

        case message::COORD_NODE_MIGRATE:
            message::unpack_message(*msg, message::COORD_NODE_MIGRATE, coord_handle, new_loc, node_handle, from_loc);
            server->update_mutex.lock();
            lnode = (common::meta_element *)server->nodes[coord_handle];
            lnode->update_loc(new_loc);
            lnode->update_addr(node_handle);
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
    
    while (1)
    {
        if ((ret = server->rec_bb.recv(&sender, &msg.buf)) != BUSYBEE_SUCCESS)
        {
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
    uint16_t client_port;
    size_t elem1, elem2, value;
    uint32_t key;
    size_t new_elem;
    bool reachable, is_widest_path;
    size_t clustering_numerator;
    size_t clustering_denominator;
    size_t cost;
    auto edge_props = std::make_shared<std::vector<common::property>>();
    switch (m_type)
    {
        case message::CLIENT_NODE_CREATE_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_CREATE_REQ, client_port);
            client_loc->port = client_port;
            new_elem = (size_t)create_node(server);
            message::prepare_message(*msg, message::CLIENT_REPLY, new_elem);
            server->client_send(*client_loc, msg->buf);
            break;

        case message::CLIENT_EDGE_CREATE_REQ: 
            message::unpack_message(*msg, message::CLIENT_EDGE_CREATE_REQ,
                    client_port, elem1, elem2);
            client_loc->port = client_port;
            new_elem = (size_t)create_edge((common::meta_element *)elem1, (common::meta_element *)elem2, server);
            message::prepare_message(*msg, message::CLIENT_REPLY, new_elem);
            server->client_send(*client_loc, msg->buf);
            break;

        case message::CLIENT_NODE_DELETE_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_DELETE_REQ,
                    client_port, elem1);
            delete_node((common::meta_element *)elem1, server);
            break;

        case message::CLIENT_EDGE_DELETE_REQ: 
            message::unpack_message(*msg, message::CLIENT_EDGE_DELETE_REQ,
                    client_port, elem1, elem2);
            delete_edge((common::meta_element *)elem1, (common::meta_element *)elem2, server);
            break;

        case message::CLIENT_REACHABLE_REQ:
            message::unpack_message(*msg, message::CLIENT_REACHABLE_REQ, 
                    client_port, elem1, elem2, *edge_props);
            client_loc->port = client_port;
            reachable = reachability_request((common::meta_element *)elem1,
                (common::meta_element *)elem2, edge_props, server);
            msg->change_type(message::CLIENT_REPLY);
            message::prepare_message(*msg, message::CLIENT_REPLY, reachable);
            server->client_send(*client_loc, msg->buf);
            break;

        case message::CLIENT_DIJKSTRA_REQ: 
            message::unpack_message(*msg, message::CLIENT_DIJKSTRA_REQ, client_port,
                    elem1, elem2, key, is_widest_path, *edge_props);
            client_loc->port = client_port;
            path_request((common::meta_element *) elem1, (common::meta_element *) elem2, key,
                    is_widest_path, edge_props, server, reachable, cost);
            message::prepare_message(*msg, message::CLIENT_DIJKSTRA_REPLY, reachable, cost);
            server->client_send(*client_loc, msg->buf);
            break;

        case message::CLIENT_CLUSTERING_REQ: 
            message::unpack_message(*msg, message::CLIENT_CLUSTERING_REQ, client_port,
                    elem1, *edge_props);
            client_loc->port = client_port;
            clustering_request((common::meta_element *)elem1, edge_props,
                    server, clustering_numerator, clustering_denominator);
            message::prepare_message(*msg, message::CLIENT_CLUSTERING_REPLY,
                    clustering_numerator, clustering_denominator);
            server->client_send(*client_loc, msg->buf);
            break;

        case message::CLIENT_ADD_EDGE_PROP:
            message::unpack_message(*msg, message::CLIENT_ADD_EDGE_PROP,
                    elem1, elem2, key, value);
            add_edge_property((common::meta_element *)elem1, (common::meta_element *)elem2, key, value, server);
            break;

        case message::CLIENT_DEL_EDGE_PROP:
            message::unpack_message(*msg, message::CLIENT_DEL_EDGE_PROP,
                    elem1, elem2, key);
            delete_edge_property((common::meta_element *)elem1, (common::meta_element *)elem2, key, server);
            break;

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
        if ((ret = server->client_rec_bb.recv(&sender, &msg.buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg.reset(new message::message(msg));
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        client_loc.reset(new po6::net::location(sender));
        thr.reset(new coordinator::thread::unstarted_thread(handle_client_req,
            server, std::move(rec_msg), mtype, std::move(client_loc)));
        server->thread_pool.add_request(std::move(thr), true);
    }
}

// periodically update cache at all shards
void
coord_daemon(coordinator::central *server)
{
    std::vector<size_t> good, bad;
    message::message msg(message::CACHE_UPDATE);
    while (true)
    {
        std::chrono::seconds duration(500); // execute every 5 seconds
        std::this_thread::sleep_for(duration);
        server->update_mutex.lock();
        std::copy(server->good_cache_ids->begin(), server->good_cache_ids->end(), std::back_inserter(good));
        std::copy(server->bad_cache_ids->begin(), server->bad_cache_ids->end(), std::back_inserter(bad));
        if ((good.size() != 0) || (bad.size() != 0)) {
            for (int i = 0; i < server->num_shards; i++)
            {
                message::prepare_message(msg, message::CACHE_UPDATE, good, bad);
                server->send(i, msg.buf);
            }
            server->transient_bad_cache_ids = std::move(server->bad_cache_ids);
            server->bad_cache_ids.reset(new std::unordered_set<size_t>());
            server->good_cache_ids.reset(new std::unordered_set<size_t>());
            while (server->cache_acks < server->num_shards)
            {
                server->cache_cond.wait();
            }
            server->cache_acks = 0;
        }
        server->update_mutex.unlock();
    }
}

int
main()
{
    coordinator::central server;
    std::thread *t;
    
    std::cout << "Weaver: coordinator" << std::endl;
    // initialize shard msg receiving thread
    t = new std::thread(shard_msg_handler, &server);
    t->detach();

    // initialize client msg receiving thread
    //t = new std::thread(coord_daemon, &server);
    //t->detach();

    // call periodic cache update function
    client_msg_handler(&server);
}
