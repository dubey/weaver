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
#include "node_prog/node_program.h"
#include "node_prog/node_prog_type.h"

void coord_daemon_end(coordinator::central *server);

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

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_start_coord(coordinator::central *server, message::message &msg, std::shared_ptr<coordinator::pending_req> request)
{
    node_prog::prog_type ignore;
    printf("coordinator ZAAAAAAAAAAAAAAAAAA\n");
    std::vector<std::pair<uint64_t, ParamsType>> initial_args;

    message::unpack_message(msg, message::CLIENT_NODE_PROG_REQ, request->client->port, ignore, initial_args);

    std::unordered_map<int, std::vector<std::pair<uint64_t, ParamsType>>> initial_batches; // map from locations to a list of start_node_params to send to that shard
    server->update_mutex.lock();

    for (std::pair<uint64_t, ParamsType> &node_params_pair : initial_args) {
        if (check_elem(server, node_params_pair.first, true)) {
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
    for (auto &batch_pair : initial_batches) {
        message::prepare_message(msg_to_send, message::NODE_PROG, request->pType, *request->vector_clock, 
                request->req_id, batch_pair.second);
        server->send(batch_pair.first, msg_to_send.buf); // later change to send without update mutex lock
    }
    server->update_mutex.unlock();
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void
node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: unpack_and_run_db(db::graph *G, message::message &msg)
{
}

// wake up thread waiting on the received message
void
handle_pending_req(coordinator::central *server, std::unique_ptr<message::message> msg,
    enum message::msg_type m_type, std::unique_ptr<po6::net::location> dummy)
{
    uint64_t req_id, cached_req_id;
    std::shared_ptr<coordinator::pending_req>request;
    std::unique_ptr<std::vector<size_t>> cached_req_ids; // for reply
    common::meta_element *lnode; // for migration
    size_t coord_handle; // for migration
    int new_loc, from_loc; // for migration
    node_prog::prog_type pType;
    
    switch(m_type) {
       
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
        
        case message::CACHE_UPDATE_ACK:
            server->update_mutex.lock();
            if ((++server->cache_acks) == server->num_shards) {
                coord_daemon_end(server);
            } else {
                server->update_mutex.unlock();
            }
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

        // response from a shard
        case message::NODE_PROG:
            message::unpack_message(*msg, message::NODE_PROG, pType, req_id); // don't unpack rest
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            // send same message along to client
            server->send(std::move(request->client), msg->buf);
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

        case message::CLIENT_NODE_PROG_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_PROG_REQ, request->client->port, request->pType);
            std::cout << "server got type " << request->pType << std::endl;
            node_prog::programs.at(request->pType)->unpack_and_start_coord(server, *msg, request);
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
