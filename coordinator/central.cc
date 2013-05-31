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
#include <fstream>
#include <signal.h>
#include <limits>
#include "e/buffer.h"
#include "busybee_constants.h"

#include "central.h"
#include "common/meta_element.h"
#include "common/message.h"
#include "common/debug.h"
#include "node_prog/node_program.h"
#include "node_prog/node_prog_type.h"
#include "db/element/remote_node.h"

static std::unordered_map<uint64_t, std::vector<uint64_t>> graph;
static coordinator::central *cserver;
void coord_daemon_end(coordinator::central *server);

void
record_time(coordinator::central *server)
{
    double mtime;
    clock_gettime(CLOCK_MONOTONIC, &server->mtime);
    mtime = server->mtime.tv_sec + ((double)server->mtime.tv_nsec)/(1000000000ULL);
    server->migr_times.emplace_back(mtime);
}

void exit_weaver()
{
    message::message msg;
    for (uint64_t s = 1; s <= NUM_SHARDS; s++) {
        message::prepare_message(msg, message::EXIT_WEAVER);
        cserver->send(s, msg.buf);
    }
    std::ofstream migr_file, shard_file;
    migr_file.open("migrations");
    migr_file.precision(std::numeric_limits<double>::digits10);
    for (double t: cserver->migr_times) {
        migr_file << t << std::endl;
    }
    migr_file.close();
    shard_file.open("shard_counts");
    for (int i = 0; i < NUM_SHARDS; i++) {
        shard_file << cserver->shard_node_count[i] << std::endl;
    }
    shard_file.close();
    exit(0);
}

// create a node
void
create_node(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    message::message msg;
    uint64_t loc;
    uint64_t clock;
    uint64_t req_id;
    
    server->update_mutex.lock();
    server->port_ctr = (server->port_ctr + 1) % NUM_SHARDS;
    loc = server->port_ctr+1; // node will be placed on this shard server
    clock = ++server->vc.clocks->at(loc-1);
    req_id = ++server->request_id;
    server->shard_node_count[loc-1]++;
    server->update_mutex.unlock();
    
    message::prepare_message(msg, message::NODE_CREATE_REQ, clock, req_id);
    server->send(loc, msg.buf);
    server->add_node(new common::meta_element(loc), req_id);
    graph.emplace(req_id, std::vector<uint64_t>());
    message::prepare_message(msg, message::CLIENT_REPLY, req_id);
    server->send(request->client, msg.buf);
}

// create an edge
void
create_edge(coordinator::central *server, std::shared_ptr<coordinator::pending_req>request)
{
    message::message msg;
    uint64_t req_id;
    uint64_t clock1, clock2;
    common::meta_element *me1, *me2;
    uint64_t loc1, loc2;

    server->update_mutex.lock();
    // checks for given node_handles
    if (check_elem(server, request->elem1, true) || check_elem(server, request->elem2, true)) {
        std::cerr << "node(s) not found or deleted, cannot create edge" << std::endl;
        message::prepare_message(msg, message::CLIENT_REPLY, 0);
        server->send(request->client, msg.buf);
        server->update_mutex.unlock();
        return;
    }
    // good to go
    me1 = server->nodes.at(request->elem1);
    me2 = server->nodes.at(request->elem2);
    clock1 = ++server->vc.clocks->at(me1->get_loc()-1);
    clock2 = ++server->vc.clocks->at(me2->get_loc()-1);
    req_id = ++server->request_id;
    server->update_mutex.unlock();

    loc1 = me1->get_loc();
    loc2 = me2->get_loc();
    message::prepare_message(msg, message::EDGE_CREATE_REQ, clock1, req_id, request->elem1, request->elem2, loc2, clock2);
    server->send(loc1, msg.buf);
    server->add_edge(new common::meta_element(loc1), req_id);
    graph.at(request->elem1).emplace_back(request->elem2);
    message::prepare_message(msg, message::CLIENT_REPLY, req_id);
    server->send(request->client, msg.buf);
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
        request->clock1 = ++server->vc.clocks->at(me->get_loc()-1);
        me->update_del_time(request->clock1);
        request->req_id = ++server->request_id;
        server->pending[request->req_id] = request;
        message::prepare_message(msg, message::NODE_DELETE_REQ, request->clock1, request->req_id, request->elem1);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(request->client, msg.buf);
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
        request->clock1 = ++server->vc.clocks->at(me1->get_loc()-1);
        me2->update_del_time(request->clock1);
        request->req_id = ++server->request_id;
        server->pending[request->req_id] = request;
        message::prepare_message(msg, message::EDGE_DELETE_REQ, request->clock1, request->req_id, request->elem1, request->elem2);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(request->client, msg.buf);
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
        request->clock1 = ++server->vc.clocks->at(me1->get_loc()-1);
        request->req_id = ++server->request_id;
        server->update_mutex.unlock();
        common::property prop(request->key, request->value, request->req_id);
        message::prepare_message(msg, message::EDGE_ADD_PROP, request->clock1, request->req_id, request->elem1, request->elem2, prop);
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(request->client, msg.buf);
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
        request->clock1 = ++server->vc.clocks->at(me1->get_loc()-1);
        request->req_id = ++server->request_id;
        server->pending[request->req_id] = request;
        message::prepare_message(msg, message::EDGE_DELETE_PROP, request->clock1, request->req_id, request->elem1, request->elem2, request->key);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(request->client, msg.buf);
}

void
delete_end(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request)
{
    server->update_mutex.lock();
    server->add_deleted_cache(request, *request->cached_req_ids);
    server->pending.erase(request->req_id);
    server->update_mutex.unlock();
    request->done = true;
}

void
write_graph(coordinator::central *server)
{
    std::ofstream file;
    file.open(GRAPH_FILE);
    for (auto &n: graph) {
        file << n.first;
        for (uint64_t nbr: n.second) {
            file << " " << nbr;
        }
        file << '\n';
    }
    file.close();
}

// caution: assuming we hold server->update_mutex
void
invalidate_migr_cache(coordinator::central *server, std::vector<uint64_t> &cached_ids)
{
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_start_coord(coordinator::central *server, message::message &msg, std::shared_ptr<coordinator::pending_req> request)
{
    node_prog::prog_type ignore;
    std::vector<std::pair<uint64_t, ParamsType>> initial_args;

    message::unpack_message(*request->req_msg, message::CLIENT_NODE_PROG_REQ, ignore, initial_args);
    
    // map from locations to a list of start_node_params to send to that shard
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> initial_batches; 
    server->update_mutex.lock();

    for (std::pair<uint64_t, ParamsType> &node_params_pair : initial_args) {
        if (check_elem(server, node_params_pair.first, true)) {
            std::cerr << "one of the arg nodes has been deleted, cannot perform request" << std::endl;
            /* TODO send back error msg */
            return;
        }
        common::meta_element *me = server->nodes.at(node_params_pair.first);
        initial_batches[me->get_loc()].emplace_back(std::make_tuple(node_params_pair.first,
            std::move(node_params_pair.second), db::element::remote_node())); // constructor
    }
    request->vector_clock.reset(new std::vector<uint64_t>(*server->vc.clocks));
    request->del_request = server->get_last_del_req(request);
    request->out_count = server->last_del;
    request->out_count->cnt++;
    request->req_id = ++server->request_id;
    server->pending.insert(std::make_pair(request->req_id, request));
    server->update_mutex.unlock();

    message::message msg_to_send;
    std::vector<uint64_t> empty_vector;
    std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> empty_tuple_vector;
    for (auto &batch_pair : initial_batches) {
        message::prepare_message(msg_to_send, message::NODE_PROG, request->pType, *request->vector_clock, 
                request->req_id, batch_pair.second, empty_vector, request->ignore_cache, empty_tuple_vector);
        server->send(batch_pair.first, msg_to_send.buf); // TODO later change to send without update mutex lock
    }
}

// caution: assuming we hold server->mutex
void end_node_prog(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request)
{
    bool done = true;
    uint64_t req_id = request->req_id;
    request->out_count->cnt--;
    for (uint64_t cached_id: *request->cached_req_ids) {
        if (!server->is_deleted_cache_id(cached_id)) {
            server->add_good_cache_id(cached_id);
        } else {
            // request was served based on cache value that should be
            // invalidated; restarting request
            done = false;
            request->ignore_cache.insert(cached_id);
            server->add_bad_cache_id(cached_id);
            request->del_request.reset();
            server->update_mutex.unlock();
            node_prog::programs.at(request->pType)->unpack_and_start_coord(server, *request->req_msg, request);
            break;
        }
    }
    server->pending.erase(req_id);
    if (done) {
        server->update_mutex.unlock();
        // send same message along to client
        server->send(request->client, request->reply_msg->buf);
        DEBUG << "Ending node prog for request " << req_id << std::endl;
        //message::message done_msg;
        //message::prepare_message(done_msg, message::DONE_NODE_PROG, req_id);
        //for (uint64_t i = 1; i <= NUM_SHARDS; i++) {
        //    server->send(i, done_msg.buf);
        //}
    }
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void
node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: unpack_and_run_db(db::graph *G, message::message &msg)
{
}

// process incoming message, either from shard or client
void
handle_msg(coordinator::central *server, std::unique_ptr<message::message> msg, enum message::msg_type m_type, uint64_t sender)
{
    uint64_t req_id, cached_req_id;
    std::shared_ptr<coordinator::pending_req>request;
    std::vector<uint64_t> vclock; // for reply
    std::unique_ptr<std::vector<uint64_t>> cached_req_ids; // for reply
    common::meta_element *lnode; // for migration
    uint64_t coord_handle; // for migration
    uint64_t new_loc, from_loc; // for migration
    node_prog::prog_type pType;
    
    auto crequest = std::make_shared<coordinator::pending_req>(m_type);
    crequest->client = sender - ID_INCR;

    switch(m_type) {

        // shard messages   
        case message::NODE_DELETE_ACK:
        case message::EDGE_DELETE_ACK:
        case message::EDGE_DELETE_PROP_ACK:
            cached_req_ids.reset(new std::vector<uint64_t>());
            message::unpack_message(*msg, m_type, req_id, *cached_req_ids);
            server->update_mutex.lock();
            request = server->pending.at(req_id);
            server->update_mutex.unlock();
            request->cached_req_ids = std::move(cached_req_ids);
            delete_end(server, request);
            break;
        
        case message::CACHE_UPDATE_ACK:
            server->update_mutex.lock();
            if ((++server->cache_acks) == NUM_SHARDS) {
                coord_daemon_end(server);
            } else {
                server->update_mutex.unlock();
            }
            break;

        case message::COORD_NODE_MIGRATE:
            cached_req_ids.reset(new std::vector<uint64_t>());
            message::unpack_message(*msg, message::COORD_NODE_MIGRATE, coord_handle, new_loc, *cached_req_ids);
            server->update_mutex.lock();
            lnode = server->nodes.at(coord_handle);
            from_loc = lnode->get_loc();
            lnode->update_loc(new_loc);
            message::prepare_message(*msg, message::COORD_NODE_MIGRATE_ACK,
                server->vc.clocks->at(from_loc-1), server->vc.clocks->at(new_loc-1), server->request_id);
            server->vc.clocks->at(new_loc-1)++;
            // invalidate cached ids
            for (uint64_t del_iter: *cached_req_ids) {
                server->bad_cache_ids->insert(del_iter);
            }
            record_time(server);
            server->shard_node_count[from_loc-1]--;
            server->shard_node_count[new_loc-1]++;
            server->update_mutex.unlock();
            server->send(from_loc, msg->buf);
            break;

        // response from a shard
        case message::NODE_PROG:
            cached_req_ids.reset(new std::vector<uint64_t>());
            message::unpack_message(*msg, message::NODE_PROG, pType, req_id, *cached_req_ids); // don't unpack rest
            server->update_mutex.lock();
            if (server->pending.count(req_id) == 0){
                // XXX anything else we need to do?
                server->update_mutex.unlock();
                std::cerr << "got response for request " << req_id << ", which does not exist anymore" << std::endl;
                return;
            }
            request = server->pending.at(req_id);
            request->cached_req_ids = std::move(cached_req_ids);
            request->reply_msg = std::move(msg);
            if (request->del_request) {
                if (request->del_request->done) {
                    end_node_prog(server, request);
                } else {
                    request->done = true;
                    server->update_mutex.unlock();
                }
            } else {
                end_node_prog(server, request);
            }
            break;
        

        // client messages
        case message::CLIENT_NODE_CREATE_REQ:
            create_node(server, crequest);
            break;

        case message::CLIENT_EDGE_CREATE_REQ: 
            message::unpack_message(*msg, message::CLIENT_EDGE_CREATE_REQ, crequest->elem1, crequest->elem2);
            create_edge(server, crequest);
            break;

        case message::CLIENT_NODE_DELETE_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_DELETE_REQ, crequest->elem1);
            delete_node_initiate(server, crequest);
            break;

        case message::CLIENT_EDGE_DELETE_REQ: 
            message::unpack_message(*msg, message::CLIENT_EDGE_DELETE_REQ, crequest->elem1, crequest->elem2);
            delete_edge_initiate(server, crequest);
            break;

        case message::CLIENT_ADD_EDGE_PROP:
            message::unpack_message(*msg, message::CLIENT_ADD_EDGE_PROP, crequest->elem1, crequest->elem2, crequest->key, crequest->value);
            add_edge_property(server, crequest);
            break;

        case message::CLIENT_DEL_EDGE_PROP:
            message::unpack_message(*msg, message::CLIENT_DEL_EDGE_PROP, crequest->elem1, crequest->elem2, crequest->key);
            delete_edge_property_initiate(server, crequest);
            break;

        case message::CLIENT_NODE_PROG_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_PROG_REQ, crequest->pType);
            crequest->req_msg = std::move(msg);
            node_prog::programs.at(crequest->pType)->unpack_and_start_coord(server, *crequest->req_msg, crequest);
            break;

        case message::CLIENT_COMMIT_GRAPH:
            write_graph(server);
            break;

        case message::EXIT_WEAVER:
            exit_weaver();

        default:
            std::cerr << "unexpected msg type " << m_type << std::endl;
    }
}

// handle incoming messages
void
msg_handler(coordinator::central *server)
{
    busybee_returncode ret;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    uint64_t sender;
    std::unique_ptr<coordinator::thread::unstarted_thread> thr;

    while (1)
    {
        rec_msg.reset(new message::message());
        if ((ret = server->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        thr.reset(new coordinator::thread::unstarted_thread(handle_msg, server, std::move(rec_msg), mtype, sender));
        server->thread_pool.add_request(std::move(thr), true);
    }
}

// periodically update cache at all shards
void
coord_daemon_initiate(coordinator::central *server)
{
    std::vector<uint64_t> good, bad;
    uint64_t perm_del_id = 0;
    uint64_t migr_del_id = 0;
    message::message msg;
    std::chrono::seconds duration(DAEMON_PERIOD); // execute every DAEMON_PERIOD seconds
    std::this_thread::sleep_for(duration);
    server->update_mutex.lock();
    // figure out minimum outstanding request id
    for (auto &r: server->pending) {
        if ((migr_del_id == 0) || (r.first < migr_del_id)) {
            migr_del_id = r.first;
        }
    }
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
    // send out messages
    for (uint64_t i = 1; i <= NUM_SHARDS; i++) {
        message::prepare_message(msg, message::CACHE_UPDATE, good, bad, perm_del_id, migr_del_id);
        server->send(i, msg.buf);
    }
}

// caution: assuming we hold server->update_mutex
void
coord_daemon_end(coordinator::central *server)
{
    assert(server->cache_acks == NUM_SHARDS);
    server->cache_acks = 0; 
    server->update_mutex.unlock();
    coord_daemon_initiate(server);
}

void
end_program(int param)
{
    std::cerr << "Ending program, param = " << param << std::endl;
    exit_weaver();
}

int
main()
{
    coordinator::central server;
    std::thread *t;
    signal(SIGINT, end_program);

    std::cout << "Weaver: coordinator" << std::endl;
    cserver = &server;
    record_time(&server);

    // call periodic cache update function
    t = new std::thread(coord_daemon_initiate, &server);
    t->detach();

    // initialize client msg receiving thread
    msg_handler(&server);
}
