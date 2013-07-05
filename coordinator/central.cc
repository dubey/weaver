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

#include "common/weaver_constants.h"
#include "common/clock.h"
#include "central.h"
#include "common/meta_element.h"
#include "common/message.h"
#include "common/debug.h"
#include "node_prog/node_program.h"
#include "node_prog/node_prog_type.h"
#include "db/element/remote_node.h"

static std::unordered_map<uint64_t, std::vector<uint64_t>> graph;
static coordinator::central *server;
void coord_daemon_end();
void coord_daemon_initiate();

void
record_time()
{
    double mtime;
    wclock::get_clock(&server->mtime);
    mtime = server->mtime.tv_sec + ((double)server->mtime.tv_nsec)/(1000000000ULL);
    server->migr_times.emplace_back(mtime);
}

void start_migration()
{
    message::message msg;
    message::prepare_message(msg, message::MIGRATION_TOKEN);
    server->send(START_MIGR_ID, msg.buf);
}

void exit_weaver()
{
    message::message msg;
    for (uint64_t s = 1; s <= NUM_SHARDS; s++) {
        message::prepare_message(msg, message::EXIT_WEAVER);
        server->send(s, msg.buf);
    }
    // record dummy time for a better-looking migration plot
    record_time();
    std::ofstream migr_file, shard_file;
    //migr_file.open("migrations.rec");
    //migr_file.precision(std::numeric_limits<double>::digits10);
    //DEBUG << "starting migr rec loop\n";
    //for (uint64_t j = 0; j < cserver->migr_times.size(); j++) {
    //    migr_file << cserver->migr_times.at(j) << ",";
    //    DEBUG << cserver->migr_times.at(j) << ",";
    //    std::vector<uint64_t> &migr_locs = cserver->migr_loc_count.at(j);
    //    for (int i = 0; i < NUM_SHARDS; i++) {
    //        if (i == (NUM_SHARDS-1)) {
    //            migr_file << migr_locs.at(i) << std::endl;
    //            DEBUG << migr_locs.at(i) << std::endl;
    //        } else {
    //            migr_file << migr_locs.at(i) << ",";
    //            DEBUG << migr_locs.at(i) << std::endl;
    //        }
    //    }
    //}
    //migr_file.close();
    shard_file.open("shard_counts.rec");
    for (int i = 0; i < NUM_SHARDS; i++) {
        shard_file << server->shard_node_count[i] << std::endl;
    }
    shard_file.close();
    exit(0);
}

// create a node
void
create_node(std::shared_ptr<coordinator::pending_req>request)
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
create_edge(std::shared_ptr<coordinator::pending_req>request)
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
    message::prepare_message(msg, message::EDGE_CREATE_REQ,
        clock1, req_id, request->elem1, request->elem2, loc2, clock2);
    server->send(loc1, msg.buf);
    server->add_edge(new common::meta_element(loc1), req_id);
    graph.at(request->elem1).emplace_back(request->elem2);
    message::prepare_message(msg, message::CLIENT_REPLY, req_id);
    server->send(request->client, msg.buf);
}

// delete a node
void
delete_node_initiate(std::shared_ptr<coordinator::pending_req>request)
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
        message::prepare_message(msg, message::NODE_DELETE_REQ,
            request->clock1, request->req_id, request->elem1);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(request->client, msg.buf);
}

// delete an edge
void
delete_edge_initiate(std::shared_ptr<coordinator::pending_req>request)
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
        message::prepare_message(msg, message::EDGE_DELETE_REQ,
            request->clock1, request->req_id, request->elem1, request->elem2);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(request->client, msg.buf);
}

// add a property, i.e. a key-value pair to an edge
void 
add_edge_property(std::shared_ptr<coordinator::pending_req>request)
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
        message::prepare_message(msg, message::EDGE_ADD_PROP,
            request->clock1, request->req_id, request->elem1, request->elem2, prop);
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(request->client, msg.buf);
}

void
delete_edge_property_initiate(std::shared_ptr<coordinator::pending_req>request)
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
        message::prepare_message(msg, message::EDGE_DELETE_PROP,
            request->clock1, request->req_id, request->elem1, request->elem2, request->key);
        server->add_pending_del_req(request);
        server->update_mutex.unlock();
        server->send(me1->get_loc(), msg.buf);
    }
    message::prepare_message(msg, message::CLIENT_REPLY);
    server->send(request->client, msg.buf);
}

void
delete_end(std::shared_ptr<coordinator::pending_req> request)
{
    server->update_mutex.lock();
    server->add_deleted_cache(request, *request->cached_req_ids);
    server->pending.erase(request->req_id);
    request->done = true;
    server->update_mutex.unlock();
}

void
write_graph()
{
    std::ofstream file;
    file.open(GRAPH_FILE);
    for (auto &n: graph) {
        file << n.first;
        for (uint64_t nbr: n.second) {
            file << " " << nbr;
        }
        file << std::endl;
    }
    file.close();
}

// caution: need to hold server->update_mutex throughout
template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_start_coord(std::shared_ptr<coordinator::pending_req> request)
{
    node_prog::prog_type ignore;
    std::vector<std::pair<uint64_t, ParamsType>> initial_args;

    message::unpack_message(*request->req_msg, message::CLIENT_NODE_PROG_REQ, ignore, initial_args);
    
    // map from locations to a list of start_node_params to send to that shard
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> initial_batches; 

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
    server->pending.emplace(std::make_pair(request->req_id, request));

    // TODO later change to send without update mutex
    message::message msg_to_send;
    std::vector<uint64_t> empty_vector;
    std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> empty_tuple_vector;
    DEBUG << "starting node prog " << request->req_id << ", recd from client\t";
    for (auto &batch_pair : initial_batches) {
        message::prepare_message(msg_to_send, message::NODE_PROG, request->pType, *request->vector_clock, 
                request->req_id, batch_pair.second, empty_vector, request->ignore_cache, empty_tuple_vector);
        server->send(batch_pair.first, msg_to_send.buf);
    }
    DEBUG << "sent to shards" << std::endl;
}

// caution: need to hold server->update_mutex throughout
void end_node_prog(std::shared_ptr<coordinator::pending_req> request)
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
            request->ignore_cache.emplace(cached_id);
            server->add_bad_cache_id(cached_id);
            request->del_request.reset();
            node_prog::programs.at(request->pType)->unpack_and_start_coord(request);
            break;
        }
    }
    server->pending.erase(req_id);
    if (done) {
        server->completed_requests->emplace_back(std::make_pair(req_id, request->pType));
        // send same message along to client
        DEBUG << "going to send msg to end node prog\t";
        server->send(request->client, request->reply_msg->buf);
        DEBUG << "ended node prog " << req_id << std::endl;
    }
}

inline void
get_node_loc(std::unique_ptr<message::message> msg, uint64_t sender)
{
    uint64_t node_handle, loc;
    message::unpack_message(*msg, message::CLIENT_NODE_LOC_REQ, node_handle);
    if (check_elem(server, node_handle, true)) {
        std::cerr << "node(s) not found or deleted, cannot return loc" << std::endl;
        loc = -1;
    } else {
        server->update_mutex.lock();
        loc = server->nodes.at(node_handle)->get_loc();
        server->update_mutex.unlock();
    }
    message::prepare_message(*msg, message::CLIENT_NODE_LOC_REPLY, loc);
    server->send(sender, msg->buf);
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> ::
    unpack_and_run_db(message::message&)
{
}

// process incoming message, either from shard or client
void
handle_msg(std::unique_ptr<message::message> msg, enum message::msg_type m_type, uint64_t sender)
{
    uint64_t req_id;
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
            delete_end(request);
            break;
        
        case message::CLEAN_UP_ACK:
            coord_daemon_end();
            break;

        case message::COORD_NODE_MIGRATE: {
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
                server->bad_cache_ids->emplace(del_iter);
            }
            record_time();
            server->shard_node_count[from_loc-1]--;
            server->shard_node_count[new_loc-1]++;
            std::vector<uint64_t> cur_counts;
            for (auto cnt: server->shard_node_count) {
                cur_counts.emplace_back(cnt);
            }
            server->migr_loc_count.emplace_back(cur_counts);
            server->update_mutex.unlock();
            server->send(from_loc, msg->buf);
            break;
        }

        case message::COORD_CLOCK_REQ:
            message::unpack_message(*msg, message::COORD_CLOCK_REQ, from_loc);
            server->update_mutex.lock();
            message::prepare_message(*msg, message::COORD_CLOCK_REPLY, server->request_id);
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
                    end_node_prog(request);
                } else {
                    request->done = true;
                }
            } else {
                end_node_prog(request);
            }
            server->update_mutex.unlock();
            break;
        

        // client messages
        case message::CLIENT_NODE_CREATE_REQ:
            create_node(crequest);
            break;

        case message::CLIENT_EDGE_CREATE_REQ: 
            message::unpack_message(*msg, message::CLIENT_EDGE_CREATE_REQ, crequest->elem1, crequest->elem2);
            create_edge(crequest);
            break;

        case message::CLIENT_NODE_DELETE_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_DELETE_REQ, crequest->elem1);
            delete_node_initiate(crequest);
            break;

        case message::CLIENT_EDGE_DELETE_REQ: 
            message::unpack_message(*msg, message::CLIENT_EDGE_DELETE_REQ, crequest->elem1, crequest->elem2);
            delete_edge_initiate(crequest);
            break;

        case message::CLIENT_ADD_EDGE_PROP:
            message::unpack_message(*msg, message::CLIENT_ADD_EDGE_PROP,
                crequest->elem1, crequest->elem2, crequest->key, crequest->value);
            add_edge_property(crequest);
            break;

        case message::CLIENT_DEL_EDGE_PROP:
            message::unpack_message(*msg, message::CLIENT_DEL_EDGE_PROP,
                crequest->elem1, crequest->elem2, crequest->key);
            delete_edge_property_initiate(crequest);
            break;

        case message::CLIENT_NODE_PROG_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_PROG_REQ, crequest->pType);
            crequest->req_msg = std::move(msg);
            server->update_mutex.lock();
            node_prog::programs.at(crequest->pType)->unpack_and_start_coord(crequest);
            server->update_mutex.unlock();
            break;

        case message::CLIENT_COMMIT_GRAPH:
            write_graph();
            break;
        
        case message::CLIENT_NODE_LOC_REQ:
            get_node_loc(std::move(msg), crequest->client);
            break;

        case message::START_MIGR:
            start_migration();
            break;

        case message::EXIT_WEAVER:
            exit_weaver();
            break;

        default:
            std::cerr << "unexpected msg type " << m_type << std::endl;
    }
    
    // check if coordinator daemon needs to be initiated
    bool to_start = false;
    server->daemon_mutex.lock();
    if (server->init_daemon) {
        timespec cur;
        wclock::get_clock(&cur);
        double diff = wclock::diff(server->daemon_time, cur);
        if (diff > DAEMON_PERIOD) {
            server->init_daemon = false;
            to_start = true;
        }
    }
    server->daemon_mutex.unlock();
    if (to_start) {
        coord_daemon_initiate();
    }
}

// handle incoming messages
void
msg_handler()
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
        thr.reset(new coordinator::thread::unstarted_thread(handle_msg, std::move(rec_msg), mtype, sender));
        server->thread_pool.add_request(std::move(thr));
    }
}

// periodically update cache at all shards, permanently delete migrated/deleted nodes,
// and delete state corresponding to completed node programs
void
coord_daemon_initiate()
{
    //std::vector<uint64_t> good, bad;
    uint64_t perm_del_id = 0;
    uint64_t migr_del_id = 0;
    message::message msg;
    server->update_mutex.lock();
    auto completed_requests = std::move(server->completed_requests);
    server->completed_requests.reset(new std::vector<std::pair<uint64_t, node_prog::prog_type>>());
    // figure out minimum outstanding request id
    for (auto &r: server->pending) {
        if ((migr_del_id == 0) || (r.first < migr_del_id)) {
            migr_del_id = r.first;
        }
    }
    if (migr_del_id == 0) {
        migr_del_id = server->request_id;
    }
    while (server->first_del->cnt == 0 && server->first_del != server->last_del) {
        perm_del_id = server->first_del->req_id;
        if (server->first_del->next) {
            server->first_del = server->first_del->next;
        } else {
            break;
        }
    }
    // send out messages
    for (uint64_t i = 1; i <= NUM_SHARDS; i++) {
        message::prepare_message(msg, message::CLEAN_UP, *server->good_cache_ids, *server->bad_cache_ids, 
            perm_del_id, migr_del_id, *completed_requests);
        server->send(i, msg.buf);
    }
    if (server->good_cache_ids->size() != 0) {
        server->good_cache_ids.reset(new std::unordered_set<uint64_t>());
    }
    if (server->bad_cache_ids->size() != 0) {
        for (uint64_t bad_id: *server->bad_cache_ids) {
            server->transient_bad_cache_ids->emplace(bad_id);
        }
        server->bad_cache_ids.reset(new std::unordered_set<uint64_t>());
    }
    server->update_mutex.unlock();
}

// caution: assuming we hold server->update_mutex
void
coord_daemon_end()
{
    server->daemon_mutex.lock();
    if (++server->cache_acks == NUM_SHARDS) {
        server->init_daemon = true;
        server->cache_acks = 0;
        wclock::get_clock(&server->daemon_time);
    }
    server->daemon_mutex.unlock();
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
    server = new coordinator::central();
    signal(SIGINT, end_program);

    std::cout << "Weaver: coordinator" << std::endl;
    record_time();

    // initialize client msg receiving thread
    msg_handler();
}
