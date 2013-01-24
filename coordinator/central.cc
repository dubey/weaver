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
#include <unordered_map>
#include "e/buffer.h"
#include "busybee_constants.h"

#include "central.h"
#include "common/meta_element.h"
#include "common/message.h"

// used to wait on pending update/reachability requests
class pending_req
{
    public:
        void *addr;
        bool reachable;
        po6::threads::mutex mutex;
        bool waiting;
        po6::threads::cond reply;
        
    pending_req()
        : addr(NULL)
        , waiting(true)
        , reply(&mutex)
    {
    }
};

static std::unordered_map<size_t, pending_req *> pending;

// create a node
void*
create_node(coordinator::central *server)
{
    pending_req *request; 
    common::meta_element *new_node;
    std::shared_ptr<po6::net::location> shard_loc_ptr;
    uint64_t creat_time;
    void *node_addr; // node handle on shard server
    message::message msg(message::NODE_CREATE_REQ);
    server->port_ctr = (server->port_ctr + 1) % NUM_SHARDS;
    shard_loc_ptr = server->shards[server->port_ctr]; // node will be placed on this shard server
    size_t req_id;

    server->update_mutex.lock();
    creat_time = ++server->vc.clocks->at(server->port_ctr); // incrementing vector clock
    request = new pending_req();
    request->mutex.lock();
    req_id = ++server->request_id;
    pending[req_id] = request;
    msg.prep_node_create(req_id, creat_time);
    server->update_mutex.unlock();
    server->send(shard_loc_ptr, msg.buf);
    
    // waiting for reply from shard
    while (request->waiting)
    {
        request->reply.wait();
    }
    new_node = new common::meta_element(shard_loc_ptr, creat_time, MAX_TIME, request->addr);
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    pending.erase(req_id);
    server->update_mutex.unlock();
    server->add_node(new_node);
    //std::cout << "Node id is " << (void *)new_node << " " <<
    //    new_node->get_addr() << std::endl;
    return (void *)new_node;
}

// create an edge
void*
create_edge(common::meta_element *node1, common::meta_element *node2, coordinator::central *server)
{
    pending_req *request;
    common::meta_element *new_edge;
    uint64_t creat_time;
    void *edge_addr;
    message::message msg(message::EDGE_CREATE_REQ);
    size_t req_id;

    //TODO need checks for given node_handles
    server->update_mutex.lock();
    creat_time = ++server->vc.clocks->at(node1->get_shard_id());
    request = new pending_req();
    request->mutex.lock();
    req_id = ++server->request_id;
    pending[req_id] = request;
    msg.prep_edge_create(req_id, (size_t)node1->get_addr(), (size_t)node2->get_addr(),
        node2->get_loc_ptr(), node2->get_creat_time(), creat_time);
    server->update_mutex.unlock();
    server->send(node1->get_loc_ptr(), msg.buf);
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    new_edge = new common::meta_element(node1->get_loc_ptr(), creat_time, MAX_TIME, request->addr);
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    pending.erase(req_id);
    server->update_mutex.unlock();
    server->add_edge(new_edge);
    //std::cout << "Edge id is " << (void *)new_edge << " " <<
    //    new_edge->get_addr() << std::endl;
    return (void *)new_edge;
}

// delete a node
void
delete_node(common::meta_element *node, coordinator::central *server)
{
    pending_req *request;
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
    del_time = ++server->vc.clocks->at(node->get_shard_id());
    node->update_del_time(del_time);
    request = new pending_req();
    request->mutex.lock();
    req_id = ++server->request_id;
    pending[req_id] = request;
    msg.prep_node_delete(req_id, (size_t)node->get_addr(), del_time);
    server->update_mutex.unlock();
    server->flaky_send(node->get_loc_ptr(), msg.buf, true);

    // waiting for reply from shard
    while (request->waiting)
    {
        request->reply.wait();
    }
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    pending.erase(req_id);
    server->update_mutex.unlock();
}

// delete an edge
void
delete_edge(common::meta_element *node, common::meta_element *edge, coordinator::central *server)
{
    pending_req *request;
    uint64_t del_time;
    message::message msg(message::EDGE_DELETE_REQ);
    size_t req_id;

    server->update_mutex.lock();
    if (edge->get_del_time() < MAX_TIME)
    {
        std::cerr << "cannot delete edge twice" << std::endl;
        server->update_mutex.unlock();
        return;
    }
    del_time = ++server->vc.clocks->at(node->get_shard_id());
    edge->update_del_time(del_time);
    request = new pending_req();
    request->mutex.lock();
    req_id = ++server->request_id;
    pending[req_id] = request;
    msg.prep_edge_delete(req_id, (size_t)node->get_addr(), (size_t)edge->get_addr(), del_time);
    server->update_mutex.unlock();
    server->flaky_send(node->get_loc_ptr(), msg.buf, true);
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    pending.erase(req_id);
    server->update_mutex.unlock();
}

// is node1 reachable from node2?
bool
reachability_request(common::meta_element *node1, common::meta_element *node2, coordinator::central *server)
{
    pending_req *request;
    message::message msg(message::REACHABLE_PROP);
    std::vector<size_t> src; // vector to hold src node
    src.push_back((size_t)node1->get_addr());
    size_t req_id;
    bool ret;
    
    server->update_mutex.lock();
    if (node1->get_del_time() < MAX_TIME || node2->get_del_time() < MAX_TIME)
    {
        std::cerr << "one of the nodes has been deleted, cannot perform request"
            << std::endl;
        server->update_mutex.unlock();
        return false;
    }
    request = new pending_req();
    req_id = ++server->request_id;
    pending[req_id] = request;
    std::cout << "Reachability request number " << req_id << " from source"
              << " node " << node1->get_addr() << " " << node1->get_loc_ptr()->port << " to destination node "
              << node2->get_addr()<< " " << node2->get_loc_ptr()->port << std::endl;
    request->mutex.lock();
    msg.prep_reachable_prop(&src, server->myrecloc, (size_t)node2->get_addr(),
        node2->get_loc_ptr(), req_id, req_id, server->vc.clocks);
    server->update_mutex.unlock();
    server->send(node1->get_loc_ptr(), msg.buf);
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    ret = request->reachable;
    std::cout << "Reachable reply is " << ret << " for " << "request " << req_id << std::endl;
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    pending.erase(req_id);
    server->update_mutex.unlock();
    return ret;
}

timespec
diff(timespec start, timespec end)
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

// wake up thread waiting on the received message
void
handle_pending_req(coordinator::central *server, std::unique_ptr<message::message> msg,
    enum message::msg_type m_type, std::unique_ptr<po6::net::location> dummy)
{
    size_t req_id;
    pending_req *request;
    void *mem_addr;
    uint32_t rec_counter; // for reply
    bool is_reachable; // for reply
    size_t src_node; //for reply
    size_t num_del_nodes; //for reply
    std::unique_ptr<std::vector<size_t>> del_nodes(new std::vector<size_t>()); //for reply
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>()); //for reply
    
    switch(m_type)
    {
        case message::NODE_CREATE_ACK:
        case message::EDGE_CREATE_ACK:
            msg->unpack_create_ack(&req_id, &mem_addr);
            server->update_mutex.lock();
            request = pending[req_id];
            server->update_mutex.unlock();
            request->mutex.lock();
            request->addr = mem_addr;
            request->waiting = false;
            request->reply.signal();
            request->mutex.unlock();
            break;

        case message::NODE_DELETE_ACK:
        case message::EDGE_DELETE_ACK:
            msg->unpack_delete_ack(&req_id);
            server->update_mutex.lock();
            request = pending[req_id];
            server->update_mutex.unlock();
            request->mutex.lock();
            request->waiting = false;
            request->reply.signal();
            request->mutex.unlock();
            break;

        case message::REACHABLE_REPLY:
            msg->unpack_reachable_rep(&req_id, &is_reachable, &src_node,
                &num_del_nodes, &del_nodes, &del_times);
            server->update_mutex.lock();
            request = pending[req_id];
            server->update_mutex.unlock();
            request->mutex.lock();
            request->reachable = is_reachable;
            request->waiting = false;
            request->reply.signal();
            request->mutex.unlock();
            break;
        
        default:
            std::cerr << "unexpected msg type " << m_type << std::endl;
    }
}

void
msg_handler(coordinator::central *server)
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
        server->thread_pool.add_request(std::move(thr), true);// XXX!(mtype == message::REACHABLE_REPLY));
    }
}

// call appropriate function based on msg from client
void
handle_client_req(coordinator::central *server, std::unique_ptr<message::message> msg,
    enum message::msg_type m_type, std::unique_ptr<po6::net::location> client_loc)
{
    uint16_t client_port;
    size_t elem1, elem2;
    size_t new_elem;
    bool reachable;
    switch (m_type)
    {
        case message::CLIENT_NODE_CREATE_REQ:
            msg->unpack_client0(&client_port);
            client_loc.reset(new po6::net::location(client_loc->address, client_port));
            new_elem = (size_t)create_node(server);
            msg->change_type(message::CLIENT_REPLY);
            msg->prep_client1(0, new_elem);
            server->client_send(*client_loc, msg->buf);
            break;

        case message::CLIENT_EDGE_CREATE_REQ: 
            msg->unpack_client2(&client_port, &elem1, &elem2);
            client_loc.reset(new po6::net::location(client_loc->address, client_port));
            //client_loc->port = client_port;
            new_elem = (size_t)create_edge((common::meta_element *)elem1, (common::meta_element *)elem2, server);
            msg->change_type(message::CLIENT_REPLY);
            msg->prep_client1(0, new_elem);
            server->client_send(*client_loc, msg->buf);
            break;

        case message::CLIENT_NODE_DELETE_REQ:
            msg->unpack_client1(&client_port, &elem1);
            delete_node((common::meta_element *)elem1, server);
            break;

        case message::CLIENT_EDGE_DELETE_REQ: 
            msg->unpack_client2(&client_port, &elem1, &elem2);
            delete_edge((common::meta_element *)elem1, (common::meta_element *)elem2, server);
            break;

        case message::CLIENT_REACHABLE_REQ: 
            msg->unpack_client2(&client_port, &elem1, &elem2);
            client_loc.reset(new po6::net::location(client_loc->address, client_port));
            //client_loc->port = client_port;
            reachable = reachability_request((common::meta_element *)elem1, (common::meta_element *)elem2, server);
            msg->change_type(message::CLIENT_REPLY);
            msg->prep_client_rr_reply(reachable);
            server->client_send(*client_loc, msg->buf);
            break;
    }
}

// handle messages from client
void
client_handler(coordinator::central *server)
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
        server->thread_pool.add_request(std::move(thr), false); // XXX
    }
}

int
main(int argc, char* argv[])
{
    coordinator::central server;
    std::thread *t;
    
    std::cout << "Weaver: coordinator" << std::endl;
    // initialize shard msg receiving thread
    t = new std::thread(msg_handler, &server);
    t->detach();

    //initialize client handler thread
    client_handler(&server);
} 
