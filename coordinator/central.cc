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
#include "common/message/message.h"
#include "threadpool/threadpool.h"

#define NUM_NODES 100000
#define NUM_EDGES 150000
#define NUM_REQUESTS 100
#define NUM_THREADS 4

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

std::unordered_map<size_t, pending_req *> pending;
size_t request_id;

// wake up thread waiting on the received message
void
handle_pending_req(coordinator::central *server, std::unique_ptr<message::message> msg,
    enum message::msg_type m_type)
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
    req_id = ++request_id;
    pending[req_id] = request;
    msg.prep_node_create(req_id, creat_time);
    server->send(shard_loc_ptr, msg.buf);
    server->update_mutex.unlock();
    
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
    req_id = ++request_id;
    pending[req_id] = request;
    msg.prep_edge_create(req_id, (size_t)node1->get_addr(), (size_t)node2->get_addr(),
        node2->get_loc_ptr(), node2->get_creat_time(), creat_time);
    server->send(node1->get_loc_ptr(), msg.buf);
    server->update_mutex.unlock();
    
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
    req_id = ++request_id;
    pending[req_id] = request;
    msg.prep_node_delete(req_id, (size_t)node->get_addr(), del_time);
    server->send(node->get_loc_ptr(), msg.buf);
    server->update_mutex.unlock();

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
    req_id = ++request_id;
    pending[req_id] = request;
    msg.prep_edge_delete(req_id, (size_t)node->get_addr(), (size_t)edge->get_addr(), del_time);
    server->send(node->get_loc_ptr(), msg.buf);
    server->update_mutex.unlock();
    
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
void
reachability_request(common::meta_element *node1, common::meta_element *node2, coordinator::central *server)
{
    pending_req *request;
    message::message msg(message::REACHABLE_PROP);
    std::vector<size_t> src; // vector to hold src node
    src.push_back((size_t)node1->get_addr());
    size_t req_id;
    
    server->update_mutex.lock();
    if (node1->get_del_time() < MAX_TIME || node2->get_del_time() < MAX_TIME)
    {
        std::cerr << "one of the nodes has been deleted, cannot perform request"
            << std::endl;
        server->update_mutex.unlock();
        return;
    }
    request = new pending_req();
    req_id = ++request_id;
    pending[req_id] = request;
    std::cout << "Reachability request number " << req_id << " from source"
              << " node " << node1->get_addr() << " " << node1->get_loc_ptr()->port << " to destination node "
              << node2->get_addr()<< " " << node2->get_loc_ptr()->port << std::endl;
    request->mutex.lock();
    msg.prep_reachable_prop(&src, server->myrecloc, (size_t)node2->get_addr(),
        node2->get_loc_ptr(), req_id, req_id, server->vc.clocks);
    server->send(node1->get_loc_ptr(), msg.buf);
    server->update_mutex.unlock();
    
    while (request->waiting)
    {
        request->reply.wait();
    }
    std::cout << "Reachable reply is " << request->reachable << " for " << 
        "request " << req_id << std::endl;
    request->mutex.unlock();
    delete request;
    server->update_mutex.lock();
    pending.erase(req_id);
    server->update_mutex.unlock();
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

void
msg_handler(coordinator::central *server)
{
    busybee_returncode ret;
    po6::net::location sender(COORD_IPADDR, COORD_PORT);
    message::message msg(message::ERROR);
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    coordinator::thread::pool thread_pool(NUM_THREADS);
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
            server, std::move(rec_msg), mtype));
        thread_pool.add_request(std::move(thr), !(mtype == message::REACHABLE_REPLY));
    }
}

int
main(int argc, char* argv[])
{
    coordinator::central server;
    void *mem_addr1, *mem_addr2, *mem_addr3, *mem_addr4;
    int i;
    std::vector<void *> nodes, edges;
    timespec start, end, time_taken;
    uint32_t time_ms;
    std::thread *t;
    
    request_id = 0;
    // initialize array of shard server locations
    for (i = 1; i <= NUM_SHARDS; i++)
    {
        auto new_shard = std::make_shared<po6::net::location>(SHARD_IPADDR, COORD_PORT + i);
        server.shards.push_back(new_shard);
    }

    // initialize msg receiving thread
    t = new std::thread(msg_handler, &server);
    t->detach();

    // build the graph
    for (i = 0; i < NUM_NODES; i++)
    {
        mem_addr1 = create_node(&server);
        nodes.push_back(mem_addr1);
    }
    srand(time(NULL));
    for (i = 0; i < NUM_EDGES; i++)
    {
        int first = rand() % NUM_NODES;
        int second = rand() % NUM_NODES;
        while (second == first) //no self-loop edges
        {
             second = rand() % NUM_NODES;
        }
        create_edge((common::meta_element *)nodes[first], 
            (common::meta_element *)nodes[second], &server);
    }
    
    /*
    for (i = 0; i < NUM_NODES; i++)
    {
        //ring graph
        edges.push_back(create_edge((common::meta_element *)nodes[i],
            (common::meta_element *)nodes[(i+1) % NUM_NODES], &server));
    }
    for (i = 0; i < 10; i++)
    {
        delete_node((common::meta_element *)nodes[i*i], &server);
    }
    for (i = 900; i < NUM_NODES - 4; i++)
    {
        delete_edge((common::meta_element *)nodes[i], (common::meta_element *)edges[i], &server);
    }
    */
    
    clock_gettime(CLOCK_MONOTONIC, &start);    
    /*
    for (i = 0; i < NUM_NODES; i++)
    {
        reachability_request((common::meta_element *)nodes[i],
        (common::meta_element *)nodes[(i+1)%NUM_NODES], &server);
    }
    */
    for (i = 0; i < NUM_REQUESTS; i++)
    {
        int first = rand() % NUM_NODES;
        int second = rand() % NUM_NODES;
        while (second == first) //no self-loop edges
        {
             second = rand() % NUM_NODES;
        }
        reachability_request((common::meta_element *)nodes[first],
            (common::meta_element *)nodes[second], &server);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);  

    time_taken = diff(start, end);
    time_ms = time_taken.tv_sec * 1000 + time_taken.tv_nsec/1000000;
    std::cout << "Time = " << time_ms << std::endl;

    std::cin >> i;

} //end main
