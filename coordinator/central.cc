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
#include "common/message/message.h"
#include "threadpool/threadpool.h"

#define NUM_NODES 1000
#define NUM_EDGES 1500
#define NUM_REQUESTS 100
#define NUM_THREADS 4

// XXX what is this?
class pending_req
{
    public:
        int type; //1:edge, 2:node, 3:reach
        void *mem_addr1;
        void *mem_addr2;
        coordinator::graph_elem *elem1;
        coordinator::graph_elem *elem2;
        po6::net::location loc1;
        po6::net::location loc2;
        int num_edge;
};

std::unordered_map<uint32_t, pending_req> pending;
po6::threads::mutex pending_mutex;

// XXX what does this do?
void
handle_pending_req(coordinator::central *server,
    std::shared_ptr<message::message> msg,
    enum message::msg_type m_type)
{
    uint32_t req_num;
    void *mem_addr;
    coordinator::graph_elem *elem;
    bool is_reachable;
    size_t src_node;
    std::unique_ptr<po6::net::location> loc;
    switch(m_type)
    {
    case message::NODE_CREATE_ACK:
        /*
        if (msg->unpack_create_ack (&req_num, &mem_addr) != 0)
        {
            std::cerr << "invalid msg in NODE_CREATE_ACK" << std::endl;
        }
        elem = new coordinator::graph_elem (pending[req_num].loc1,
                                                    pending[req_num].loc2,
                                                    mem_addr,
                                                    mem_addr);
        server->elements.push_back (elem);
        pending.erase (req_num);
        */
        break;

    case message::EDGE_CREATE_ACK:
        break;

    case message::REACHABLE_REPLY:
        /*
        msg->unpack_reachable_rep (&req_num, &is_reachable, &src_node);
        std::cout << "Reachable reply is " << is_reachable << " for "
                  << "request " << req_num << std::endl;
        */
        break;
    
    default:
        std::cerr << "unexpected msg type " << m_type << std::endl;
    }
}

void*
create_edge(void *mem_addr1, void *mem_addr2, coordinator::central *server)
{
    coordinator::graph_elem *elem; // new edge
    coordinator::graph_elem *elem1 = (coordinator::graph_elem *)mem_addr1; //from node
    coordinator::graph_elem *elem2 = (coordinator::graph_elem *)mem_addr2; //to node
    po6::net::location loc1(elem1->loc1); //from node's location
    po6::net::location loc2(elem2->loc2); //to node's location
    std::unique_ptr<po6::net::location> loc_ptr; //location pointer reused for msg packing
    enum message::edge_direction dir = message::FIRST_TO_SECOND; //edge direction
    uint64_t creat_time1, creat_time2;
    message::message msg(message::EDGE_CREATE_REQ);
    busybee_returncode ret;
    creat_time1 = ++server->vc.clocks[elem1->loc1.port - COORD_PORT - 1];
    creat_time2 = ++server->vc.clocks[elem2->loc1.port - COORD_PORT - 1];

    loc_ptr.reset(new po6::net::location(elem2->loc1));
    msg.prep_edge_create((size_t)elem1->mem_addr1, (size_t)elem2->mem_addr1,
        std::move(loc_ptr), elem1->creat_time1, elem2->creat_time1, dir, creat_time1);
    if ((ret = server->bb.send(loc1, msg.buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg send error: " << ret << std::endl;
        return NULL;
    }
    if ((ret = server->bb.recv(&loc1, &msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return NULL;
    }
    msg.unpack_create_ack(&mem_addr1);

    dir = message::SECOND_TO_FIRST;
    loc_ptr.reset(new po6::net::location(elem1->loc1));
    msg.prep_edge_create((size_t)elem2->mem_addr1, (size_t)elem1->mem_addr1,
        std::move(loc_ptr), elem2->creat_time1, elem1->creat_time1, dir, creat_time2);
    if ((ret = server->bb.send(loc2, msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg send error: " << ret << std::endl;
        return NULL;
    }
    if ((ret = server->bb.recv(&loc2, &msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return NULL;
    }
    msg.unpack_create_ack(&mem_addr2);

    elem = new coordinator::graph_elem(elem1->loc1, elem2->loc1, 
       mem_addr1, mem_addr2, creat_time1, creat_time2);
    server->elements.push_back(elem);
            
    //std::cout << "Edge id is " << (void *) elem << std::endl;
    return (void *)elem;
}

void*
create_node(coordinator::central *server)
{
    busybee_returncode ret;
    coordinator::graph_elem *elem; // for new node
    void *node_addr; // node handle on shard server
    message::message msg(message::NODE_CREATE_REQ);
    std::unique_ptr<po6::net::location> shard_loc_ptr;
    uint64_t creat_time;
    server->port_ctr = (server->port_ctr + 1) % NUM_SHARDS;
    /*
    if (server->port_ctr == 0) 
    {
        server->port_ctr = COORD_PORT+1;
    }
    */
    po6::net::location shard_loc(COORD_IPADDR, 1 + COORD_PORT + server->port_ctr); //node will be placed on this shard server
    shard_loc_ptr.reset(new po6::net::location(shard_loc));
    creat_time = ++server->vc.clocks[server->port_ctr]; // incrementing vector clock
    std::cout << "sending creat time " << creat_time << std::endl;
    msg.prep_node_create(creat_time); 
    ret = server->bb.send(shard_loc, msg.buf);
    if (ret != BUSYBEE_SUCCESS)
    {
        //error occurred
        std::cerr << "msg send error: " << ret << std::endl;
        return NULL;
    }
    if ((ret = server->bb.recv(&shard_loc, &msg.buf)) != BUSYBEE_SUCCESS)        
    {
        //error occurred
        std::cerr << "msg recv error: " << ret << std::endl;
        return NULL;
    }
    msg.unpack_create_ack(&node_addr);
    elem = new coordinator::graph_elem(*shard_loc_ptr, *shard_loc_ptr,
        node_addr, node_addr, creat_time, creat_time);
    server->elements.push_back(elem);
    
    //std::cout << "Node id is " << (void *) elem << " at port " 
    return (void *)elem;
}

void
delete_node(void *node_handle, coordinator::central *server)
{
    coordinator::graph_elem *node = (coordinator::graph_elem *)node_handle;
    message::message msg(message::NODE_DELETE_REQ);
    uint64_t del_time = ++server->vc.clocks[node->loc1.port - COORD_PORT - 1];
    busybee_returncode ret;
    msg.prep_node_delete((size_t)node->mem_addr1, del_time);
    if ((ret = server->bb.send(node->loc1, msg.buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg send error " << ret << std::endl;
        return;
    }

    std::cout << "Sent message to delete node " << (void*)node_handle
        << std::endl;
}

void
reachability_request(void *mem_addr1, void *mem_addr2, coordinator::central *server)
{
    static uint32_t req_counter = 0;

    coordinator::graph_elem *elem1 = (coordinator::graph_elem *)mem_addr1; //src node
    coordinator::graph_elem *elem2 = (coordinator::graph_elem *)mem_addr2; //dest node
    std::unique_ptr<po6::net::location> src_loc, dest_loc;
    message::message msg(message::REACHABLE_PROP);
    busybee_returncode ret;
    std::vector<size_t> src; // vector to hold src node
    uint32_t rec_counter; // for reply
    bool is_reachable; // for reply
    size_t src_node; //for reply
    size_t num_del_nodes;
    std::unique_ptr<std::vector<size_t>> del_nodes(new std::vector<size_t>());
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>());
    po6::net::location rec_loc(COORD_IPADDR, COORD_PORT);

    req_counter++;
    src.push_back((size_t)elem1->mem_addr1);
    std::cout << "Reachability request number " << req_counter << " from source"
              << " node " << mem_addr1 << " to destination node " << mem_addr2
              << std::endl;
    src_loc.reset(new po6::net::location(COORD_IPADDR, COORD_REC_PORT));
    dest_loc.reset(new po6::net::location(elem2->loc1));
    msg.prep_reachable_prop(src, std::move(src_loc), (size_t) elem2->mem_addr1,
        std::move(dest_loc), req_counter, req_counter, server->vc.clocks);
    if ((ret = server->bb.send(elem1->loc1, msg.buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg send error: " << ret << std::endl;
        return;
    }
    if ((ret = server->rec_bb.recv(&rec_loc, &msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return;
    }
    msg.unpack_reachable_rep(&rec_counter, &is_reachable, &src_node,
        &num_del_nodes, &del_nodes, &del_times);
    std::cout << "Reachable reply is " << is_reachable << " for " << 
        "request " << rec_counter << std::endl;
    
} //end reachability request

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
    std::shared_ptr<message::message> rec_msg;
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
            server, rec_msg, mtype));
        thread_pool.add_request(std::move(thr));
    }
}

int
main(int argc, char* argv[])
{
    coordinator::central server;
    void *mem_addr1, *mem_addr2, *mem_addr3, *mem_addr4;
    int i;
    std::vector<void *> nodes;
    timespec start, end, time_taken;
    uint32_t time_ms;
    std::thread *t;
    
    for (i = 0; i < NUM_NODES; i++)
    {
        nodes.push_back(create_node(&server));
    }
    srand(time(NULL));
    /*
    for (i = 0; i < NUM_EDGES; i++)
    {
        int first = rand() % NUM_NODES;
        int second = rand() % NUM_NODES;
        while(second == first) //no self-loop edges
        {
            second = rand() % NUM_NODES;
        }
        create_edge(nodes[first], nodes[second], &server);
    }
    */
    for (i = 0; i < NUM_NODES; i++)
    {
        //ring graph
        create_edge(nodes[i], nodes[(i+1) % NUM_NODES], &server);
    }
    //clock_gettime (CLOCK_PROCESS_CPUTIME_ID, &start); 
    //t = new std::thread (msg_handler, &server);
    //t->detach();
    for (i = 0; i < 10; i++)
    {
        //int node_index = rand() % NUM_NODES;
        delete_node(nodes[i*i], &server);
    }
    clock_gettime(CLOCK_MONOTONIC, &start);    
    /*
    for (i = 0; i < NUM_REQUESTS; i++)
    {
        reachability_request(nodes[rand() % 10 + NUM_NODES/2], 
            nodes[rand() % 10 + NUM_NODES/2], 
            &server);
    }
    */
    for (i = 0; i < NUM_NODES; i++)
    {
        reachability_request(nodes[i], nodes[(i+1) % NUM_NODES], &server);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);  
    //clock_gettime (CLOCK_PROCESS_CPUTIME_ID, &end);   

    time_taken = diff(start, end);
    time_ms = time_taken.tv_sec * 1000 + time_taken.tv_nsec/1000000;
    std::cout << "Time = " << time_ms << std::endl;

    std::cin >> i;

    while (0) {
    uint32_t choice;
    std::cout << "Options:\n1. Create edge\n2. Create vertex\n3. Reachability"
        << " request\n";
    std::cin >> choice;
    
    switch (choice)
    {
        case 1:
            std::cout << "Enter node 1" << std::endl;
            std::cin >> mem_addr1;
            std::cout << "Enter node 2" << std::endl;
            std::cin >> mem_addr2;
            create_edge (mem_addr1, mem_addr2, &server);
            break;

        case 2:
            create_node (&server);
            break;

        case 3:
            std::cout << "Enter node 1" << std::endl;
            std::cin >> mem_addr1;
            std::cout << "Enter node 2" << std::endl;
            std::cin >> mem_addr2;
            reachability_request (mem_addr1, mem_addr2, &server);
            break;

    } //end switch
    } //end while
} //end main
