/*
 * =====================================================================================
 *
 *       Filename:  central.cc
 *
 *    Description:  Central server test run
 *
 *        Version:  1.0
 *        Created:  11/06/2012 11:47:04 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

//C
#include <unistd.h>
#include <cstdlib>

//C++
#include <iostream>
#include <time.h>
#include <unordered_map>

//e
#include "e/buffer.h"

//Busybee
#include "busybee_constants.h"

//Weaver
#include "central.h"
#include "../message/message.h"
#include "threadpool/threadpool.h"

#define NUM_NODES 100000
#define NUM_EDGES 150000
#define NUM_REQUESTS 100
#define NUM_THREADS 4

class pending_req
{
    public:
        int type; //1:edge, 2:node, 3:reach
        void *mem_addr1;
        void *mem_addr2;
        central_coordinator::graph_elem *elem1;
        central_coordinator::graph_elem *elem2;
        po6::net::location loc1;
        po6::net::location loc2;
        int num_edge;
};

std::unordered_map<uint32_t, pending_req> pending;
po6::threads::mutex pending_mutex;

void
handle_pending_req (central_coordinator::central *server,
                    std::shared_ptr<message::message> msg,
                    enum message::msg_type m_type)
{
    uint32_t req_num;
    void *mem_addr;
    central_coordinator::graph_elem *elem;
    bool is_reachable;
    size_t src_node;
    uint16_t src_port;
    switch (m_type)
    {
    case message::NODE_CREATE_ACK:
        /*
        if (msg->unpack_create_ack (&req_num, &mem_addr) != 0)
        {
            std::cerr << "invalid msg in NODE_CREATE_ACK" << std::endl;
        }
        elem = new central_coordinator::graph_elem (pending[req_num].loc1,
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
        msg->unpack_reachable_rep (&req_num, &is_reachable, &src_node, &src_port);
        std::cout << "Reachable reply is " << is_reachable << " for "
                  << "request " << req_num << std::endl;
        break;
    
    default:
        std::cerr << "unexpected msg type " << m_type << std::endl;
    }
}

void*
create_edge (void *mem_addr1, void *mem_addr2, 
    central_coordinator::central *server)
{
    central_coordinator::graph_elem *elem;
    central_coordinator::graph_elem *elem1 = 
            (central_coordinator::graph_elem *) mem_addr1;
    central_coordinator::graph_elem *elem2 = 
            (central_coordinator::graph_elem *) mem_addr2;
    po6::net::location send_loc1 (elem1->loc1);
    po6::net::location send_loc2 (elem2->loc1);
    enum message::edge_direction dir = message::FIRST_TO_SECOND;
    message::message msg (message::EDGE_CREATE_REQ);
    busybee_returncode ret;

    msg.prep_edge_create ((size_t) elem1->mem_addr1, (size_t) elem2->mem_addr1,
                          elem2->loc1, dir);
    if ((ret = server->bb.send (send_loc1, msg.buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg send error: " << ret << std::endl;
        return NULL;
    }
    if ((ret = server->bb.recv (&send_loc1, &msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return NULL;
    }
    msg.unpack_create_ack (&mem_addr1);

    dir = message::SECOND_TO_FIRST;
    msg.change_type (message::EDGE_CREATE_REQ);
    msg.prep_edge_create ((size_t) elem2->mem_addr1, (size_t) elem1->mem_addr1, 
                          elem1->loc1, dir);
    if ((ret = server->bb.send (send_loc2, msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg send error: " << ret << std::endl;
        return NULL;
    }
    if ((ret = server->bb.recv (&send_loc2, &msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return NULL;
    }
    msg.unpack_create_ack (&mem_addr2);

    elem = new central_coordinator::graph_elem (elem1->loc1, elem2->loc1, 
                                                mem_addr1, mem_addr2);
    server->elements.push_back (elem);
            
    //std::cout << "Edge id is " << (void *) elem << std::endl;
    return (void *) elem;
} //end create edge

void*
create_node (central_coordinator::central *server)
{
    po6::net::location *temp_loc;
    busybee_returncode ret;
    central_coordinator::graph_elem *elem;
    void *mem_addr1;
    message::message msg (message::NODE_CREATE_REQ);
    static int node_cnt = 0;
    server->port_ctr = (server->port_ctr + 1) % (MAX_PORT+1);
    if (server->port_ctr == 0) 
    {
        server->port_ctr = CENTRAL_PORT+1;
    }
    
    temp_loc = new po6::net::location (LOCAL_IPADDR, server->port_ctr);
    msg.prep_node_create();
    ret = server->bb.send (*temp_loc, msg.buf);
    if (ret != BUSYBEE_SUCCESS) 
    {
        //error occurred
        std::cerr << "msg send error: " << ret << std::endl;
        return NULL;
    }
    if ((ret = server->bb.recv (temp_loc, &msg.buf)) != BUSYBEE_SUCCESS)        
    {
        //error occurred
        std::cerr << "msg recv error: " << ret << std::endl;
        return NULL;
    }
    temp_loc = new po6::net::location (LOCAL_IPADDR, server->port_ctr);
    msg.unpack_create_ack (&mem_addr1);
    elem = new central_coordinator::graph_elem (*temp_loc, *temp_loc, mem_addr1,
                                                mem_addr1);
    server->elements.push_back (elem);
    
    //std::cout << "Node id is " << (void *) elem << " at port " 
    //        << elem->loc1.port << " " << (node_cnt++) << std::endl;
    return (void *) elem;
} //end create node

void
reachability_request (void *mem_addr1, void *mem_addr2,
    central_coordinator::central *server)
{
    static uint32_t req_counter = 0;

    uint32_t rec_counter;
    bool is_reachable;
    po6::net::location rec_loc (LOCAL_IPADDR, CENTRAL_PORT);
    central_coordinator::graph_elem *elem1 = 
            (central_coordinator::graph_elem *) mem_addr1;
    central_coordinator::graph_elem *elem2 = 
            (central_coordinator::graph_elem *) mem_addr2;
    message::message msg(message::REACHABLE_PROP);
    busybee_returncode ret;
    std::vector<size_t> src;
    size_t src_node;
    uint16_t src_port;

    req_counter++;
    src.push_back ((size_t)elem1->mem_addr1);
    std::cout << "Reachability request number " << req_counter << " from source"
              << " node " << mem_addr1 << " to destination node " << mem_addr2
              << std::endl;
    if (msg.prep_reachable_prop (src, CENTRAL_REC_PORT, (size_t) elem2->mem_addr1,
                                 elem2->loc1.port, req_counter, req_counter) 
        != 0) 
    {
        std::cerr << "invalid msg packing" << std::endl;
        return;
    }
    if ((ret = server->bb.send (elem1->loc1, msg.buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg send error: " << ret << std::endl;
        return;
    }

    if ((ret = server->rec_bb.recv (&rec_loc, &msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return;
    }
    msg.unpack_reachable_rep (&rec_counter, &is_reachable, &src_node, &src_port);
    std::cout << "Reachable reply is " << is_reachable << " for " << 
        "request " << rec_counter << std::endl;
    
} //end reachability request

timespec
diff (timespec start, timespec end)
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
msg_handler (central_coordinator::central *server)
{
        busybee_returncode ret;
        po6::net::location sender (LOCAL_IPADDR, CENTRAL_PORT);
        message::message msg (message::ERROR);
        uint32_t code;
        enum message::msg_type mtype;
        std::shared_ptr<message::message> rec_msg;
        central_coordinator::thread::pool thread_pool (NUM_THREADS);
        std::unique_ptr<central_coordinator::thread::unstarted_thread> thr;
        
        while (1)
        {
            if ((ret = server->rec_bb.recv (&sender, &msg.buf)) != BUSYBEE_SUCCESS)
            {
                std::cerr << "msg recv error: " << ret << std::endl;
                continue;
            }
            rec_msg.reset (new message::message (msg));
            rec_msg->buf->unpack_from (BUSYBEE_HEADER_SIZE) >> code;
            mtype = (enum message::msg_type) code;
            thr.reset (new central_coordinator::thread::unstarted_thread (
                handle_pending_req,
                server,
                rec_msg,
                mtype));
            thread_pool.add_request (std::move (thr));
        }
}

int
main (int argc, char* argv[])
{
    central_coordinator::central server;
    void *mem_addr1, *mem_addr2, *mem_addr3, *mem_addr4;
    int i;
    std::vector<void *> nodes;
    timespec start, end, time_taken;
    uint32_t time_ms;
    std::thread *t;
    
    for (i = 0; i < NUM_NODES; i++)
    {
        nodes.push_back (create_node (&server));
    }
    srand (time (NULL));
    for (i = 0; i < NUM_EDGES; i++)
    {
        int first = rand() % NUM_NODES;
        int second = rand() % NUM_NODES;
        while (second == first) //no self-loop edges
        {
            second = rand() % NUM_NODES;
        }
        create_edge (nodes[first], nodes[second], &server);
    }
    //clock_gettime (CLOCK_PROCESS_CPUTIME_ID, &start); 
    //t = new std::thread (msg_handler, &server);
    //t->detach();
    clock_gettime (CLOCK_MONOTONIC, &start);    
    for (i = 0; i < NUM_REQUESTS; i++)
    {
        reachability_request (nodes[rand() % 10 + NUM_NODES/2], 
                              nodes[rand() % 10 + NUM_NODES/2], 
                              &server);
    }
    clock_gettime (CLOCK_MONOTONIC, &end);  
    //clock_gettime (CLOCK_PROCESS_CPUTIME_ID, &end);   

    time_taken = diff (start, end);
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
