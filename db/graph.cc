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

#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>
#include <unordered_map>
#include <po6/net/location.h>
#include <po6/threads/mutex.h>
#include <e/buffer.h>
#include "busybee_constants.h"

//Weaver
#include "graph.h"
#include "common/message/message.h"
#include "threadpool/threadpool.h"

#define IP_ADDR "127.0.0.1"
#define COORD_IP_ADDR "127.0.0.1" //XXX
#define MY_IP_ADDR "127.0.0.1" //XXX
#define PORT_BASE 5200

//Pending batched request
class batch_request
{
    public:
    uint16_t prev_port; //prev server's port
    uint32_t id; //prev server's req id
    int num; //number of onward requests
    bool reachable;
    std::vector<size_t> src_nodes;
    void *dest_addr; //dest node's handle
    uint16_t dest_port; //dest node's port
    po6::threads::mutex mutex;

    batch_request ()
    {
        prev_port = 0;
        id = 0;
        num = 0;
        reachable = false;
        dest_addr = NULL;
        dest_port = 0;
    }

    batch_request (uint16_t p, uint32_t c, int n)
        : prev_port(p)
        , id(c)
        , num(n)
        , reachable (false)
    {
    }
    
    batch_request (const batch_request &tup)
    {
        prev_port = tup.prev_port;
        id = tup.id;
        num = tup.num;
        reachable = tup.reachable;
        src_nodes = tup.src_nodes;
        dest_addr = tup.dest_addr;
        dest_port = tup.dest_port;
    }

};

int order, port;
uint32_t incoming_req_id_counter, outgoing_req_id_counter;
po6::threads::mutex incoming_req_id_counter_mutex, outgoing_req_id_counter_mutex;
std::unordered_map<uint32_t, batch_request> outstanding_req; 
std::unordered_map<uint32_t, uint32_t> pending_batch;
db::thread::pool thread_pool (NUM_THREADS);

// create a graph node
void
handle_create_node (db::graph *G, std::shared_ptr<message::message> m)
{
    db::element::node *n = G->create_node (0);
    message::message msg (message::NODE_CREATE_ACK);
    po6::net::location coord (COORD_IP_ADDR, PORT_BASE);
    busybee_returncode ret;

    if (msg.prep_create_ack ((size_t) n) != 0) 
    {
        return;
    }
    G->bb_lock.lock();
    if ((ret = G->bb.send (coord, msg.buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg send error: " << ret << std::endl;
        G->bb_lock.unlock();
        return;
    }
    G->bb_lock.unlock();
}

// create a graph edge
void
handle_create_edge (db::graph *G, std::shared_ptr<message::message> msg)
{
    void *node1_handle, *node2_handle;
    std::unique_ptr<po6::net::location> remote, local;
    uint32_t direction;
    std::unique_ptr<db::element::meta_element> n1, n2;
    db::element::edge *e;
    po6::net::location coord (COORD_IP_ADDR, PORT_BASE);
    busybee_returncode ret;

    remote = msg->unpack_edge_create (&node1_handle, &node2_handle, &direction);
    if (remote == NULL)
    {
        return;
    }
    local.reset (new po6::net::location (MY_IP_ADDR, port));
    n1.reset (new db::element::meta_element (*local, 0, UINT_MAX, node1_handle));
    n2.reset (new db::element::meta_element (*remote, 0, UINT_MAX, node2_handle));
    
    e = G->create_edge (std::move(n1), std::move(n2), (uint32_t) direction, 0);
    msg->change_type (message::EDGE_CREATE_ACK);
    if (msg->prep_create_ack ((size_t) e) != 0) 
    {
        return;
    }
    G->bb_lock.lock();
    if ((ret = G->bb.send (coord, msg->buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg send error: " << ret << std::endl;
        G->bb_lock.unlock();
        return;
    }
    G->bb_lock.unlock();
}

// reachability request starting from src_nodes to dest_node
void
handle_reachable_request (db::graph *G, std::shared_ptr<message::message> msg)
{
    void *dest_node; // destination node handle
    uint16_t prev_port, // previous server's port
             dest_port; // target node's port
    uint32_t coord_req_id, // central coordinator req id
             prev_req_id, // previous server's req counter
             my_batch_req_id, // this server's request id
             my_outgoing_req_id; // each forwarded batched req id

    std::unique_ptr<po6::net::location> remote; // location pointer reused for sending messages
    db::element::node *n; // node pointer reused for each source node
    busybee_returncode ret;
    bool reached = false; // indicates if we have reached destination node
    void *reach_node = NULL; // if reached destination, immediate preceding neighbor
    bool propagate_req = false; // need to propagate request onward
    std::unordered_map<uint16_t, std::vector<size_t>> msg_batch; // batched messages to propagate
    std::vector<size_t> src_nodes;
    std::vector<size_t>::iterator src_iter;
        
    // get the list of source nodes to check for reachability, as well as the single sink node
    src_nodes = msg->unpack_reachable_prop (&prev_port, 
        &dest_node, 
        &dest_port,
        &coord_req_id, 
        &prev_req_id);

    for (src_iter = src_nodes.begin(); src_iter < src_nodes.end(); src_iter++)
    {
        static int node_ctr = 0;
        // because the coordinator placed the node's address in the message, 
        // we can just cast it back to a pointer
        n = (db::element::node *) (*src_iter);
        if (!G->mark_visited (n, coord_req_id))
        {
            n->cache_mutex.lock();
            if (n->cache.entry_exists (dest_port, dest_node))
            {
                // we have a cached result
                std::cout << "Serving request from cache as " ;
                if (n->cache.get_cached_value (dest_port, dest_node))
                {
                    // is cached and is reachable
                    reached = true;
                    reach_node = (void *) n;
                    std::cout << "true" << std::endl;
                } else {
                    // is not reachable
                    std::cout << "false" << std::endl;
                }
                n->cache_mutex.unlock();
            } else {
                n->cache_mutex.unlock();
                std::vector<db::element::meta_element>::iterator iter;
                for (iter = n->out_edges.begin(); iter < n->out_edges.end(); iter++)
                {
                    db::element::edge *nbr = (db::element::edge *) iter->get_addr();
                    propagate_req = true;
                    if (nbr->to.get_addr() == dest_node &&
                        nbr->to.get_port() == dest_port)
                    {
                // Done! Send msg back to central server
                        reached = true;
                        reach_node = (void *) n;
                        break;
                    } else {
                // Continue propagating reachability request
                        msg_batch[nbr->to.get_port()].push_back((size_t)nbr->to.get_addr());
                    }
                }
            }
        }
        if (reached)
        {
            break;
        }
    } //end src_nodes loop
    
    //send messages
    if (reached)
    {   //need to send back ack
        msg->change_type (message::REACHABLE_REPLY);
        msg->prep_reachable_rep (prev_req_id, true, (size_t) reach_node,
            G->myloc.port);
        remote.reset (new po6::net::location (IP_ADDR, prev_port)); //TODO change IP_ADDR here
        G->bb_lock.lock();
        if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        G->bb_lock.unlock();
    } else if (propagate_req) { 
        // the destination is not reachable locally in one hop from this server, so
        // now we have to contact all the reachable neighbors and see if they can reach
        std::unordered_map<uint16_t, std::vector<size_t>>::iterator loc_iter;
        //need mutex since there can be multiple replies
        //for same outstanding req
        //TODO coarse locking for now.
        incoming_req_id_counter_mutex.lock();
        my_batch_req_id = incoming_req_id_counter++;
        //outstanding_req[my_batch_req_id].mutex.lock();
        //incoming_req_id_counter_mutex.unlock();
        outstanding_req[my_batch_req_id].prev_port = prev_port;
        outstanding_req[my_batch_req_id].id = prev_req_id;
        outstanding_req[my_batch_req_id].num = 0;
        outstanding_req[my_batch_req_id].src_nodes = src_nodes;
        outstanding_req[my_batch_req_id].dest_addr = dest_node;
        outstanding_req[my_batch_req_id].dest_port = dest_port;
        //incoming_req_id_counter_mutex.unlock();
        for (loc_iter = msg_batch.begin(); loc_iter !=
             msg_batch.end(); loc_iter++)
        {
            msg.reset (new message::message (message::REACHABLE_PROP));
            //adding this as a pending request
            outstanding_req[my_batch_req_id].num++;
            //outgoing_req_id_counter_mutex.lock();
            my_outgoing_req_id = outgoing_req_id_counter++;
            pending_batch[my_outgoing_req_id] = my_batch_req_id;
            //outgoing_req_id_counter_mutex.unlock();
            msg->prep_reachable_prop (loc_iter->second,
                G->myloc.port, 
                (size_t)dest_node, 
                dest_port,
                coord_req_id,
                (my_outgoing_req_id));
            if (loc_iter->first == G->myloc.port)
            {   //no need to send message since it is local
                std::unique_ptr<db::thread::unstarted_thread> t;
                t.reset (new db::thread::unstarted_thread (handle_reachable_request, G, msg));
                thread_pool.add_request (std::move(t));
            } else {
                remote.reset (new po6::net::location (IP_ADDR, loc_iter->first));
                G->bb_lock.lock();
                if ((ret = G->bb.send (*remote, msg->buf)) !=
                    BUSYBEE_SUCCESS)
                {
                    std::cerr << "msg send error: " << ret <<
                    std::endl;
                }
                G->bb_lock.unlock();
            }
        }
        //outstanding_req[my_batch_req_id].mutex.unlock();
        incoming_req_id_counter_mutex.unlock();
        msg_batch.clear();  
    } else {   
        //need to send back nack
        msg->change_type (message::REACHABLE_REPLY);
        msg->prep_reachable_rep (prev_req_id, false, 0, G->myloc.port);
        remote.reset (new po6::net::location (IP_ADDR, prev_port));
        G->bb_lock.lock();
        if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        G->bb_lock.unlock();
    }
}

// handle reply for a previously forwarded reachability request
// if this is the first positive or last negative reply, propagate to previous
// server immediately;
// otherwise wait for other replies
void
handle_reachable_reply (db::graph *G, std::shared_ptr<message::message> msg)
{
    busybee_returncode ret;
    uint32_t my_outgoing_req_id, my_batch_req_id, prev_req_id;
    bool reachable_reply;
    uint16_t prev_port;
    size_t reach_node, prev_reach_node;
    uint16_t reach_port;
    db::element::node *n;
    std::unique_ptr<po6::net::location> remote;

    //TODO coarse locking for now.
    incoming_req_id_counter_mutex.lock();
    msg->unpack_reachable_rep (&my_outgoing_req_id,
                               &reachable_reply,
                               &reach_node,
                               &reach_port);
    //outgoing_req_id_counter_mutex.lock();
    my_batch_req_id = pending_batch[my_outgoing_req_id];
    pending_batch.erase (my_outgoing_req_id);
    //outgoing_req_id_counter_mutex.unlock();
    //outstanding_req[my_batch_req_id].mutex.lock();
    --outstanding_req[my_batch_req_id].num;
    prev_port = outstanding_req[my_batch_req_id].prev_port;
    prev_req_id = outstanding_req[my_batch_req_id].id;
    
    if (reachable_reply)
    {   //caching positive result
        std::vector<size_t>::iterator node_iter;
        std::vector<size_t> src_nodes = outstanding_req[my_batch_req_id].src_nodes;
        for (node_iter = src_nodes.begin(); node_iter < src_nodes.end(); node_iter++)
        {
            std::vector<db::element::meta_element>::iterator iter;
            n = (db::element::node *) (*node_iter);
            for (iter = n->out_edges.begin(); iter < n->out_edges.end(); iter++)
            {
                db::element::edge *nbr = (db::element::edge *) iter->get_addr();
                if ((nbr->to.get_addr() == (void *)reach_node) &&
                    (nbr->to.get_port() == reach_port))
                {
                    n->cache_mutex.lock();
                    n->cache.insert_entry (
                        outstanding_req[my_batch_req_id].dest_port,
                        outstanding_req[my_batch_req_id].dest_addr, true);
                    n->cache_mutex.unlock();
                    prev_reach_node = (size_t) n;
                    break;
                }
            }
        }
    }

    /*
     * check if this is the last expected reply for this batched request
     * and we got all negative replies till now
     * or this is a positive reachable reply
     */
    if (((outstanding_req[my_batch_req_id].num == 0) || reachable_reply)
        && !outstanding_req[my_batch_req_id].reachable)
    {
        outstanding_req[my_batch_req_id].reachable |= reachable_reply;
        msg->prep_reachable_rep (prev_req_id,
                                 reachable_reply,
                                 prev_reach_node,
                                 G->myloc.port);
        if (prev_port == G->myloc.port)
        { //no need to send msg over network
            std::unique_ptr<db::thread::unstarted_thread> t;
            t.reset (new db::thread::unstarted_thread (handle_reachable_reply, G, msg));
            thread_pool.add_request (std::move(t));
        } else
        {
            remote.reset (new po6::net::location (IP_ADDR, prev_port));
            G->bb_lock.lock();
            if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
            {
                std::cerr << "msg send error: " << ret << std::endl;
            }
            G->bb_lock.unlock();
        }
    }

    if (outstanding_req[my_batch_req_id].num == 0)
    {
        //delete visited property
        std::vector<size_t>::iterator node_iter;
        std::vector<size_t> src_nodes =  outstanding_req[my_batch_req_id].src_nodes;
        for (node_iter = src_nodes.begin(); node_iter < src_nodes.end(); node_iter++)
        {
            std::vector<db::element::meta_element>::iterator iter;
            n = (db::element::node *) (*node_iter);
            G->remove_visited (n, my_batch_req_id);
            /*
             * TODO:
             * Caching negative results is tricky, because we don't
             * know whether its truly a negative result or it was
             * because the next node was visited by someone else
            if (!outstanding_req[my_batch_req_id].reachable)
            {   //cache negative result
                n->cache_mutex.lock();
                n->cache.insert_entry (
                    outstanding_req[my_batch_req_id].dest_port,
                    outstanding_req[my_batch_req_id].dest_addr,
                    false);
                n->cache_mutex.unlock();
            }
            */
        }
        //delete batch request
        //outstanding_req[my_batch_req_id].mutex.unlock();
        //incoming_req_id_counter_mutex.lock();
        outstanding_req.erase (my_batch_req_id);
        //incoming_req_id_counter_mutex.unlock();
    } else
    {
        //outstanding_req[my_batch_req_id].mutex.unlock();
    }
    incoming_req_id_counter_mutex.unlock();
} 

// server loop for the shard server
void
runner (db::graph *G)
{
    busybee_returncode ret;
    po6::net::location sender (IP_ADDR, PORT_BASE);
    message::message msg (message::ERROR);
    uint32_t code;
    enum message::msg_type mtype;
    std::shared_ptr<message::message> rec_msg;
    std::unique_ptr<db::thread::unstarted_thread> thr;
    std::unique_ptr<std::thread> t;

    uint32_t loop_count = 0;
    while (1)
    {
        if ((ret = G->bb_recv.recv (&sender, &msg.buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg.reset (new message::message (msg));
        rec_msg->buf->unpack_from (BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type) code;
        switch (mtype)
        {
            case message::NODE_CREATE_REQ:
                thr.reset (new db::thread::unstarted_thread (
                    handle_create_node,
                    G,
                    NULL));
                thread_pool.add_request (std::move(thr));
                break;

            case message::EDGE_CREATE_REQ:
                thr.reset (new db::thread::unstarted_thread (
                    handle_create_edge,
                    G,
                    rec_msg));
                thread_pool.add_request (std::move(thr));
                break;
            
            case message::REACHABLE_PROP:
                thr.reset (new db::thread::unstarted_thread (
                    handle_reachable_request,
                    G,
                    rec_msg));
                thread_pool.add_request (std::move(thr));
                break;

            case message::REACHABLE_REPLY:
                thr.reset (new db::thread::unstarted_thread (
                    handle_reachable_reply,
                    G,
                    rec_msg));
                thread_pool.add_request (std::move(thr));
                break;

            default:
                std::cerr << "unexpected msg type " << code << std::endl;
        }

    }

}

int
main (int argc, char* argv[])
{
    if (argc != 2) 
    {
        std::cerr << "Usage: " << argv[0] << " <order> " << std::endl;
        return -1;
    }

    std::cout << "Testing Weaver" << std::endl;
    
    order = atoi (argv[1]);
    port = PORT_BASE + order;
    outgoing_req_id_counter = 0;
    incoming_req_id_counter = 0;

    db::graph G (IP_ADDR, port);
    
    runner (&G);

    return 0;
} //end main
