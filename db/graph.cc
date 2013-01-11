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
#include "common/message.h"
#include "threadpool/threadpool.h"

#define IP_ADDR "127.0.0.1"
#define COORD_IP_ADDR "127.0.0.1" //XXX
#define MY_IP_ADDR "127.0.0.1" //XXX
#define PORT_BASE 5200

/*
 * XXX what is a batch tuple? rename to "request"
 */
class batch_tuple
{
    public:
    uint16_t port;
    uint32_t counter; //prev req id
    int num; //number of requests
    bool reachable;
    std::vector<size_t> src_nodes;
    void *dest_addr;
    uint16_t dest_port;
    po6::threads::mutex mutex;

    batch_tuple ()
    {
        port = 0;
        counter = 0;
        num = 0;
        reachable = false;
    }

    batch_tuple (uint16_t p, uint32_t c, int n)
        : port(p)
        , counter(c)
        , num(n)
        , reachable (false)
    {
    }
    
    batch_tuple (const batch_tuple &tup)
    {
        port = tup.port;
        counter = tup.counter;
        num = tup.num;
        reachable = tup.reachable;
        src_nodes = tup.src_nodes;
        dest_addr = tup.dest_addr;
        dest_port = tup.dest_port;
    }

};

int order, port;
uint32_t batch_req_counter, local_req_counter;
po6::threads::mutex batch_req_counter_mutex, local_req_counter_mutex;
std::unordered_map<uint32_t, batch_tuple> outstanding_req; 
std::unordered_map<uint32_t, uint32_t> pending_batch;
db::thread::pool thread_pool (NUM_THREADS);

// create a graph node
void
handle_create_node (db::graph *G, std::shared_ptr<message::message> m)
{
    db::element::node *n = G->create_node (0);
    message::message msg (message::NODE_CREATE_ACK);
    po6::net::location central (IP_ADDR, PORT_BASE);
    busybee_returncode ret;

    if (msg.prep_create_ack ((size_t) n) != 0) 
    {
        return;
    }
    G->bb_lock.lock();
    if ((ret = G->bb.send (central, msg.buf)) != BUSYBEE_SUCCESS) 
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
    void *mem_addr1, *mem_addr2;
    po6::net::location *remote, *local;
    uint32_t direction;
    std::unique_ptr<db::element::meta_element> n1, n2;
    db::element::edge *e;
    po6::net::location central (IP_ADDR, PORT_BASE);
    busybee_returncode ret;

    if (msg->unpack_edge_create (&mem_addr1, &mem_addr2, &remote, &direction) != 0)
    {
        return;
    }
    local = new po6::net::location (IP_ADDR, port);
    n1.reset (new db::element::meta_element (*local, 0, UINT_MAX,
                                             mem_addr1));
    n2.reset (new db::element::meta_element (*remote, 0, UINT_MAX,
                                             mem_addr2));
    
    e = G->create_edge (std::move(n1), std::move(n2), (uint32_t) direction, 0);
    msg->change_type (message::EDGE_CREATE_ACK);
    if (msg->prep_create_ack ((size_t) e) != 0) 
    {
        return;
    }
    G->bb_lock.lock();
    if ((ret = G->bb.send (central, msg->buf)) != BUSYBEE_SUCCESS) 
    {
        std::cerr << "msg send error: " << ret << std::endl;
        G->bb_lock.unlock();
        return;
    }
    G->bb_lock.unlock();
    delete remote;
    delete local;
}

// XXX ??? check if node XXX can reach node XXX
void
handle_reachable_request (db::graph *G, std::shared_ptr<message::message> msg)
{
    void *mem_addr2; //XXX rename
    db::element::node *n; // XXX?
    busybee_returncode ret;
    std::unique_ptr<po6::net::location> remote; // XXX?
    uint16_t from_port, //previous node's port
             to_port; //target node's port
    uint32_t req_counter, // XXX counter -> id rename -- central server req counter
             prev_req_counter, //previous node req counter
             my_batch_req_counter, //this request's number
             my_local_req_counter; //each forward batched req number
    bool reached = false;
    void *reach_node = NULL; // XXX?
    bool send_msg = false; // XXX?
    std::unordered_map<uint16_t, std::vector<size_t>> msg_batch;
    std::vector<size_t> src_nodes;
    std::vector<size_t>::iterator src_iter;
        
    // get the list of source nodes to check for reachability, as well as the single sink node
    src_nodes = msg->unpack_reachable_prop (&from_port, 
                                            &mem_addr2, 
                                            &to_port,
                                            &req_counter, 
                                            &prev_req_counter);

    for (src_iter = src_nodes.begin(); src_iter < src_nodes.end();
         src_iter++)
    {
    static int node_ctr = 0;
    // because the coordinator placed the node's address in the message, we can just cast it back to a pointer
    n = (db::element::node *) (*src_iter);
    // XXX old properties removed in handle_reachable_reply() XXX what does this mean?
    if (!G->mark_visited (n, req_counter))
    {
        n->cache_mutex.lock();
        if (n->cache.entry_exists (to_port, mem_addr2))
        {
	  // we have a cached result
            std::cout << "Serving request from cache as " ;
            if (n->cache.get_cached_value (to_port, mem_addr2))
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
                send_msg = true;
                if (nbr->to.get_addr() == mem_addr2 &&
                    nbr->to.get_port() == to_port)
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
        msg->prep_reachable_rep (prev_req_counter, true, (size_t) reach_node,
                                 G->myloc.port);
        remote.reset (new po6::net::location (IP_ADDR, from_port));
        G->bb_lock.lock();
        if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }   
        G->bb_lock.unlock();
    } else if (send_msg)
    { 
        // the destination is not reachable locally in one hop from this server, so
        // now we have to contact all the reachable neighbors and see if they can reach
      //need to send batched msges onwards
        std::unordered_map<uint16_t, std::vector<size_t>>::iterator loc_iter;
        //need mutex since there can be multiple replies
        //for same outstanding req
        //TODO coarse locking for now.
        batch_req_counter_mutex.lock();
        my_batch_req_counter = batch_req_counter++;
        //outstanding_req[my_batch_req_counter].mutex.lock();
        //batch_req_counter_mutex.unlock();
        outstanding_req[my_batch_req_counter].port = from_port;
        outstanding_req[my_batch_req_counter].counter = prev_req_counter;
        outstanding_req[my_batch_req_counter].num = 0;
        outstanding_req[my_batch_req_counter].src_nodes = src_nodes;
        outstanding_req[my_batch_req_counter].dest_addr = mem_addr2;
        outstanding_req[my_batch_req_counter].dest_port = to_port;
        //batch_req_counter_mutex.unlock();
        for (loc_iter = msg_batch.begin(); loc_iter !=
             msg_batch.end(); loc_iter++)
        {
            msg.reset (new message::message (message::REACHABLE_PROP));
            //new batched request
            //adding this as a pending request
            outstanding_req[my_batch_req_counter].num++;
            //local_req_counter_mutex.lock();
            my_local_req_counter = local_req_counter++;
            pending_batch[my_local_req_counter] = my_batch_req_counter;
            //local_req_counter_mutex.unlock();
            msg->prep_reachable_prop (loc_iter->second,
                                      G->myloc.port, 
                                      (size_t)mem_addr2, 
                                      to_port,
                                      req_counter,
                                      (my_local_req_counter));
            if (loc_iter->first == G->myloc.port)
            {   //no need to send message since it is local
                std::unique_ptr<db::thread::unstarted_thread> t;
                t.reset (new db::thread::unstarted_thread (
                        handle_reachable_request,
                        G,
                        msg));
                thread_pool.add_request (std::move(t));
            } else
            {
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
        //outstanding_req[my_batch_req_counter].mutex.unlock();
        batch_req_counter_mutex.unlock();
        msg_batch.clear();  
    } else
    {   //need to send back nack
        msg->change_type (message::REACHABLE_REPLY);
        msg->prep_reachable_rep (prev_req_counter, false, 0, G->myloc.port);
        remote.reset (new po6::net::location (IP_ADDR, from_port));
        G->bb_lock.lock();
        if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        G->bb_lock.unlock();
    }
}

// XXX
void
handle_reachable_reply (db::graph *G, std::shared_ptr<message::message> msg)
{
    busybee_returncode ret;
    uint32_t my_local_req_counter, my_batch_req_counter, prev_req_counter;
    bool reachable_reply;
    uint16_t from_port;
    size_t reach_node, prev_reach_node;
    uint16_t reach_port;
    db::element::node *n;
    std::unique_ptr<po6::net::location> remote;

    //TODO coarse locking for now.
    batch_req_counter_mutex.lock();
    msg->unpack_reachable_rep (&my_local_req_counter, 
                               &reachable_reply,
                               &reach_node,
                               &reach_port);
    //local_req_counter_mutex.lock();
    my_batch_req_counter = pending_batch[my_local_req_counter];
    pending_batch.erase (my_local_req_counter);
    //local_req_counter_mutex.unlock();
    //outstanding_req[my_batch_req_counter].mutex.lock();
    --outstanding_req[my_batch_req_counter].num;
    from_port = outstanding_req[my_batch_req_counter].port;
    prev_req_counter = outstanding_req[my_batch_req_counter].counter;
    
    if (reachable_reply)
    {   //caching positive result
        std::vector<size_t>::iterator node_iter;
        std::vector<size_t> src_nodes =
                outstanding_req[my_batch_req_counter].src_nodes;
        for (node_iter = src_nodes.begin(); node_iter < src_nodes.end();
             node_iter++)
        {
            std::vector<db::element::meta_element>::iterator iter;
            n = (db::element::node *) (*node_iter);
            for (iter = n->out_edges.begin(); iter < n->out_edges.end();
                 iter++)
            {
                db::element::edge *nbr = (db::element::edge *)
                                          iter->get_addr();
                if ((nbr->to.get_addr() == (void *)reach_node) &&
                    (nbr->to.get_port() == reach_port))
                {
                    n->cache_mutex.lock();
                    n->cache.insert_entry (
                        outstanding_req[my_batch_req_counter].dest_port,
                        outstanding_req[my_batch_req_counter].dest_addr,
                        true);
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
    if (((outstanding_req[my_batch_req_counter].num == 0) || reachable_reply)
        && !outstanding_req[my_batch_req_counter].reachable)
    {
        outstanding_req[my_batch_req_counter].reachable |= reachable_reply;
        msg->prep_reachable_rep (prev_req_counter, 
                                 reachable_reply,
                                 prev_reach_node,
                                 G->myloc.port);
        if (from_port == G->myloc.port)
        { //no need to send msg over network
            std::unique_ptr<db::thread::unstarted_thread> t;
            t.reset (new db::thread::unstarted_thread (
                    handle_reachable_reply,
                    G,
                    msg));
            thread_pool.add_request (std::move(t));
        } else
        {
            remote.reset (new po6::net::location (IP_ADDR, from_port));
            G->bb_lock.lock();
            if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
            {
                std::cerr << "msg send error: " << ret << std::endl;
            }
            G->bb_lock.unlock();
        }
    }

    if (outstanding_req[my_batch_req_counter].num == 0)
    {
        //delete visited property
        std::vector<size_t>::iterator node_iter;
        std::vector<size_t> src_nodes =
                outstanding_req[my_batch_req_counter].src_nodes;
        for (node_iter = src_nodes.begin(); 
             node_iter < src_nodes.end();
             node_iter++)
        {
            std::vector<db::element::meta_element>::iterator iter;
            n = (db::element::node *) (*node_iter);
            G->remove_visited (n, my_batch_req_counter);
            /*
             * TODO:
             * Caching negative results is tricky, because we don't
             * know whether its truly a negative result or it was
             * because the next node was visited by someone else
            if (!outstanding_req[my_batch_req_counter].reachable)
            {   //cache negative result
                n->cache_mutex.lock();
                n->cache.insert_entry (
                    outstanding_req[my_batch_req_counter].dest_port,
                    outstanding_req[my_batch_req_counter].dest_addr,
                    false);
                n->cache_mutex.unlock();
            }
            */
        }
        //delete batch request
        //outstanding_req[my_batch_req_counter].mutex.unlock();
        //batch_req_counter_mutex.lock();
        outstanding_req.erase (my_batch_req_counter);
        //batch_req_counter_mutex.unlock();
    } else
    {
        //outstanding_req[my_batch_req_counter].mutex.unlock();
    }
    batch_req_counter_mutex.unlock();
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
    local_req_counter = 0;
    batch_req_counter = 0;

    db::graph G (IP_ADDR, port);
    
    runner (&G);

    return 0;
} //end main
