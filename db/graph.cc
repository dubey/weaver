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

#ifndef PO6_NDEBUG_LEAKS
#define PO6_NDEBUG_LEAKS

#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>
#include <unordered_map>
#include <po6/net/location.h>
#include <po6/threads/mutex.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#include "../common/weaver_constants.h"
#include "graph.h"
#include "common/message/message.h"
#include "threadpool/threadpool.h"

//Pending batched request
class batch_request
{
    public:
    po6::net::location prev_loc; //prev server's port
    uint32_t id; //prev server's req id
    int num; //number of onward requests
    bool reachable;
    std::vector<size_t> src_nodes;
    void *dest_addr; //dest node's handle
    po6::net::location dest_loc; //dest node's port
    po6::threads::mutex mutex;

    batch_request()
        : prev_loc(SHARD_IPADDR, 42)
        , dest_loc(SHARD_IPADDR, 42)
    {
        id = 0;
        num = 0;
        reachable = false;
        dest_addr = NULL;
    }

    batch_request(const batch_request &tup)
    {
        prev_loc = tup.prev_loc;
        id = tup.id;
        num = tup.num;
        reachable = tup.reachable;
        src_nodes = tup.src_nodes;
        dest_addr = tup.dest_addr;
        dest_loc = tup.dest_loc;
    }
};

int order, port;
uint32_t incoming_req_id_counter, outgoing_req_id_counter;
po6::threads::mutex incoming_req_id_counter_mutex, outgoing_req_id_counter_mutex;
std::unordered_map<uint32_t, std::shared_ptr<batch_request>> pending_batch;
db::thread::pool thread_pool(NUM_THREADS);

// create a graph node
void
handle_create_node(db::graph *G, std::unique_ptr<message::message> msg)
{
    uint64_t creat_time;
    db::element::node *n;
    po6::net::location coord(COORD_IPADDR, COORD_PORT);
    
    if (msg->unpack_node_create(&creat_time) != 0)
    {
        std::cerr << "error unpacking message" << std::endl;
        return;
    }
    n = G->create_node(creat_time);

    msg->change_type(message::NODE_CREATE_ACK);
    if (msg->prep_create_ack((size_t)n) != 0) 
    {
        return;
    }
    G->send(coord, msg->buf);
}

// delete a graph node
void
handle_delete_node(db::graph *G, std::unique_ptr<message::message> msg)
{
    db::element::node *n; // node to be deleted
    void *node_addr; // temp var to hold node handle
    uint64_t del_time; // time of deletion

    if (msg->unpack_node_delete(&node_addr, &del_time) != 0)
    {
        std::cerr << "error unpacking message" << std::endl;
        return;
    }
    n = (db::element::node *)node_addr;
    G->delete_node(n, del_time);
    std::cout << "Set deletion time as " << del_time << " for node "
        << node_addr << std::endl;

    //TODO send messages to all in-neighbors to mark edges as deleted.
    //TODO need locking here. Entire deletion process should be atomic.
}

// create a graph edge
void
handle_create_edge(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::unique_ptr<db::element::meta_element> n1, n2;
    uint32_t direction;
    uint64_t creat_time;
    po6::net::location coord(COORD_IPADDR, COORD_PORT);
    std::unique_ptr<po6::net::location> local(new po6::net::location(G->myloc));
    db::element::edge *e;

    if (msg->unpack_edge_create(&n1, &n2, std::move(local), &direction, &creat_time) != 0)
    {
        std::cerr << "error unpacking message" << std::endl;
        return;
    }
    
    e = G->create_edge(std::move(n1), std::move(n2), direction, creat_time);
    msg->change_type(message::EDGE_CREATE_ACK);
    if (msg->prep_create_ack((size_t)e) != 0) 
    {
        std::cerr << "error unpacking message" << std::endl;
        return;
    }
    G->send(coord, msg->buf);
}

// delete a graph edge
void
handle_delete_edge(db::graph *G, std::unique_ptr<message::message> msg)
{
}

// reachability request starting from src_nodes to dest_node
void
handle_reachable_request(db::graph *G, std::unique_ptr<message::message> msg)
{
    void *dest_node; // destination node handle
    std::unique_ptr<po6::net::location> prev_loc, //previous server's location
        dest_loc, //target node's location
        my_loc; //this server's location
    uint32_t coord_req_id, // central coordinator req id
        prev_req_id, // previous server's req counter
        my_batch_req_id, // this server's request id
        my_outgoing_req_id; // each forwarded batched req id
    std::unique_ptr<std::vector<uint64_t>> vector_clock;

    std::unique_ptr<po6::net::location> dest; // dest location reused for sending messages
    db::element::node *n; // node pointer reused for each source node
    bool reached = false; // indicates if we have reached destination node
    void *reach_node = NULL; // if reached destination, immediate preceding neighbor
    bool propagate_req = false; // need to propagate request onward
    std::unordered_map<po6::net::location, std::vector<size_t>> msg_batch; // batched messages to propagate
    std::vector<size_t> src_nodes;
    std::vector<size_t>::iterator src_iter;
        
    // get the list of source nodes to check for reachability, as well as the single sink node
    src_nodes = msg->unpack_reachable_prop(&prev_loc,
        &dest_node,
        &dest_loc,
        &coord_req_id,
        &prev_req_id,
        &vector_clock);

    // wait till all updates for this shard arrive

    //TODO need locking here. Entire traversal process should be atomic (at
    //least at the level of each node)
    for (src_iter = src_nodes.begin(); src_iter < src_nodes.end(); src_iter++)
    {
        static int node_ctr = 0;
        // because the coordinator placed the node's address in the message, 
        // we can just cast it back to a pointer
        n = (db::element::node *)(*src_iter);
        if (!G->mark_visited(n, coord_req_id))
        {
            n->cache_mutex.lock();
            if (n->cache.entry_exists(*dest_loc, dest_node))
            {
                // we have a cached result
                std::cout << "Serving request from cache as " ;
                if (n->cache.get_cached_value(*dest_loc, dest_node))
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
                    db::element::edge *nbr = (db::element::edge *)iter->get_addr();
                    propagate_req = true;
                    if (nbr->to.get_addr() == dest_node && nbr->to.get_loc() == *dest_loc) //dest matches
                        //&& nbr->get_t_u() >= coord_req_id && nbr->get_t_d() <= coord_req_id) //object not deleted
                    {
                        // Done! Send msg back to central server
                        reached = true;
                        reach_node = (void *) n;
                        break;
                    } else// if (nbr->get_t_u() >= coord_req_id && nbr->get_t_d() <= coord_req_id) //object not deleted
                    {
                        // Continue propagating reachability request
                        msg_batch[nbr->to.get_loc()].push_back((size_t)nbr->to.get_addr());
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
        my_loc.reset(new po6::net::location(G->myloc));
        msg->change_type(message::REACHABLE_REPLY);
        msg->prep_reachable_rep(prev_req_id, true, (size_t) reach_node, std::move(my_loc));
        G->send(*prev_loc, msg->buf);
    } else if (propagate_req) { 
        // the destination is not reachable locally in one hop from this server, so
        // now we have to contact all the reachable neighbors and see if they can reach
        std::unordered_map<po6::net::location, std::vector<size_t>>::iterator loc_iter;
        std::shared_ptr<batch_request> request(new batch_request());
        //need mutex since there can be multiple replies
        //for same outstanding req
        request->mutex.lock();
        request->num = 0;
        for (loc_iter = msg_batch.begin(); loc_iter != msg_batch.end(); loc_iter++)
        {
            my_loc.reset(new po6::net::location(G->myloc));
            dest.reset(new po6::net::location(*dest_loc));
            msg.reset(new message::message(message::REACHABLE_PROP));
            //adding this as a pending request
            request->num++;
            outgoing_req_id_counter_mutex.lock();
            my_outgoing_req_id = outgoing_req_id_counter++;
            pending_batch[my_outgoing_req_id] = request;
            outgoing_req_id_counter_mutex.unlock();
            msg->prep_reachable_prop(loc_iter->second,
                std::move(my_loc),
                (size_t)dest_node, 
                std::move(dest),
                coord_req_id,
                (my_outgoing_req_id),
                *vector_clock);
            if (loc_iter->first == G->myloc)
            {   //no need to send message since it is local
                std::unique_ptr<db::thread::unstarted_thread> t;
                t.reset(new db::thread::unstarted_thread(handle_reachable_request, G, std::move(msg)));
                thread_pool.add_request(std::move(t));
            } else {
                G->send(loc_iter->first, msg->buf);
            }
        }
        request->prev_loc = *prev_loc;
        request->id = prev_req_id;
        request->src_nodes = src_nodes;
        request->dest_addr = dest_node;
        request->dest_loc = *dest_loc;
        request->mutex.unlock();
        msg_batch.clear();
    } else {
        //need to send back nack
        my_loc.reset(new po6::net::location(G->myloc));
        msg->change_type(message::REACHABLE_REPLY);
        msg->prep_reachable_rep(prev_req_id, false, 0, std::move(my_loc));
        G->send(*prev_loc, msg->buf);
    }
}

// handle reply for a previously forwarded reachability request
// if this is the first positive or last negative reply, propagate to previous
// server immediately;
// otherwise wait for other replies
void
handle_reachable_reply(db::graph *G, std::unique_ptr<message::message> msg)
{
    uint32_t my_outgoing_req_id, my_batch_req_id, prev_req_id;
    std::shared_ptr<batch_request> request;
    bool reachable_reply;
    std::unique_ptr<po6::net::location> reach_loc, my_loc;
    po6::net::location prev_loc;
    size_t reach_node, prev_reach_node;
    db::element::node *n;

    reach_loc = msg->unpack_reachable_rep(&my_outgoing_req_id,
        &reachable_reply,
        &reach_node);
    outgoing_req_id_counter_mutex.lock();
    request = std::move(pending_batch[my_outgoing_req_id]);
    pending_batch.erase(my_outgoing_req_id);
    outgoing_req_id_counter_mutex.unlock();
    request->mutex.lock();
    --request->num;
    prev_loc = request->prev_loc;
    prev_req_id = request->id;
    
    if (reachable_reply)
    {   //caching positive result
        std::vector<size_t>::iterator node_iter;
        std::vector<size_t> src_nodes = request->src_nodes;
        for (node_iter = src_nodes.begin(); node_iter < src_nodes.end(); node_iter++)
        {
            std::vector<db::element::meta_element>::iterator iter;
            n = (db::element::node *) (*node_iter);
            for (iter = n->out_edges.begin(); iter < n->out_edges.end(); iter++)
            {
                db::element::edge *nbr = (db::element::edge *) iter->get_addr();
                if ((nbr->to.get_addr() == (void *)reach_node) &&
                    (nbr->to.get_loc() == *reach_loc))
                {
                    n->cache_mutex.lock();
                    n->cache.insert_entry(request->dest_loc, request->dest_addr, true);
                    n->cache_mutex.unlock();
                    prev_reach_node = (size_t)n;
                    break;
                }
            }
        }
    }

    /*
     * check if this is the last expected reply for this batched request
     * and we got all negative replies till now
     * or this is a positive reachable reply
     *
    */
    if (((request->num == 0) || reachable_reply) && !request->reachable)
    {
        my_loc.reset(new po6::net::location(G->myloc));
        request->reachable |= reachable_reply;
        msg->prep_reachable_rep(prev_req_id,
            reachable_reply,
            prev_reach_node,
            std::move(my_loc));
        if (prev_loc == G->myloc)
        { 
            //no need to send msg over network
            std::unique_ptr<db::thread::unstarted_thread> t;
            t.reset(new db::thread::unstarted_thread(handle_reachable_reply, G, std::move(msg)));
            thread_pool.add_request(std::move(t));
        } else {
            G->send(prev_loc, msg->buf);
        }
    }

    if (request->num == 0)
    {
        //delete visited property
        std::vector<size_t>::iterator node_iter;
        std::vector<size_t> src_nodes = request->src_nodes;
        for (node_iter = src_nodes.begin(); node_iter < src_nodes.end(); node_iter++)
        {
            std::vector<db::element::meta_element>::iterator iter;
            n = (db::element::node *)(*node_iter);
            G->remove_visited(n, my_batch_req_id);
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
        //implicitly deleting batch request
        assert(request.use_count() == 1);
        return;
    }
    request->mutex.unlock();
} 

// server loop for the shard server
void
runner(db::graph *G)
{
    busybee_returncode ret;
    po6::net::location sender(SHARD_IPADDR, COORD_PORT);
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    message::message msg(message::ERROR);
    std::unique_ptr<db::thread::unstarted_thread> thr;
    std::unique_ptr<std::thread> t;

    uint32_t loop_count = 0;
    while (1)
    {
        if ((ret = G->bb_recv.recv(&sender, &msg.buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg.reset(new message::message(msg));
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        switch (mtype)
        {
            case message::NODE_CREATE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_create_node, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::NODE_DELETE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_delete_node, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::EDGE_CREATE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_create_edge, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;
            
            case message::REACHABLE_PROP:
                thr.reset(new db::thread::unstarted_thread(handle_reachable_request, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::REACHABLE_REPLY:
                thr.reset(new db::thread::unstarted_thread(handle_reachable_reply, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            default:
                std::cerr << "unexpected msg type " << code << std::endl;
        }

    }

}

int
main(int argc, char* argv[])
{
    if (argc != 2) 
    {
        std::cerr << "Usage: " << argv[0] << " <order> " << std::endl;
        return -1;
    }

    std::cout << "Testing Weaver" << std::endl;
    
    order = atoi(argv[1]);
    port = COORD_PORT + order;
    outgoing_req_id_counter = 0;
    incoming_req_id_counter = 0;

    db::graph G(SHARD_IPADDR, port);
    
    runner(&G);

    return 0;
} //end main

#endif
