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
#include <e/buffer.h>
#include "busybee_constants.h"

#include "common/weaver_constants.h"
#include "graph.h"
#include "common/message.h"

// create a graph node
void
handle_create_node(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    uint64_t creat_time;
    db::element::node *n;
    msg->unpack_node_create(&req_id, &creat_time);

    G->wait_for_updates(creat_time - 1);
    n = G->create_node(creat_time);

    msg->prep_node_create_ack(req_id, (size_t)n); 
    G->send_coord(msg->buf);
}

// delete a graph node
void
handle_delete_node(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    db::element::node *n; // node to be deleted
    size_t node_addr; // temp var to hold node handle
    uint64_t del_time; // time of deletion
    std::unique_ptr<std::vector<size_t>> cached_req_ids;
    msg->unpack_node_delete(&req_id, &node_addr, &del_time);

    n = (db::element::node *)node_addr;
    G->wait_for_updates(del_time - 1);
    cached_req_ids = std::move(G->delete_node(n, del_time));

    msg->prep_node_delete_ack(req_id, std::move(cached_req_ids));
    G->send_coord(msg->buf);
}

// create a graph edge
void
handle_create_edge(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    size_t n1;
    std::unique_ptr<common::meta_element> n2;
    uint64_t creat_time;
    db::element::edge *e;
    msg->unpack_edge_create(&req_id, &n1, &n2, &creat_time);

    G->wait_for_updates(creat_time - 1);
    e = G->create_edge((void *)n1, std::move(n2), creat_time);

    msg->prep_edge_create_ack(req_id, (size_t)e);
    G->send_coord(msg->buf);
}

// delete a graph edge
void
handle_delete_edge(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    db::element::node *n; // node whose edge is being deleted
    db::element::edge *e;
    size_t node_addr, edge_addr;
    uint64_t del_time;
    std::unique_ptr<std::vector<size_t>> cached_req_ids;
    msg->unpack_edge_delete(&req_id, &node_addr, &edge_addr, &del_time);

    n = (db::element::node *)node_addr;
    e = (db::element::edge *)edge_addr;
    G->wait_for_updates(del_time - 1);
    cached_req_ids = std::move(G->delete_edge(n, e, del_time));

    msg->prep_edge_delete_ack(req_id, std::move(cached_req_ids));
    G->send_coord(msg->buf);
}

// add edge property
void
handle_add_edge_property(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    db::element::node *n;
    db::element::edge *e;
    size_t node_addr, edge_addr;
    std::unique_ptr<common::property> new_prop;
    uint64_t prop_add_time;
    msg->unpack_add_prop(&req_id, &node_addr, &edge_addr, &new_prop, &prop_add_time);

    n = (db::element::node *)node_addr;
    e = (db::element::edge *)edge_addr;
    G->wait_for_updates(prop_add_time - 1);
    G->add_edge_property(n, e, std::move(new_prop), prop_add_time);
}

// delete all edge properties with the given key
void
handle_delete_edge_property(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    db::element::node *n;
    db::element::edge *e;
    size_t node_addr, edge_addr;
    uint32_t key;
    uint64_t prop_del_time;
    msg->unpack_del_prop(&req_id, &node_addr, &edge_addr, &key, &prop_del_time);

    n = (db::element::node *)node_addr;
    e = (db::element::edge *)edge_addr;
    G->wait_for_updates(prop_del_time - 1);
    G->delete_all_edge_property(n, e, key, prop_del_time);
}

// reachability request starting from src_nodes to dest_node
void
handle_reachable_request(db::graph *G, std::unique_ptr<message::message> msg)
{
    int prev_loc, //previous server's location
        dest_loc; //target node's location
    size_t dest_node, // destination node handle
        coord_req_id, // central coordinator req id
        prev_req_id, // previous server's req counter
        my_outgoing_req_id, // each forwarded batched req id
        cached_req_id; // if the request is served from cache
    auto edge_props = std::make_shared<std::vector<common::property>>();
    auto vector_clock = std::make_shared<std::vector<uint64_t>>();
    uint64_t myclock_recd;

    db::element::node *n; // node pointer reused for each source node
    bool reached = false, // indicates if we have reached destination node
        propagate_req = false; // need to propagate request onward
    void *reach_node = NULL; // if reached destination, immediate preceding neighbor
    std::unordered_map<int, std::vector<size_t>> msg_batch; // batched messages to propagate
    std::unique_ptr<std::vector<size_t>> src_nodes;
    std::vector<db::element::node *> visited_nodes; // nodes which are visited by this request, in case we need to unmark them
    size_t src_iter, src_end;
    std::unique_ptr<std::vector<size_t>> deleted_nodes(new std::vector<size_t>()); // to send back to requesting shard
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>()); // corresponding to deleted_nodes
    
    // get the list of source nodes to check for reachability, as well as the single sink node
    src_nodes = msg->unpack_reachable_prop(&prev_loc, &dest_node, &dest_loc,
        &coord_req_id, &prev_req_id, &edge_props, &vector_clock, G->myid);
    cached_req_id = coord_req_id;
    
    if (!G->check_request(coord_req_id)) // checking if the request has been handled
    {
    // wait till all updates for this shard arrive
    myclock_recd = vector_clock->at(G->myid);
    G->wait_for_updates(myclock_recd);

    // iterating over src_nodes
    src_iter = 0;
    src_end = src_nodes->size();
    while ((src_iter < src_end) && (!reached)) // traverse local graph as much as possible
    {
        for (; src_iter < src_end; src_iter++)
        {
            // because the coordinator placed the node's address in the message, 
            // we can just cast it back to a pointer
            n = (db::element::node *)(src_nodes->at(src_iter));
            n->update_mutex.lock();
            if (n->get_del_time() <= myclock_recd)
            {
                // trying to traverse deleted node
                deleted_nodes->push_back((size_t)n);
                del_times->push_back(n->get_del_time());
            } else if (n == (db::element::node *)dest_node && G->myid == dest_loc) {
                reached = true;
                reach_node = (void *)n;
            } else if (!G->mark_visited(n, coord_req_id)) {
                size_t temp_cache = G->get_cache((size_t)n, dest_loc, dest_node);
                visited_nodes.push_back(n);
                if (temp_cache != 0) {
                    // cached +ve result
                    reached = true;
                    reach_node = (void *)n;
                    cached_req_id = temp_cache;
                    std::cout << "Serving request from cache\n";
                } else {
                    std::vector<db::element::edge *>::iterator iter;
                    // check the properties of each out-edge
                    for (iter = n->out_edges.begin(); iter < n->out_edges.end(); iter++)
                    {
                        db::element::edge *e = *iter;
                        uint64_t nbrclock_recd = vector_clock->at(e->nbr->get_loc());
                        bool traverse_edge = e->get_creat_time() <= myclock_recd  
                            && e->get_del_time() > myclock_recd // edge created and deleted in acceptable timeframe
                            && e->nbr->get_del_time() > nbrclock_recd; // nbr not deleted
                        size_t i;
                        for (i = 0; i < edge_props->size() && traverse_edge; i++) // checking edge properties
                        {
                            if (!e->has_property(edge_props->at(i)))
                            {
                                traverse_edge = false;
                                break;
                            }
                        }
                        if (traverse_edge)
                        {
                            // Continue propagating reachability request
                            if (e->nbr->get_loc() == G->myid) {
                                src_nodes->push_back((size_t)e->nbr->get_addr());
                            } else {
                                propagate_req = true;
                                msg_batch[e->nbr->get_loc()].push_back((size_t)e->nbr->get_addr());
                            }
                        }
                    }
                }
            }
            n->update_mutex.unlock();
            if (reached)
            {
                break;
            }
        }
        src_end = src_nodes->size();
    }
    } else {
        std::cout << "Request " << coord_req_id << " killed\n";
    }
    
    //send messages
    if (reached)
    {   
        std::vector<db::element::node *>::iterator node_iter;
        // need to send back ack
        msg->prep_reachable_rep(prev_req_id, true, (size_t)reach_node,
            G->myid, std::move(deleted_nodes), std::move(del_times), cached_req_id);
        G->send(prev_loc, msg->buf);
        // telling everyone this request is done
        G->add_done_request(coord_req_id);
        G->broadcast_done_request(coord_req_id);
        // unmark just visited nodes
        for (node_iter = visited_nodes.begin(); node_iter < visited_nodes.end(); node_iter++)
        {
            n = *node_iter;
            n->update_mutex.lock();
            G->remove_visited(n, coord_req_id);
            n->update_mutex.unlock();
        }
    } else if (propagate_req) {
        // the destination is not reachable locally in one hop from this server, so
        // now we have to contact all the reachable neighbors and see if they can reach
        std::unordered_map<int, std::vector<size_t>>::iterator loc_iter;
        auto request = std::make_shared<db::batch_request>(); //c++11 auto magic
        //need mutex since there can be multiple replies
        //for same outstanding req
        request->mutex.lock();
        request->num = 0;
        for (loc_iter = msg_batch.begin(); loc_iter != msg_batch.end(); loc_iter++)
        {
            msg.reset(new message::message(message::REACHABLE_PROP));
            //adding this as a pending request
            request->num++;
            //request in the message
            G->outgoing_req_id_counter_mutex.lock();
            my_outgoing_req_id = G->outgoing_req_id_counter++;
            G->pending_batch[my_outgoing_req_id] = request;
            G->outgoing_req_id_counter_mutex.unlock();
            msg->prep_reachable_prop(&loc_iter->second, G->myid, dest_node, 
                dest_loc, coord_req_id, my_outgoing_req_id, edge_props, vector_clock);
            // no local messages possible, so have to send via network
            G->send(loc_iter->first, msg->buf);
        }
        request->coord_id = coord_req_id;
        request->prev_loc = prev_loc;
        request->prev_id = prev_req_id;
        request->src_nodes = std::move(src_nodes);
        request->dest_addr = dest_node;
        request->dest_loc = dest_loc;
        // store deleted nodes for sending back in reachable reply
        request->del_nodes = std::move(deleted_nodes);
        request->del_times = std::move(del_times);
        assert(request->del_nodes->size() == request->del_times->size());
        request->mutex.unlock();
        msg_batch.clear();
    } else {
        //need to send back nack
        std::vector<db::element::node *>::iterator node_iter;
        msg->prep_reachable_rep(prev_req_id, false, 0, G->myid,
            std::move(deleted_nodes), std::move(del_times), cached_req_id);
        G->send(prev_loc, msg->buf);
        // unmark just visited nodes
        for (node_iter = visited_nodes.begin(); node_iter < visited_nodes.end(); node_iter++)
        {
            n = *node_iter;
            n->update_mutex.lock();
            G->remove_visited(n, coord_req_id);
            n->update_mutex.unlock();
        }
    }
}

// handle reply for a previously forwarded reachability request
// if this is the first positive or last negative reply, propagate to previous
// server immediately;
// otherwise wait for other replies
void
handle_reachable_reply(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t my_outgoing_req_id, prev_req_id, cached_req_id;
    std::shared_ptr<db::batch_request> request;
    bool reachable_reply;
    int prev_loc, reach_loc;
    size_t reach_node, prev_reach_node;
    db::element::node *n;
    std::unique_ptr<std::vector<size_t>> del_nodes(new std::vector<size_t>());
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>());
    size_t num_del_nodes;

    msg->unpack_reachable_rep(&my_outgoing_req_id, &reachable_reply, &reach_node,
        &reach_loc, &num_del_nodes, &del_nodes, &del_times, &cached_req_id);
    assert(num_del_nodes == del_nodes->size());
    G->outgoing_req_id_counter_mutex.lock();
    request = std::move(G->pending_batch[my_outgoing_req_id]);
    G->pending_batch.erase(my_outgoing_req_id);
    G->outgoing_req_id_counter_mutex.unlock();
    request->mutex.lock();
    --request->num;
    prev_loc = request->prev_loc;
    prev_req_id = request->prev_id;
    
    if (reachable_reply || num_del_nodes > 0)
    {   
        // caching positive result
        // also deleting edges for nodes that have been deleted
        std::vector<size_t>::iterator node_iter;
        for (node_iter = request->src_nodes->begin(); node_iter < request->src_nodes->end(); node_iter++)
        {
            std::vector<db::element::edge *>::iterator iter;
            n = (db::element::node *)(*node_iter);
            n->update_mutex.lock();
            for (iter = n->out_edges.begin(); iter < n->out_edges.end(); iter++)
            {
                db::element::edge *e = *iter;
                // deleting edges
                if (num_del_nodes > 0)
                {
                    for (size_t i = 0; i < num_del_nodes; i++)
                    {
                        if (e->nbr->get_addr() == (void*)del_nodes->at(i))
                        {
                            e->nbr->update_del_time(del_times->at(i));
                        }
                    }
                }
                // caching
                if (reachable_reply && !request->reachable)
                {
                    if ((e->nbr->get_addr() == (void *)reach_node) &&
                        (e->nbr->get_loc() == reach_loc))
                    {
                        G->add_cache((size_t)n, request->dest_loc, request->dest_addr, cached_req_id);
                        prev_reach_node = (size_t)n;
                        break;
                    }
                }
            }
            n->update_mutex.unlock();
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
        request->reachable |= reachable_reply;
        msg->prep_reachable_rep(prev_req_id, reachable_reply, prev_reach_node,
            G->myid, std::move(request->del_nodes), std::move(request->del_times), cached_req_id);
        // would never have to send locally
        G->send(prev_loc, msg->buf);
    }

    if (request->num == 0)
    {
        //delete visited property
        std::vector<size_t>::iterator node_iter;
        std::unique_ptr<std::vector<size_t>> src_nodes = std::move(request->src_nodes);
        for (node_iter = src_nodes->begin(); node_iter < src_nodes->end(); node_iter++)
        {
            std::vector<common::meta_element>::iterator iter;
            n = (db::element::node *)(*node_iter);
            n->update_mutex.lock();
            G->remove_visited(n, request->coord_id);
            n->update_mutex.unlock();
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
        //implicitly deleting batch request as shared_ptr refcnt drops to 0
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
    size_t done_id;

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
                G->thread_pool.add_request(std::move(thr), true);
                break;

            case message::NODE_DELETE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_delete_node, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr), true);
                break;

            case message::EDGE_CREATE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_create_edge, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr), true);
                break;
            
            case message::EDGE_DELETE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_delete_edge, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr), true);
                break;

            case message::EDGE_ADD_PROP:
                thr.reset(new db::thread::unstarted_thread(handle_add_edge_property, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr), true);
                break;

            case message::EDGE_DELETE_PROP:
                thr.reset(new db::thread::unstarted_thread(handle_delete_edge_property, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr), true);
                break;

            case message::REACHABLE_PROP:
                thr.reset(new db::thread::unstarted_thread(handle_reachable_request, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::REACHABLE_REPLY:
                thr.reset(new db::thread::unstarted_thread(handle_reachable_reply, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::REACHABLE_DONE:
                rec_msg->unpack_done_request(&done_id);
                G->add_done_request(done_id);
                break;

            default:
                std::cerr << "unexpected msg type " << code << std::endl;
        }
    }
}

int
main(int argc, char* argv[])
{
    if (argc != 4)
    {
        std::cerr << "Usage: " << argv[0] << " <myid> <ipaddr> <port> " << std::endl;
        return -1;
    }
    db::graph G(atoi(argv[1]) - 1, argv[2], atoi(argv[3]));
    std::cout << "Weaver: shard instance " << G.myid << std::endl;
    runner(&G);

    return 0;
} 

#endif
