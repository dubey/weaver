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
#include <unordered_set>
#include <po6/net/location.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#include "common/weaver_constants.h"
#include "graph.h"
#include "common/message.h"

// Ensures that the nodes pointed to in the edge list of n have not been deleted since the edge was made.
void
refresh_node_neighbors(db::graph *G, db::element::node *n, std::vector<uint64_t> &vector_clock)
{
    uint64_t myclock_recd = vector_clock[G->myid];
    std::unordered_map<int, std::vector<size_t>> nbrs;

    G->wait_for_updates(myclock_recd);

    // Build list of non-deleted nodes to check for each shard
    for (db::element::edge *e : n->out_edges)
    {
        if (e->get_del_time() == MAX_TIME) { // need to check more than this? {
            nbrs[e->nbr->get_loc()].push_back((size_t)e->nbr->get_addr());
        } 
    }
    size_t responses_left = nbrs.size();
    db::refresh_request req(responses_left, n);

    std::vector<size_t> *local_nodes = NULL;
    for (std::pair<const int, std::vector<size_t>> &p : nbrs)
    {
        if (p.first == G->myid) {
            // neightbors on local machine, check these after all messages to neighbors sent
            local_nodes = &p.second;
        } else {
            message::message msg(message::NODE_REFRESH_REQ);
            message::prepare_message(msg, message::NODE_REFRESH_REQ, (size_t) &req, G->myid, vector_clock, p.second);
            G->send(p.first, msg.buf);
        }
    }

    if (local_nodes != NULL) {
        std::vector<std::pair<size_t,uint64_t>> local_deleted_nodes;
        for (size_t node_ptr : *local_nodes){
            db::element::node * node = (db::element::node *) node_ptr;
            if (node->get_del_time() < MAX_TIME){
                local_deleted_nodes.push_back(std::make_pair(node_ptr, node->get_del_time()));
            }
        }
        req.add_response(local_deleted_nodes, G->myid);
    }
    // Wait on condition variable until all responses have been added
    req.wait_on_responses();
}

// Given a list of nodes stored on a shard, returns a list of nodes that have been deleted and their delete times.
void
handle_refresh_request(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t return_req_ptr;
    std::vector<size_t> nodes_to_check;
    std::vector<uint64_t> vector_clock;
    int reply_loc;

    message::unpack_message(*msg, message::NODE_REFRESH_REQ, return_req_ptr, reply_loc, vector_clock, nodes_to_check);
    uint64_t myclock_recd = vector_clock[G->myid];

    G->wait_for_updates(myclock_recd);

    std::vector<std::pair<size_t,uint64_t>> deleted_nodes;
    for (size_t node_ptr : nodes_to_check){
        uint64_t del_time = ((db::element::node *) node_ptr)->get_del_time();
        if (del_time < MAX_TIME){
            deleted_nodes.push_back(std::make_pair(node_ptr, del_time));
        }
    }
    message::prepare_message(*msg, message::NODE_REFRESH_REPLY, return_req_ptr, deleted_nodes, G->myid);
    G->send(reply_loc, msg->buf);
}

// Given a list of nodes that have been deleted and the delete times, give that information to the outstanding refresh request.
void
handle_refresh_response(db::graph *G, std::unique_ptr<message::message> msg){
    size_t refresh_req_ptr;
    int from;
    std::vector<std::pair<size_t,uint64_t>> deleted_nodes;

    message::unpack_message(*msg, message::NODE_REFRESH_REPLY, refresh_req_ptr, deleted_nodes, from);

    db::refresh_request *req = (db::refresh_request *)refresh_req_ptr;
    req->add_response(deleted_nodes, from);
}

// Updates the create time for a list of properties
inline void
change_property_times(std::vector<common::property> &props, uint64_t creat_time)
{
    for (auto &p : props)
    {
        p.creat_time = creat_time;
    }
}


// create a graph node
void
handle_create_node(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    uint64_t creat_time;
    db::element::node *n;
    message::unpack_message(*msg, message::NODE_CREATE_REQ, req_id, creat_time);

    G->wait_for_updates(creat_time - 1);
    n = G->create_node(creat_time);

    message::prepare_message(*msg, message::NODE_CREATE_ACK, req_id, (size_t) n);
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
    message::unpack_message(*msg, message::NODE_DELETE_REQ, req_id, node_addr, del_time);

    n = (db::element::node *)node_addr;
    G->wait_for_updates(del_time - 1);
    cached_req_ids = std::move(G->delete_node(n, del_time));

    message::prepare_message(*msg, message::NODE_DELETE_ACK, req_id, *cached_req_ids);
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
    size_t e;
    msg->unpack_edge_create(&req_id, &n1, &n2, &creat_time);

    G->wait_for_updates(creat_time - 1);
    e = (size_t) G->create_edge(n1, std::move(n2), creat_time);

    message::prepare_message(*msg, message::EDGE_CREATE_ACK, req_id, e);
    G->send_coord(msg->buf);
}

// create a back pointer for an edge
void
handle_create_reverse_edge(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t local_node, remote_node;
    int remote_loc;
    message::unpack_message(*msg, message::REVERSE_EDGE_CREATE, remote_node, remote_loc, local_node);

    G->create_reverse_edge(local_node, remote_node, remote_loc);
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
    message::unpack_message(*msg, message::EDGE_DELETE_REQ, req_id, node_addr, edge_addr, del_time);

    n = (db::element::node *)node_addr;
    e = (db::element::edge *)edge_addr;
    G->wait_for_updates(del_time - 1);
    cached_req_ids = std::move(G->delete_edge(n, e, del_time));

    message::prepare_message(*msg, message::EDGE_DELETE_ACK, req_id, *cached_req_ids);
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
    std::unique_ptr<std::vector<size_t>> cached_req_ids;
    message::unpack_message(*msg, message::EDGE_DELETE_PROP, req_id, node_addr,
            edge_addr, key, prop_del_time);

    n = (db::element::node *)node_addr;
    e = (db::element::edge *)edge_addr;
    G->wait_for_updates(prop_del_time - 1);
    cached_req_ids = G->delete_all_edge_property(n, e, key, prop_del_time);

    message::prepare_message(*msg, message::EDGE_DELETE_PROP_ACK, req_id, *cached_req_ids);
    G->send_coord(msg->buf);
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
    std::vector<size_t> visited_nodes; // nodes which are visited by this request, in case we need to unmark them
    size_t src_iter, src_end;
    std::unique_ptr<std::vector<size_t>> deleted_nodes(new std::vector<size_t>()); // to send back to requesting shard
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>()); // corresponding to deleted_nodes
    std::vector<size_t> ignore_cache; // invalid cached ids
    
    // get the list of source nodes to check for reachability, as well as the single sink node
    src_nodes = msg->unpack_reachable_prop(&prev_loc, &dest_node, &dest_loc,
        &coord_req_id, &prev_req_id, &edge_props, &vector_clock, &ignore_cache, G->myid);
    cached_req_id = coord_req_id;

    // invalidating stale cache entries
    for (size_t i = 0; i < ignore_cache.size(); i++)
    {
        G->remove_cache(ignore_cache[i]);
    }
    
    auto request = std::make_shared<db::batch_request>(); //c++11 auto magic
    //need mutex since there can be multiple replies
    //for same outstanding req
    request->mutex.lock();
    
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
#ifdef DEBUG
                G->req_count[coord_req_id]++;
#endif
                size_t temp_cache = G->get_cache((size_t)n, dest_loc, dest_node);
                visited_nodes.push_back((size_t)n);
                // need to check whether the cached_req_id is for a request
                // which is BEFORE this request, so as to ensure we are not
                // using parts of the graph that are not yet created
                // deleted graph elements are handled at the coordinator
                if (temp_cache < coord_req_id && temp_cache > 0) {
                    // cached +ve result
                    reached = true;
                    reach_node = (void *)n;
                    cached_req_id = temp_cache;
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
                                std::vector<size_t> *loc_nodes = &(msg_batch[e->nbr->get_loc()]);
                                propagate_req = true;
                                loc_nodes->push_back((size_t)e->nbr->get_addr());
                                
                                if (loc_nodes->size() > 500) {
                                    // propagating request because
                                    // 1. increase parallelism
                                    // 2. Busybee cannot handle extremely large messages
                                    G->propagate_request(loc_nodes, request, e->nbr->get_loc(),
                                        dest_node, dest_loc, coord_req_id, edge_props, vector_clock, ignore_cache);
                                    loc_nodes->clear();
                                }
                               
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
        // Request killed because dest already reached
        // send back bogus reply in nack
    }
   
    // record just visited nodes for deletion later 
    G->record_visited(coord_req_id, visited_nodes);
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
        request->reachable = true; 
        deleted_nodes.reset(new std::vector<size_t>());
        del_times.reset(new std::vector<uint64_t>());
    } else if (propagate_req) {
        // the destination is not reachable locally in one hop from this server, so
        // now we have to contact all the reachable neighbors and see if they can reach
        std::unordered_map<int, std::vector<size_t>>::iterator loc_iter;
        std::vector<size_t> to_pack;
        // caution: Busybee cannot send very large messages. If either the
        // vector of src nodes or the vector of cache ignores becomes very
        // large, the msg will be dropped with no error printed.
#ifdef DEBUG
        std::cout << "Count for request " << coord_req_id << " is " << G->req_count[coord_req_id] << std::endl;
#endif
        for (loc_iter = msg_batch.begin(); loc_iter != msg_batch.end(); loc_iter++)
        {
            if (loc_iter->second.size() != 0) {
                G->propagate_request(&loc_iter->second, request, loc_iter->first,
                    dest_node, dest_loc, coord_req_id, edge_props, vector_clock, ignore_cache);
            }
        }
        msg_batch.clear();
    } else {
        //need to send back nack
        std::vector<db::element::node *>::iterator node_iter;
        msg->prep_reachable_rep(prev_req_id, false, 0, G->myid,
            std::move(deleted_nodes), std::move(del_times), cached_req_id);
        G->send(prev_loc, msg->buf);
    }
    if (deleted_nodes) {
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
    }
    request->mutex.unlock();
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

    if (!request->src_nodes) {
        return;
    }
    
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
                        if (cached_req_id == request->coord_id) {
                            G->add_cache((size_t)n, request->dest_loc, request->dest_addr, cached_req_id);
                        } else {
                            G->transient_add_cache((size_t)n, request->dest_loc, request->dest_addr, cached_req_id);
                        }
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
        /*
        std::vector<size_t>::iterator node_iter;
        std::unique_ptr<std::vector<size_t>> src_nodes = std::move(request->src_nodes);
        for (node_iter = src_nodes->begin(); node_iter < src_nodes->end(); node_iter++)
        {
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
        }
        */
        //implicitly deleting batch request as shared_ptr refcnt drops to 0
        return;
    }
    request->mutex.unlock();
}

// update cache based on confirmations for transient cached values
// and invalidations for stale entries
void
handle_cache_update(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::vector<size_t> good, bad;
    message::message sendmsg(message::CACHE_UPDATE_ACK);
    msg->unpack_cache_update(&good, &bad);
    
    // invalidations
    for (size_t i = 0; i < bad.size(); i++)
    {
        G->remove_cache(bad[i]);
    }

    // confirmations
    for (size_t i = 0; i < good.size(); i++)
    {
        G->commit_cache(good[i]);
    }
    
    sendmsg.prep_done_cache();
    G->send_coord(sendmsg.buf);
}

// Adds to numerator of a clustering coefficent and replies to coordinator and frees request if all responses have been recieved
inline void
add_clustering_response(db::graph *G, db::clustering_request *request, size_t to_add, std::unique_ptr<message::message> msg)
{
    request->mutex.lock();
    request->edges += to_add;
    request->responses_left--;

    bool kill = (request->responses_left == 0);
    request->mutex.unlock();

    // clustering request finished
    if (kill)
    {
        message::prepare_message(*msg, message::CLUSTERING_REPLY, request->id, request->edges, request->possible_edges);
        G->send_coord(msg->buf);
        delete request;
    }
}

// Used in a clustering request, finds cardinality of  intersection of the neighbors of n with a given map (of shard id to nodes on that shard) of nodes to check
inline size_t
find_num_valid_neighbors(db::element::node *n, std::unordered_map<int, std::unordered_set<size_t>> &nbrs,
        std::vector<common::property> &edge_props, uint64_t myclock_recd, std::vector<uint64_t> &vector_clock)
{
    size_t num_valid_nbrs = 0;
    for(db::element::edge *e : n->out_edges)
    {
        if (nbrs.count(e->nbr->get_loc()) > 0 && nbrs[e->nbr->get_loc()].count((size_t) e->nbr->get_addr()) > 0){
            uint64_t nbrclock_recd = vector_clock[e->nbr->get_loc()];
            bool use_edge = e->get_creat_time() <= myclock_recd  
                && e->get_del_time() > myclock_recd // edge created and deleted in acceptable timeframe
                && e->nbr->get_del_time() > nbrclock_recd; // nbr not deleted
            if (use_edge){
                for (common::property &prop : edge_props){
                    if (!e->has_property(prop)){
                        use_edge = false;
                        break;
                    }
                }
                if (use_edge)
                {
                    num_valid_nbrs++;
                }
            }
        }
    }
    return num_valid_nbrs;
}

// Calculates the local clustering coefficient for a given node
void
handle_clustering_request(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t node_ptr;
    db::element::node *main_node; // pointer to node to calc clustering coefficient for
    db::clustering_request *request = new db::clustering_request(); // will be deleted by add_clustering_response once all replies are recieved
    std::vector<common::property> edge_props;
    std::vector<uint64_t> vector_clock;
    int total_nbrs = 0;

    message::unpack_message(*msg, message::CLUSTERING_REQ, node_ptr, request->id, edge_props, vector_clock);
    main_node = (db::element::node *)node_ptr;

    uint64_t myclock_recd = vector_clock[G->myid];
    // TODO pass clock into has_property
    change_property_times(edge_props, myclock_recd);

    // wait till all updates for this shard arrive
    G->wait_for_updates(myclock_recd);
    // made sure neighbors have not been deleted
    refresh_node_neighbors(G, main_node, vector_clock);

    assert(main_node->get_del_time() > myclock_recd);
    std::vector<db::element::edge *>::iterator iter;
    // check the properties of each out-edge
    for (iter = main_node->out_edges.begin(); iter < main_node->out_edges.end(); iter++)
    {
        db::element::edge *e = *iter;
        uint64_t nbrclock_recd = vector_clock[e->nbr->get_loc()];
        bool use_edge = e->get_creat_time() <= myclock_recd  
            && e->get_del_time() > myclock_recd // edge created and deleted in acceptable timeframe
            && e->nbr->get_del_time() > nbrclock_recd; // nbr not deleted
        for (size_t i = 0; i < edge_props.size() && use_edge; i++) // checking edge properties
        {
            if (!e->has_property(edge_props[i])) {
                use_edge = false;
                break;
            }
        }
        if (use_edge) {
            total_nbrs++;
            (request->nbrs)[e->nbr->get_loc()].insert((size_t)e->nbr->get_addr());
        }
    }

    request->possible_edges = total_nbrs*(total_nbrs-1); // denominator of coefficient

    if (total_nbrs <= 1) { // cant compute coeff for this few neighbors
        request->responses_left = 1;
        add_clustering_response(G, request, 0, std::move(msg));
        return;
    } else {
        request->responses_left = request->nbrs.size();
    }

    for (std::pair<const int, std::unordered_set<size_t>> &p : request->nbrs)
    {
        if (p.first == G->myid) {   
            // calculation for neighbors on local machine below
        } else {
            msg.reset(new message::message(message::CLUSTERING_PROP));
            message::prepare_message(*msg, message::CLUSTERING_PROP, (size_t)request, G->myid, edge_props, vector_clock, request->nbrs);
            G->send(p.first, msg->buf);
        }
    }
    // compute for local neighbors after we have sent batches to other shards
    if (request->nbrs.count(G->myid) > 0) {
        size_t nbr_count = 0;
        for (size_t node_ptr : request->nbrs[G->myid])
        {
            nbr_count += find_num_valid_neighbors((db::element::node *) node_ptr, request->nbrs, edge_props, myclock_recd, vector_clock);
        }
        add_clustering_response(G, request, nbr_count, std::move(msg));
    }
}

// Given a map with neighbors of the node of a clustering request, 
// calculate for each neighbor on this machine the number of other neighbors it is connected to
void
handle_clustering_prop(db::graph *G, std::unique_ptr<message::message> msg){
    size_t return_req_ptr;
    std::unordered_map<int, std::unordered_set<size_t>> nbrs;
    std::vector<uint64_t> vector_clock;
    std::vector<common::property> edge_props;
    int reply_loc;

    message::unpack_message(*msg, message::CLUSTERING_PROP, return_req_ptr, reply_loc, edge_props, vector_clock, nbrs);

    uint64_t myclock_recd = vector_clock[G->myid];
    change_property_times(edge_props, myclock_recd);

    size_t nbr_count = 0;
    for (size_t node_ptr : nbrs[G->myid])
    {
        nbr_count += find_num_valid_neighbors((db::element::node *) node_ptr, nbrs, edge_props, myclock_recd, vector_clock);
    }

    msg.reset(new message::message(message::CLUSTERING_PROP_REPLY));
    message::prepare_message(*msg, message::CLUSTERING_PROP_REPLY, return_req_ptr, nbr_count);
    G->send(reply_loc, msg->buf);
}

// Add a response to a clustering prop to the associated request
void
handle_clustering_prop_reply(db::graph *G, std::unique_ptr<message::message> msg)
{
    db::clustering_request *req;
    size_t req_ptr;
    size_t to_add;

    message::unpack_message(*msg, message::CLUSTERING_PROP_REPLY, req_ptr, to_add);    

    req = (db::clustering_request *)req_ptr;
    add_clustering_response(G, req, to_add, std::move(msg));
}

// Receive and place a node which has been migrated to this shard
void
handle_node_migrate(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t request;
    int from_loc;
    // create a new node, unpack the message
    db::element::node *n = new db::element::node();
    // TODO change edges to that they are hash map for each node
    // change at coordinator so that user does not have to enter node for edge
    message::unpack_message(*msg, message::MIGRATE_NODE_SYN, *n, from_loc, request);
    message::prepare_message(*msg, message::MIGRATE_NODE_SYNACK, ((size_t)n), request);
    G->send(from_loc, msg->buf);
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
    size_t migr_node, request;
    db::migrate_request *mreq;
    size_t done_id;
    uint64_t other_clock, my_clock;

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

            case message::REVERSE_EDGE_CREATE:
                thr.reset(new db::thread::unstarted_thread(handle_create_reverse_edge, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr), true);
            
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

            case message::CACHE_UPDATE:
                thr.reset(new db::thread::unstarted_thread(handle_cache_update, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::CLUSTERING_REQ:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_request, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::CLUSTERING_PROP:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_prop, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::CLUSTERING_PROP_REPLY:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_prop_reply, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::NODE_REFRESH_REQ:
                thr.reset(new
                db::thread::unstarted_thread(handle_refresh_request, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::NODE_REFRESH_REPLY:
                thr.reset(new
                db::thread::unstarted_thread(handle_refresh_response, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;

            case message::MIGRATE_NODE_SYN:
                thr.reset(new db::thread::unstarted_thread(handle_node_migrate, G, std::move(rec_msg)));
                G->thread_pool.add_request(std::move(thr));
                break;
            
            case message::MIGRATE_NODE_SYNACK:
                message::unpack_message(*rec_msg, message::MIGRATE_NODE_SYNACK, migr_node, request);
                mreq = (db::migrate_request *)request;
                mreq->mutex.lock();
                mreq->node_handle = migr_node;
                mreq->created_node = true;
                mreq->cond.signal();
                mreq->mutex.unlock();
                break;

            case message::COORD_NODE_MIGRATE_ACK:
                message::unpack_message(*rec_msg, message::COORD_NODE_MIGRATE_ACK, request, other_clock, my_clock);
                mreq = (db::migrate_request *)request;
                mreq->mutex.lock();
                mreq->other_clock = other_clock;
                mreq->my_clock = my_clock;
                mreq->informed_coord = true;
                mreq->cond.signal();
                mreq->mutex.unlock(); 
                break;

            default:
                std::cerr << "unexpected msg type " << (message::CLIENT_REPLY ==
                code) << std::endl;
        }
    }
}

// migrate node n to shard
void
migrate_node(db::graph *G, db::element::node *n, int shard)
{
    n->update_mutex.lock();
    // pack entire node info in a ginormous message
    message::message msg(message::MIGRATE_NODE);
    db::migrate_request *request = new db::migrate_request();
    request->mutex.lock();
    n->migr_request = request;
    //TODO check msg packing
    message::prepare_message(msg, message::MIGRATE_NODE, *n, G->myid, ((size_t)request));
    n->in_transit = true;
    n->update_mutex.unlock();
    G->send(shard, msg.buf);
    while (!request->created_node)
    {
        request->cond.wait();
    }
    // send migration information to coord = new node and edge handles
    message::prepare_message(msg, message::COORD_NODE_MIGRATE, n->id, shard, request->migr_node, G->myid, ((size_t)request));
    G->send_coord(msg.buf);
    while (!request->informed_coord)
    {
        request->cond.wait();
    }
    // coord has incremented clock at dest shard, so that the queued updates are applied first
    // wait for updates till the received clock value
    G->wait_for_arrived_updates(request->my_clock);
    // increment local clock to above value
    G->sync_clocks();
    // forward queued update requests
    message::prepare_message(msg, message::COORD_MIGRATE_ACK, request->pending_updates, shard, request->migr_node);
    // and start informing other shards of migration
    // beware of edges to nodes that may themselves be migrating - XXX simplifying assumption - only 1 shard migrates at a time?
    // henceforth traversal requests should be refused and pointed to new loc
    n->update_mutex.lock();
    // ...

    // XXX wait for some time for transient requests to arrive
    // delete the node!
}

// delete old visited properties
// TODO delete old done_requests
void
shard_daemon(db::graph *G)
{
    std::unordered_map<size_t, std::vector<size_t>> *next_map;
    while(true)
    {
        std::chrono::seconds duration(40); // execution frequency in seconds
        std::this_thread::sleep_for(duration);
        // deleting visited props
        G->visited_mutex.lock();
        if (G->visit_map) {
            next_map = &G->visit_map_odd;
            G->visit_map = false;
        } else {
            next_map = &G->visit_map_even;
            G->visit_map = true;
        }
        for (auto it = next_map->begin(); it != next_map->end(); it++)
        {
            size_t req_id = it->first;
            for (auto vec_it = it->second.begin(); vec_it != it->second.end(); vec_it++)
            {
                db::element::node *n = (db::element::node *)*vec_it;
                n->update_mutex.lock();
                G->remove_visited(n, req_id);
                n->update_mutex.unlock();
            }
        }
        next_map->clear();
        G->visited_mutex.unlock();
    }
}

int
main(int argc, char* argv[])
{
    std::thread *t;
    if (argc != 4)
    {
        std::cerr << "Usage: " << argv[0] << " <myid> <ipaddr> <port> " << std::endl;
        return -1;
    }
    db::graph G(atoi(argv[1]) - 1, argv[2], atoi(argv[3]));
    std::cout << "Weaver: shard instance " << G.myid << std::endl;

    t = new std::thread(&shard_daemon, &G);
    t->detach();
    runner(&G);

    return 0;
} 

#endif
