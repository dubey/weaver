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

void handle_reachable_request(db::graph *G, void *request);
void handle_dijkstra_prop(db::graph *G, void * request);
void migrate_node_step1(db::graph*, db::element::node*, int);
void migrate_node_step4_1(db::graph *G);
/*
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
*/

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
handle_create_node(db::graph *G, size_t req_id) 
{
    db::element::node *n;
    message::message msg;
    n = G->create_node(req_id);
    size_t node_ptr = (size_t)n;
    G->increment_clock();
    message::prepare_message(msg, message::NODE_CREATE_ACK, req_id, node_ptr);
    G->send_coord(msg.buf);
}

// delete a graph node
void
handle_delete_node(db::graph *G, size_t req_id, size_t node_handle, std::unique_ptr<std::vector<size_t>> cache)
{
    std::pair<bool, std::unique_ptr<std::vector<size_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_node((db::element::node*)node_handle, (uint64_t)req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first) {
        message::prepare_message(*msg, message::NODE_DELETE_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_NODE_DELETE_REQ, req_id, *success.second);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// create a graph edge
void
handle_create_edge(db::graph *G, size_t req_id, size_t n1, size_t n2, int loc2, uint64_t tc2)
{
    std::pair<bool, size_t> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->create_edge(n1, req_id, n2, loc2, tc2);
    if (success.first) {
        message::prepare_message(*msg, message::EDGE_CREATE_ACK, req_id, success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_EDGE_CREATE_REQ, req_id, n2, loc2, tc2);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// create a back pointer for an edge
void
handle_create_reverse_edge(db::graph *G, size_t req_id, size_t remote_node, int remote_loc, size_t local_node)
{
    std::unique_ptr<message::message> msg(new message::message());
    if (!G->create_reverse_edge(req_id, local_node, remote_node, remote_loc)) {
        message::prepare_message(*msg, message::TRANSIT_REVERSE_EDGE_CREATE, req_id, remote_node, remote_loc);
        G->queue_transit_node_update(0, std::move(msg));
    }
}

// delete an edge
void
handle_delete_edge(db::graph *G, size_t req_id, size_t n, size_t e, std::unique_ptr<std::vector<size_t>> cache)
{
    std::pair<bool, std::unique_ptr<std::vector<size_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_edge((db::element::node*)n, e, req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first) {
        message::prepare_message(*msg, message::EDGE_DELETE_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_EDGE_DELETE_REQ, req_id, e, *success.second);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// add edge property
void
handle_add_edge_property(db::graph *G, size_t req_id, size_t node_addr, size_t edge_addr, common::property &prop)
{
    std::unique_ptr<message::message> msg(new message::message());
    if (!G->add_edge_property(node_addr, edge_addr, prop)) {
        message::prepare_message(*msg, message::TRANSIT_EDGE_ADD_PROP, req_id, edge_addr, prop);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// delete all edge properties with the given key
void
handle_delete_edge_property(db::graph *G, size_t req_id, size_t node_addr, size_t edge_addr, uint32_t key,
    std::unique_ptr<std::vector<size_t>> cache)
{
    std::pair<bool, std::unique_ptr<std::vector<size_t>>> success;
    std::unique_ptr<message::message> msg(new message::message());
    success = G->delete_all_edge_property(node_addr, edge_addr, key, req_id);
    if (cache) {
        success.second = std::move(cache);
    }
    if (success.first) {
        message::prepare_message(*msg, message::EDGE_DELETE_PROP_ACK, req_id, *success.second);
        G->send_coord(msg->buf);
    } else {
        message::prepare_message(*msg, message::TRANSIT_EDGE_DELETE_PROP, req_id, edge_addr, key, *success.second);
        G->queue_transit_node_update(req_id, std::move(msg));
    }
}

// reachability request starting from src_nodes to dest_node
void
handle_reachable_request(db::graph *G, void *reqptr)
{
    std::shared_ptr<db::batch_request> request((db::batch_request *) reqptr); // XXX is shared needed?
    size_t cached_req_id = request->coord_id;
    db::element::node *n; // node pointer reused for each source node
    bool reached = false; // indicates if we have reached destination node
    bool propagate_req = false; // need to propagate request onward
    size_t reach_node = 0; // if reached destination, immediate preceding neighbor
    std::vector<size_t> visited_nodes; // nodes which are visited by this request, in case we need to unmark them
    /* these pointers are crazy!
    std::unique_ptr<std::vector<size_t>> visited_nodes(new std::vector<size_t>()); // nodes which are visited by this request, in case we need to unmark them
    std::unique_ptr<std::vector<size_t>> deleted_nodes(new std::vector<size_t>()); // to send back to requesting shard
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>()); // corresponding to deleted_nodes
    */
    std::unordered_map<int, std::vector<size_t>> msg_batch; // batched messages to propagate
    size_t src_iter, src_end;
    message::message msg(message::ERROR);
    
    //need mutex since there can be multiple replies
    //for same outstanding req
    request->lock();
    
    if (!G->check_request(request->coord_id)) // checking if the request has been handled
    {
    // iterating over src_nodes
    src_iter = 0;
    src_end = request->src_nodes.size();
    while ((src_iter < src_end) && (!reached)) // traverse local graph as much as possible
    {
        for (; src_iter < src_end; src_iter++)
        {
            // because the coordinator placed the node's address in the message, 
            // we can just cast it back to a pointer
            n = (db::element::node *)(request->src_nodes.at(src_iter));
            n->update_mutex.lock();
            if (n->get_del_time() <= request->coord_id)
            {
                // trying to traverse deleted node
                request->del_nodes.push_back((size_t)n);
                request->del_times.push_back(n->get_del_time());
            } else if (n == (db::element::node *)request->dest_addr && G->myid == request->dest_loc) {
                reached = true;
                reach_node = (size_t)n;
            } else if (!G->mark_visited(n, request->coord_id)) {
#ifdef DEBUG
                G->req_count[coord_req_id]++;
#endif
                if (n->state == db::element::node::mode::IN_TRANSIT) {
                    // node is being migrated, queue this request
                    request->num++;
                    G->mrequest.mutex.lock();
                    G->mrequest.pending_requests.push_back(request);
                    G->mrequest.mutex.unlock();
                } else if (n->state == db::element::node::mode::MOVED) {
                    std::vector<size_t> migrated_node;
                    migrated_node.emplace_back((size_t)n->new_handle);
                    request->num++;
                    G->mrequest.mutex.lock();
                    G->propagate_request(migrated_node, request, n->new_loc);
                    G->mrequest.mutex.unlock();
                } else { 
                    size_t temp_cache = G->get_cache((size_t)n, request->dest_loc, request->dest_addr, request->edge_props);
                    visited_nodes.emplace_back((size_t)n);
                    // need to check whether the cached_req_id is for a request
                    // which is BEFORE this request, so as to ensure we are not
                    // using parts of the graph that are not yet created
                    // deleted graph elements are handled at the coordinator
                    if (temp_cache < request->coord_id && temp_cache > 0) {
                        // cached +ve result
                        reached = true;
                        reach_node = (size_t)n;
                        cached_req_id = temp_cache;
#ifdef DEBUG
                        std::cout << "Serving from cache, req id " << request->coord_id << ", from this node " << (size_t)n << " " << G->myid
                            << " to dest node " << request->dest_addr << " " << request->dest_loc << ", cached id " << temp_cache << std::endl;
#endif
                    } else {
                        // check the properties of each out-edge
                        for (auto &iter : n->out_edges)
                        {
                            db::element::edge *e = iter.second;
                            bool traverse_edge = e->get_creat_time() <= request->coord_id
                                && e->get_del_time() > request->coord_id; // edge created and deleted in acceptable timeframe
                            size_t i;
                            for (i = 0; i < request->edge_props.size() && traverse_edge; i++) // checking edge properties
                            {
                                if (!e->has_property(request->edge_props.at(i)))
                                {
                                    traverse_edge = false;
                                    break;
                                }
                            }
                            if (traverse_edge)
                            {
                                // Continue propagating reachability request
                                if (e->nbr.loc == G->myid) {
                                    request->src_nodes.emplace_back(e->nbr.handle);
                                    request->parent_nodes.emplace_back(src_iter);
                                    n->msg_count[G->myid]++;
                                } else {
                                    std::vector<size_t> &loc_nodes = msg_batch[e->nbr.loc];
                                    propagate_req = true;
                                    loc_nodes.push_back(e->nbr.handle);
                                    n->msg_count[e->nbr.loc]++;
                                    
                                    if (loc_nodes.size() > MAX_NODE_PER_REQUEST) {
                                        // propagating request because
                                        // 1. increase parallelism
                                        // 2. Busybee cannot handle extremely large messages
                                        request->num++;
                                        G->propagate_request(loc_nodes, request, e->nbr.loc);
                                        loc_nodes.clear();
                                    }
                                   
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
        src_end = request->src_nodes.size();
    }
    } else {
        // Request killed because dest already reached
        // send back bogus reply in nack
    }
   
    // record just visited nodes for deletion later 
    G->record_visited(request->coord_id, visited_nodes);

    /* Now this isn't needed
    if (deleted_nodes) {
        // store deleted nodes for sending back in reachable reply
        request->del_nodes = std::move(deleted_nodes);
        request->del_times = std::move(del_times);
        */
        assert(request->del_nodes.size() == request->del_times.size());
        /*
    }
    */

    //send messages
    if (reached) {
        // need to send back ack
        message::prepare_message(msg, message::REACHABLE_REPLY, request->prev_id, true, reach_node,
            G->myid, request->del_nodes, request->del_times, cached_req_id);
        G->send(request->prev_loc, msg.buf);
        // telling everyone this request is done
        G->add_done_request(request->coord_id);
        G->broadcast_done_request(request->coord_id);
        request->reachable = true; 
    } else if (propagate_req) {
        // the destination is not reachable locally in one hop from this server, so
        // now we have to contact all the reachable neighbors and see if they can reach
        std::unordered_map<int, std::vector<size_t>>::iterator loc_iter;
        std::vector<size_t> to_pack;
        // caution: Busybee cannot send very large messages. If either the
        // vector of src nodes or the vector of cache ignores becomes very
        // large, the msg will be dropped with no error printed.
#ifdef DEBUG
        std::cout << "Count for request " << request->coord_id << " is " << G->req_count[request->coord_id] << std::endl;
#endif
        for (loc_iter = msg_batch.begin(); loc_iter != msg_batch.end(); loc_iter++)
        {
            if (loc_iter->second.size() != 0) {
                request->num++;
                G->propagate_request(loc_iter->second, request, loc_iter->first);
            }
        }
        msg_batch.clear();
    } else {
        //need to send back nack
        message::prepare_message(msg, message::REACHABLE_REPLY, request->prev_id, false, reach_node,
            G->myid, request->del_nodes, request->del_times, cached_req_id);
        G->send(request->prev_loc, msg.buf);
    }
    request->unlock();
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
    int prev_reach_node_pos;
    db::element::node *n;
    std::vector<size_t> del_nodes;
    std::vector<uint64_t> del_times;
    size_t num_del_nodes;

    message::unpack_message(*msg, message::REACHABLE_REPLY, my_outgoing_req_id, reachable_reply,
        reach_node, reach_loc, del_nodes, del_times, cached_req_id);
    num_del_nodes = del_nodes.size();
    G->outgoing_req_id_counter_mutex.lock();
    request = std::move(G->pending_batch.at(my_outgoing_req_id));
    G->pending_batch.erase(my_outgoing_req_id);
    G->outgoing_req_id_counter_mutex.unlock();
    request->lock();
    --request->num;
    prev_loc = request->prev_loc;
    prev_req_id = request->prev_id;

    /* XXX
    if (!request->src_nodes) {
        return;
    }
    */
    
    prev_reach_node = 0;
    prev_reach_node_pos = -1;
    if (reachable_reply || num_del_nodes > 0)
    {   
        // caching positive result
        // also deleting edges for nodes that have been deleted
        for (auto node_iter: request->src_nodes)
        {
            prev_reach_node_pos++;
            n = (db::element::node *)(node_iter);
            n->update_mutex.lock();
            for (auto &iter : n->out_edges)
            {
                db::element::edge *e = iter.second;
                // deleting edges
                if (num_del_nodes > 0)
                {
                    for (size_t i = 0; i < num_del_nodes; i++)
                    {
                        if (e->nbr.handle == del_nodes[i])
                        {
                            e->update_del_time(del_times[i]);
                        }
                    }
                }
                // caching
                if (reachable_reply && !request->reachable)
                {
                    if ((e->nbr.handle == reach_node) &&
                        (e->nbr.loc == reach_loc))
                    {
                        bool traverse_edge = e->get_creat_time() <= request->coord_id
                            && e->get_del_time() > request->coord_id; // edge created and deleted in acceptable timeframe
                        size_t i;
                        for (i = 0; i < request->edge_props.size() && traverse_edge; i++) // checking edge properties
                        {
                            if (!e->has_property(request->edge_props.at(i)))
                            {
                                traverse_edge = false;
                                break;
                            }
                        }
                        if (traverse_edge) {
                            if (cached_req_id == request->coord_id) {
#ifdef DEBUG
                                std::cout << "Adding to cache, req " << request->coord_id << ", from this node " << node_iter << " " << G->myid
                                    << " to dest " << request->dest_addr << " " << request->dest_loc << std::endl;
#endif
                                G->add_cache((size_t)n, request->dest_loc, request->dest_addr, cached_req_id, request->edge_props);
                            } else {
                                G->transient_add_cache((size_t)n, request->dest_loc, request->dest_addr, cached_req_id, request->edge_props);
                            }
                            prev_reach_node = (size_t)n;
                            break;
                        }
                    }
                }
            }
            n->update_mutex.unlock();
        }
        // continue caching
        if (prev_reach_node != 0) {
            bool loop = true;
            db::element::node *n;
            size_t prev_pos = request->parent_nodes.at(prev_reach_node_pos);
            while (loop)
            {
                if (prev_pos == UINT64_MAX) {
                    loop = false;
                } else {
                    prev_reach_node = request->src_nodes.at(prev_pos);
                    n = (db::element::node*)prev_reach_node;
                    n->update_mutex.lock();
                    if (cached_req_id == request->coord_id) {
#ifdef DEBUG
                        std::cout << "Adding to cache, req " << request->coord_id << ", from this node " << node_iter << " " << G->myid
                            << " to dest " << request->dest_addr << " " << request->dest_loc << std::endl;
#endif
                        G->add_cache((size_t)n, request->dest_loc, request->dest_addr, cached_req_id, request->edge_props);
                    } else {
                        G->transient_add_cache((size_t)n, request->dest_loc, request->dest_addr, cached_req_id, request->edge_props);
                    }
                    n->update_mutex.unlock();
                    prev_pos = request->parent_nodes.at(prev_pos);
                }
            }
        }
    }

    // check if this is the last expected reply for this batched request
    // and we got all negative replies till now
    // or this is a positive reachable reply
    if (((request.use_count() == 1) || reachable_reply) && !request->reachable)
    {
        request->reachable |= reachable_reply;
        message::prepare_message(*msg, message::REACHABLE_REPLY, prev_req_id, reachable_reply, prev_reach_node,
            G->myid, request->del_nodes, request->del_times, cached_req_id);
        // would never have to send locally
        G->send(prev_loc, msg->buf);
    }

    if (request->num == 0)
    {
        //implicitly deleting batch request as shared_ptr refcnt drops to 0
        //return;
    }
    request->unlock();
}

// unpack and create an object for recording traversal state
void
unpack_traversal_request(db::graph *G, std::unique_ptr<message::message> msg)
{    
    /* This is what the templates were supposed to avoid!
    int prev_loc, //previous server's location
        dest_loc; //target node's location
    size_t dest_node, // destination node handle
        coord_req_id, // central coordinator req id
        prev_req_id; // previous server's req counter
    std::unique_ptr<std::vector<size_t>> src_nodes(new std::vector<size_t>());
    std::shared_ptr<std::vector<common::property>> edge_props(new std::vector<common::property>());
    std::unique_ptr<std::vector<uint64_t>> vector_clock(new std::vector<uint64_t>());
    std::unique_ptr<std::vector<size_t>> ignore_cache(new std::vector<size_t>()); // invalid cached ids
    */

    db::batch_request *req = new db::batch_request();

    // get the list of source nodes to check for reachability, as well as the single sink node
    message::unpack_message(*msg, message::REACHABLE_PROP, req->vector_clock, req->src_nodes, req->prev_loc, req->dest_addr, req->dest_loc,
        req->coord_id, req->prev_id, req->edge_props, req->ignore_cache);


    // invalidating stale cache entries
    for (auto &remove: req->ignore_cache)
    {
        G->remove_cache(remove);
    }

    // leftover stuff from constructor
    req->start_time = req->vector_clock.at(G->myid);
    req->parent_nodes.assign(req->src_nodes.size(), UINT64_MAX);
std::cout << "adding a traversal reqeuest" << std::endl;
    db::thread::unstarted_thread *trav_req = new db::thread::unstarted_thread(req->start_time,  handle_reachable_request, G, req);
    G->thread_pool.add_request(trav_req);
}

// update cache based on confirmations for transient cached values
// and invalidations for stale entries
void
handle_cache_update(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::vector<size_t> good, bad;
    message::unpack_message(*msg, message::CACHE_UPDATE, good, bad);
    
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
   
    message::prepare_message(*msg, message::CACHE_UPDATE_ACK); 
    G->send_coord(msg->buf);
}

/*
template<typename Func>
void apply_to_valid_edges(db::element::node* n, std::vector<common::property>& edge_props, uint64_t myclock_recd, Func func){
    // check the properties of each out-edge
    n->update_mutex.lock();
    for (db::element::edge* e : n->out_edges)
    {
        //uint64_t nbrclock_recd = vector_clock[e->nbr->get_loc()];
        bool use_edge = e->get_creat_time() <= myclock_recd  
            && e->get_del_time() > myclock_recd; // edge created and deleted in acceptable timeframe
           // && e->nbr->get_del_time() > nbrclock_recd; // nbr not deleted XXX this is lazily evaluated right now anyway
        for (size_t i = 0; i < edge_props.size() && use_edge; i++) // checking edge properties
        {
            if (!e->has_property(edge_props[i])) {
                use_edge = false;
                break;
            }
        }
        if (use_edge) {
            func(e);
        }
    }
    n->update_mutex.unlock();
}

void
handle_dijkstra_request(db::graph *G, void *req)
{
    db::path_request *request = (db::path_request *) req;
    message::message msg(message::DIJKSTRA_PROP);
    uint64_t myclock_recd = request->start_time;
    size_t current_cost = 0;

    auto queue_add_fun = [&current_cost, myclock_recd, &request] (db::element::edge * e) {
        std::pair<bool, size_t> weightpair = e->get_property_value(request->edge_weight_name, myclock_recd); // first is whether key exists, second is value
        if (weightpair.first){
            size_t priority = request->is_widest_path ? weightpair.second : weightpair.second + current_cost;
    //        std::cout << "adding priority" << priority << " from edge of weight " << weightpair.second << " and current cost " <<  current_cost << std::endl;
            request->possible_next_nodes.push(db::dijkstra_queue_elem(priority, e->nbr->get_loc(), (size_t) e->nbr->get_addr()));
        }
    };

    while (!request->possible_next_nodes.empty()){
        db::dijkstra_queue_elem next_to_add = request->possible_next_nodes.top();
        request->possible_next_nodes.pop();
        current_cost = next_to_add.cost;
        // we have found destination!
        if (next_to_add.addr == request->dest_ptr && next_to_add.shard_loc == request->dest_loc){
            message::prepare_message(msg, message::DIJKSTRA_REPLY, request->coord_id, true, current_cost);
            G->send_coord(msg.buf);
            delete request;
            return;
        } else if (next_to_add.shard_loc != G->myid) {
            // node to add neighbors for is on another shard
            message::prepare_message(msg, message::DIJKSTRA_PROP, (size_t) request, next_to_add.addr, request->is_widest_path, 
                    request->edge_weight_name, next_to_add.cost, request->edge_props, G->myid, request->coord_id);
            G->send(next_to_add.shard_loc, msg.buf);
            // let thread who handles the response continue the search, request object still in heap and pointer passed in message
            return;
        } else {
            //if a local node
            db::element::node * next_node = (db::element::node *) next_to_add.addr;

            // need to check if node hadn't been deleted. This needed if we don't update in edges for a deleted node
            if (next_node->get_del_time() <= myclock_recd){
                continue;
            }

            auto location_pair = std::make_pair(next_to_add.shard_loc, next_to_add.addr); 
            if (request->visited_map.count(location_pair) > 0 && request->visited_map[location_pair] <= current_cost){
                continue;
            } else {
                apply_to_valid_edges(next_node, request->edge_props, myclock_recd, queue_add_fun);
                request->visited_map[location_pair] = current_cost; // mark the cost to get here
            }
        }
    }
    // dest couldn't be reached
    message::prepare_message(msg, message::DIJKSTRA_REPLY, request->coord_id, false, 0);
    G->send_coord(msg.buf);
    delete request;
}

// XXX
void
unpack_dijkstra_request(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t source_ptr;

    db::path_request *req= new db::path_request;

    message::unpack_message(*msg, message::DIJKSTRA_REQ, source_ptr, req->dest_ptr, req->dest_loc, 
            req->edge_weight_name, req->coord_id, req->is_widest_path, req->edge_props, req->vector_clock);


    req->start_time = req->vector_clock[G->myid];

    // TODO pass clock into has_property
    change_property_times(req->edge_props, req->start_time);

    req->possible_next_nodes.push(db::dijkstra_queue_elem( 0, G->myid, req->source_ptr)); //no emplace but only 3 words
    db::thread::unstarted_traversal_thread *dijkstra_req = new db::thread::unstarted_traversal_thread(req->coord_id, req->start_time,  handle_dijkstra_request, G, req);
    G->thread_pool.add_traversal_request(dijkstra_req);
}

// XXX
void
unpack_dijkstra_prop(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t source_ptr;

    db::dijkstra_prop *req= new db::dijkstra_prop;

    message::unpack_message(*msg, message::DIJKSTRA_PROP, req->req_ptr, req->node_ptr, req->is_widest_path, 
            req->edge_weight_name, req->current_cost, req->edge_props, req->start_time, req->reply_loc, req->coord_id);

    // TODO pass clock into has_property
    change_property_times(req->edge_props, req->start_Time);

    std::cout << "got a path prop!" << std::endl;

    // TODO pass clock into has_property
    change_property_times(req->edge_props, req->start_time);

    db::thread::unstarted_traversal_thread *dijkstra_req = new db::thread::unstarted_traversal_thread(req->coord_id, req->start_time,  handle_dijkstra_prop, G, req);
    G->thread_pool.add_traversal_request(dijkstra_req);
}


inline void
handle_dijkstra_prop(db::graph *G, void * request)
{
    db::dijkstra_prop * req = (db::dijkstra_prop *) request;
    std::vector<db::dijkstra_queue_elem> entries_to_add;

    db::element::node * next_node = (db::element::node *) req->node_ptr;

    if (next_node->get_del_time() > req->start_time){
        auto list_add_fun = [&req] (db::element::edge * e) {
            std::pair<bool, size_t> weightpair = e->get_property_value(req->edge_weight_name, req->start_time); // first is whether key exists, second is value
            if (weightpair.first){
                size_t priority = req->is_widest_path ? weightpair.second : weightpair.second + req->current_cost;
                entries_to_add.emplace_back(db::dijkstra_queue_elem(priority, e->nbr->get_loc(), (size_t) e->nbr->get_addr()));
            }
        };
        apply_to_valid_edges(next_node, req->edge_props, req->start_time, list_add_fun);
    }
    message::message msg;
    message::prepare_message(msg, message::DIJKSTRA_PROP_REPLY, req->req_ptr, entries_to_add);
    G->send(req->reply_loc, msg.buf);
}

inline void
handle_path_prop_reply(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_ptr;
    std::vector<db::dijkstra_queue_elem> entries_to_add;
    db::path_request *request;

    message::unpack_message(*msg, message::DIJKSTRA_PROP_REPLY, req_ptr, entries_to_add);
    request = (db::path_request *) req_ptr;
    for (auto &elem : entries_to_add){
        request->possible_next_nodes.push(elem);
    }
    // continue the path request from this thread
    handle_dijkstra_req(G, request);
}
*/

/*
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
*/

// send node information to new shard
// mark node as "in_transit" so that subsequent requests are queued up
void
migrate_node_step1(db::graph *G, db::element::node *n, int shard)
{
    G->mrequest.mutex.lock();
    n->update_mutex.lock();
    n->state = db::element::node::mode::IN_TRANSIT;
    n->new_loc = shard;
    // pack entire node info in a ginormous message
    message::message msg(message::MIGRATE_NODE_STEP1);
    G->mrequest.cur_node = n;
    G->mrequest.new_loc = shard;
    G->mrequest.num_pending_updates = 0;
    G->mrequest.my_clock = 0;
    std::vector<std::unique_ptr<message::message>>().swap(G->mrequest.pending_updates);
    std::vector<size_t>().swap(G->mrequest.pending_update_ids);
    std::vector<std::shared_ptr<db::batch_request>>().swap(G->mrequest.pending_requests);
    message::prepare_message(msg, message::MIGRATE_NODE_STEP1, *n, G->myid);
    n->update_mutex.unlock();
    G->mrequest.mutex.unlock();
    G->send(shard, msg.buf);
}

// Receive and place a node which has been migrated to this shard
void
migrate_node_step2(db::graph *G, std::unique_ptr<message::message> msg)
{
    int from_loc;
    // create a new node, unpack the message
    db::element::node *n = G->create_node(0, true);
    // TODO 
    // change at coordinator so that user does not have to enter node for edge
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP1, *n, from_loc);
    n->prev_loc = from_loc;
    size_t nptr = (size_t)n;
    message::prepare_message(*msg, message::MIGRATE_NODE_STEP2, nptr);
    G->send(from_loc, msg->buf);
}

// send migration information to coord
void
migrate_node_step3(db::graph *G, std::unique_ptr<message::message> msg)
{
    db::element::node *n;
    size_t new_node_handle;
    G->mrequest.mutex.lock();
    n = G->mrequest.cur_node;
    n->update_mutex.lock();
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP2, new_node_handle);
    G->mrequest.migr_node = new_node_handle;
    n->new_handle = new_node_handle;
    uint64_t tc = n->get_creat_time();
    message::prepare_message(*msg, message::COORD_NODE_MIGRATE, tc, n->new_loc, new_node_handle, G->myid);
    n->update_mutex.unlock();
    G->send_coord(msg->buf);
    G->mrequest.mutex.unlock();
}

// wait for updates till the received clock value, which are being forwarded to new shard
void
migrate_node_step4(db::graph *G, std::unique_ptr<message::message> msg)
{
    uint64_t my_clock;
    message::unpack_message(*msg, message::COORD_NODE_MIGRATE_ACK, my_clock);
    if (G->set_target_clock(my_clock)) {
        migrate_node_step4_1(G);
    }
}

// forward queued update requests
void
migrate_node_step4_1(db::graph *G)
{
    message::message msg;
    G->mrequest.mutex.lock();
    message::prepare_message(msg, message::MIGRATE_NODE_STEP4, G->mrequest.pending_update_ids);
    G->send(G->mrequest.new_loc, msg.buf);
    for (auto &m: G->mrequest.pending_updates)
    {
        G->send(G->mrequest.new_loc, m->buf);
    }
    G->mrequest.mutex.unlock();
}

// receive the number of pending updates for the newly migrated node
void
migrate_node_step5(db::graph *G, std::unique_ptr<message::message> msg)
{
    std::vector<size_t> update_ids;
    db::element::node *n = G->migr_node;
    message::unpack_message(*msg, message::MIGRATE_NODE_STEP4, update_ids);
    n->update_mutex.lock();
    if (update_ids.empty()) {
        n->state = db::element::node::mode::STABLE;
        message::prepare_message(*msg, message::MIGRATE_NODE_STEP5);
        G->send(n->prev_loc, msg->buf);
        n->update_mutex.unlock();
    } else {
        n->update_mutex.unlock();
        G->set_update_ids(update_ids);
    }
}

// inform other shards of migration
// forward queued traversal requests to new location
void
migrate_node_step6(db::graph *G)
{
    message::message msg(message::REACHABLE_PROP);
    std::vector<size_t> nodes;
    db::element::node *n;
    G->mrequest.mutex.lock();
    n = G->mrequest.cur_node;
    n->update_mutex.lock();
    size_t nptr = (size_t)n;
    n->state = db::element::node::mode::MOVED;
    // inform all in-nbrs of new location
    std::cout << n->in_edges.size() << std::endl;
    for (auto &nbr: n->in_edges)
    {
        message::prepare_message(msg, message::MIGRATED_NBR_UPDATE, nbr.second->nbr.handle, 
            nptr, G->myid, G->mrequest.migr_node, G->mrequest.new_loc);
        G->send(nbr.second->nbr.loc, msg.buf);
    }
    nodes.push_back(G->mrequest.migr_node);
    for (auto &r: G->mrequest.pending_requests)
    {
        r->lock();
        G->propagate_request(nodes, r, G->mrequest.new_loc);
        r->unlock();
    }
    n->update_mutex.unlock();
    G->mrequest.mutex.unlock();
}

void
migrated_nbr_update(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t local_node, orig_node, new_node;
    int orig_loc, new_loc;
    message::unpack_message(*msg, message::MIGRATED_NBR_UPDATE, local_node,
        orig_node, orig_loc, new_node, new_loc);
    G->update_migrated_nbr(local_node, orig_node, orig_loc, new_node, new_loc);
}

// unpack update request and call appropriate function
void
unpack_update_request(db::graph *G, void *req)
{
    db::update_request *request = (db::update_request *) req;
    size_t req_id;
    uint64_t start_time, time2;
    size_t n1, n2, edge;
    int loc;
    common::property prop;
    uint32_t key;
    std::unique_ptr<std::vector<size_t>> cache;

    switch (request->type)
    {
        case message::NODE_CREATE_REQ:
            message::unpack_message(*request->msg, message::NODE_CREATE_REQ, start_time, req_id);
            handle_create_node(G, req_id);
            break;

        case message::NODE_DELETE_REQ:
            message::unpack_message(*request->msg, message::NODE_DELETE_REQ, start_time, req_id, n1);
            handle_delete_node(G, req_id, n1, std::move(cache));
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::EDGE_CREATE_REQ:
            message::unpack_message(*request->msg, message::EDGE_CREATE_REQ, start_time, req_id,
                n1, n2, loc, time2);
            handle_create_edge(G, req_id, n1, n2, loc, time2);
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::REVERSE_EDGE_CREATE:
            message::unpack_message(*request->msg, message::REVERSE_EDGE_CREATE, start_time, req_id, n2, loc, n1);
            handle_create_reverse_edge(G, req_id, n2, loc, n1);
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::EDGE_DELETE_REQ:
            message::unpack_message(*request->msg, message::EDGE_DELETE_REQ, start_time, req_id, n1, edge);
            handle_delete_edge(G, req_id, n1, edge, std::move(cache));
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::EDGE_ADD_PROP:
            message::unpack_message(*request->msg, message::EDGE_ADD_PROP, start_time, req_id, n1, edge, prop);
            handle_add_edge_property(G, req_id, n1, edge, prop);
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::EDGE_DELETE_PROP:
            message::unpack_message(*request->msg, message::EDGE_DELETE_PROP, start_time, req_id, n1, edge, key);
            handle_delete_edge_property(G, req_id, n1, edge, key, std::move(cache));
            if (G->increment_clock()) {
                migrate_node_step4_1(G);
            }
            break;

        case message::REACHABLE_PROP:
            unpack_traversal_request(G, std::move(request->msg));
            break;

        case message::REACHABLE_REPLY:
            handle_reachable_reply(G, std::move(request->msg));
            break;

        case message::CACHE_UPDATE:
            handle_cache_update(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP1:
            migrate_node_step2(G, std::move(request->msg));
            break;
        
        case message::MIGRATE_NODE_STEP2:
            migrate_node_step3(G, std::move(request->msg));
            break;

        case message::COORD_NODE_MIGRATE_ACK:
            migrate_node_step4(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP4:
            migrate_node_step5(G, std::move(request->msg));
            break;

        case message::MIGRATE_NODE_STEP5:
            migrate_node_step6(G);
            break;

        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(G, std::move(request->msg));
            break;

        default:
            std::cerr << "Bad msg type in unpack_update_request" << std::endl;
    }
    delete request;
}

// assuming caller holds migration_lock
void
unpack_transit_update_request(db::graph *G, db::update_request *request)
{
    size_t req_id;
    size_t n2, edge;
    int loc;
    uint64_t time;
    common::property prop;
    uint32_t key;
    std::unique_ptr<std::vector<size_t>> cache;

    switch (request->type)
    {
        case message::TRANSIT_NODE_DELETE_REQ:
            cache.reset(new std::vector<size_t>());
            message::unpack_message(*request->msg, message::TRANSIT_NODE_DELETE_REQ, req_id, *cache);
            handle_delete_node(G, req_id, (size_t)G->migr_node, std::move(cache));
            break;

        case message::TRANSIT_EDGE_CREATE_REQ:
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_CREATE_REQ, req_id, n2, loc, time);
            handle_create_edge(G, req_id, (size_t)G->migr_node, n2, loc, time);
            break;

        case message::TRANSIT_REVERSE_EDGE_CREATE:
            message::unpack_message(*request->msg, message::TRANSIT_REVERSE_EDGE_CREATE, req_id, n2, loc);
            handle_create_reverse_edge(G, req_id, n2, loc, (size_t)G->migr_node);
            break;
        
        case message::TRANSIT_EDGE_DELETE_REQ:
            cache.reset(new std::vector<size_t>());
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_DELETE_REQ, req_id, edge, *cache);
            handle_delete_edge(G, req_id, (size_t)G->migr_node, edge, std::move(cache));
            break;

        case message::TRANSIT_EDGE_ADD_PROP:
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_ADD_PROP, req_id, edge, prop);
            handle_add_edge_property(G, req_id, (size_t)G->migr_node, edge, prop);
            break;

        case message::TRANSIT_EDGE_DELETE_PROP:
            cache.reset(new std::vector<size_t>());
            message::unpack_message(*request->msg, message::TRANSIT_EDGE_DELETE_PROP, req_id, edge, key, *cache);
            handle_delete_edge_property(G, req_id, (size_t)G->migr_node, edge, key, std::move(cache));
            break;

        default:
            std::cerr << "Bad msg type in unpack_transit_update_request" << std::endl;
    }
}

void
process_pending_updates(db::graph *G, void *req)
{
    db::update_request *request = (db::update_request *) req;
    size_t req_id;
    request->msg->buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum message::msg_type)) >> req_id;
    db::update_request *r = new db::update_request(request->type, req_id, std::move(request->msg));
    G->migration_mutex.lock();
    G->pending_updates.push(r);
    while (G->pending_update_ids.front() == G->pending_updates.top()->start_time)
    {
        unpack_transit_update_request(G, G->pending_updates.top());
        G->pending_updates.pop();
        G->pending_update_ids.pop_front();
        if (G->pending_update_ids.empty()) {
            message::message msg;
            G->migr_node->update_mutex.lock();
            G->migr_node->state = db::element::node::mode::STABLE;
            message::prepare_message(msg, message::MIGRATE_NODE_STEP5);
            G->send(G->migr_node->prev_loc, msg.buf);
            G->migr_node->update_mutex.unlock();
            break;
        }
    }
    G->migration_mutex.unlock();
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
    message::message msg;
    db::thread::unstarted_thread *thr;
    db::update_request *request;
    size_t done_id;
    uint64_t start_time;

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
        rec_msg->change_type(mtype);
        switch (mtype)
        {
            case message::NODE_CREATE_REQ:
            case message::NODE_DELETE_REQ:
            case message::EDGE_CREATE_REQ:
            case message::REVERSE_EDGE_CREATE:
            case message::EDGE_DELETE_REQ:
            case message::EDGE_ADD_PROP:
            case message::EDGE_DELETE_PROP:
                rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(mtype)) >> start_time;
                request = new db::update_request(mtype, start_time - 1, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(start_time -1, unpack_update_request, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::TRANSIT_NODE_DELETE_REQ:
            case message::TRANSIT_EDGE_CREATE_REQ:
            case message::TRANSIT_REVERSE_EDGE_CREATE:
            case message::TRANSIT_EDGE_DELETE_REQ:
            case message::TRANSIT_EDGE_ADD_PROP:
            case message::TRANSIT_EDGE_DELETE_PROP:
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, process_pending_updates, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::REACHABLE_REPLY:
            case message::CACHE_UPDATE:
            case message::MIGRATE_NODE_STEP1:
            case message::MIGRATE_NODE_STEP2:
            case message::COORD_NODE_MIGRATE_ACK:
            case message::MIGRATE_NODE_STEP4:
            case message::MIGRATE_NODE_STEP5:
            case message::MIGRATED_NBR_UPDATE:
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, unpack_update_request, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::REACHABLE_PROP:
                request = new db::update_request(mtype, 0, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, unpack_update_request, G, request);
                G->thread_pool.add_request(thr);
                break;

            case message::REACHABLE_DONE:
                message::unpack_message(*rec_msg, message::REACHABLE_DONE, done_id);
                G->add_done_request(done_id);
                break;

            /*
            case message::DIJKSTRA_REQ:
            case message::DIJKSTRA_PROP:
                unpack_dijkstra_request(G, std::move(rec_msg));
                break;

            case message::DIJKSTRA_PROP_REPLY:
                db::thread::unstarted_traversal_thread *dijkstra_prop_reply = new db::thread::unstarted_traversal_thread(0, 0, handle_path_prop_reply, G, std::move(rec_msg));
                G->thread_pool.add_traversal_request(dijkstra_prop_reply);
                break;
                */

/*
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
*/
           default:
                std::cerr << "unexpected msg type " << (message::CLIENT_REPLY ==
                code) << std::endl;
        }
    }
}

// delete old visited properties
// TODO delete old done_requests
void
shard_daemon(db::graph *G)
{
    std::unordered_map<size_t, std::vector<size_t>> *next_map;
    while (true)
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
