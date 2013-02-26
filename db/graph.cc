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
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#include "common/weaver_constants.h"
#include "graph.h"
#include "common/message.h"
#include "threadpool/threadpool.h"
#include "db/cache/cache.h" //so we have std::hash override for location


// Pending batched request
class batch_request
{
    public:
    std::unique_ptr<po6::net::location> prev_loc; // prev server's port
    size_t id; // prev server's req id
    int num; // number of onward requests
    bool reachable;
    std::unique_ptr<std::vector<size_t>> src_nodes;
    void *dest_addr; // dest node's handle
    std::shared_ptr<po6::net::location> dest_loc; // dest node's port
    std::unique_ptr<std::vector<size_t>> del_nodes; // deleted nodes
    std::unique_ptr<std::vector<uint64_t>> del_times; // delete times corr. to del_nodes
    po6::threads::mutex mutex;

    batch_request()
    {
        id = 0;
        num = 0;
        reachable = false;
        dest_addr = NULL;
    }

    batch_request(const batch_request &tup)
    {
        prev_loc.reset(new po6::net::location(*tup.prev_loc));
        id = tup.id;
        num = tup.num;
        reachable = tup.reachable;
        src_nodes.reset(new std::vector<size_t>(*tup.src_nodes));
        dest_addr = tup.dest_addr;
        dest_loc.reset(new po6::net::location(*tup.dest_loc));
    }
};

// Pending clustering request
class clustering_request
{
    public:
    po6::net::location coordinator_loc;
    size_t id; // coordinator's req id
    std::unordered_map<po6::net::location, std::unordered_set<size_t>> nbrs;
    size_t edges; // numerator in calculation
    size_t possible_edges;
    size_t responses_left;
    po6::threads::mutex mutex;

    clustering_request()
    {
        edges = 0;
    }
};

// Pending refresh request
class refresh_request
{
    public:
    refresh_request(size_t num_shards, db::element::node * n);
    private:
    po6::threads::mutex finished_lock;
    po6::threads::cond finished_cond;
    public:
    db::element::node *node;
    size_t responses_left;
    bool finished;


    void wait_on_responses(){
        finished_lock.lock();
        while (finished != true){
            finished_cond.wait();
        }

        finished_lock.unlock();
    }

    void add_response(std::vector<std::pair<size_t, uint64_t>> &deleted_nodes,
            po6::net::location from_loc)
    {
        node->update_mutex.lock();
        for (std::pair<size_t,uint64_t> &p : deleted_nodes){
            for (db::element::edge *e : node->out_edges){
                if (from_loc == e->nbr->get_loc() && p.first ==(size_t) e->nbr->get_addr())
                    e->nbr->update_del_time(p.second);
            }
        }
        node->update_mutex.unlock();
        finished_lock.lock();
        responses_left--;
        if (responses_left == 0){
            finished =  true;
            finished_cond.broadcast();
        }
        finished_lock.unlock();
    }
};

    refresh_request::refresh_request(size_t num_shards, db::element::node * n)
    : finished_cond(&finished_lock),
    node(n),
    responses_left(num_shards),
    finished(false)
{
}

static int myid, port;
static size_t incoming_req_id_counter, outgoing_req_id_counter;
static po6::threads::mutex incoming_req_id_counter_mutex, outgoing_req_id_counter_mutex;
static std::unordered_map<size_t, std::shared_ptr<batch_request>> pending_batch;
static db::thread::pool thread_pool(NUM_THREADS);

void
refresh_node_neighbors(db::graph *G, db::element::node *n, std::vector<uint64_t>
        &vector_clock)
{
    uint64_t myclock_recd = vector_clock[myid-1];
    std::unordered_map<po6::net::location, std::vector<size_t>> nbrs;
    G->wait_for_updates(myclock_recd);
    for (db::element::edge *e : n->out_edges){
        bool check_edge = e->get_del_time() == MAX_TIME; //need to check more than this?
        if (check_edge){
            nbrs[*e->nbr->get_loc_ptr()].push_back((size_t)e->nbr->get_addr());
            }
    }
    size_t responses_left = nbrs.size();
    refresh_request req(responses_left, n);
    for (std::pair<const po6::net::location, std::vector<size_t>> &p : nbrs)
    {
        if (p.first == *G->myloc)
        {   //neighbors on local machine
        // later move this to after sending all reqs out
            std::vector<std::pair<size_t,uint64_t>> local_deleted_nodes;
            for (size_t node_ptr : p.second){
                db::element::node *node = (db::element::node *) node_ptr;
                if (node->get_del_time() < MAX_TIME){
                    local_deleted_nodes.push_back(std::make_pair(node_ptr, node->get_del_time()));
                }
            }
            req.add_response(local_deleted_nodes, *G->myloc);
        } else {
            message::message msg(message::NODE_REFRESH_REQ);
            message::prepare_message(msg, message::NODE_REFRESH_REQ, 
                    (size_t) &req, *G->myloc, vector_clock, p.second);
            G->send(p.first, msg.buf);
        }
    }
    req.wait_on_responses();
}

void
handle_refresh_request(db::graph *G, std::unique_ptr<message::message> msg){
    size_t return_req_ptr;
    std::vector<size_t> nodes_to_check;
    std::vector<uint64_t> vector_clock;
    po6::net::location reply_loc;
    message::unpack_message(*msg, message::NODE_REFRESH_REQ, return_req_ptr,
        reply_loc, vector_clock, nodes_to_check);
    uint64_t myclock_recd = vector_clock[myid-1];

    // wait till all updates for this shard arrive
    G->wait_for_updates(myclock_recd);

    std::vector<std::pair<size_t,uint64_t>> deleted_nodes;
    db::element::node *node;
    for (size_t node_ptr : nodes_to_check){
        node = (db::element::node *) node_ptr;
        if (node->get_del_time() < MAX_TIME){
            deleted_nodes.push_back(std::make_pair(node_ptr, node->get_del_time()));
        }
    }
    message::prepare_message(*msg, message::NODE_REFRESH_REPLY, return_req_ptr,
        deleted_nodes, *G->myloc);
    G->send(reply_loc, msg->buf);
}

void
handle_refresh_response(db::graph *G, std::unique_ptr<message::message> msg){
    size_t refresh_req_ptr;
    po6::net::location from;
    std::vector<std::pair<size_t,uint64_t>> deleted_nodes;
    message::unpack_message(*msg, message::NODE_REFRESH_REPLY, refresh_req_ptr,
            deleted_nodes, from);

    refresh_request *req = (refresh_request *) refresh_req_ptr;
    req->add_response(deleted_nodes, from);
}

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
    message::unpack_message(*msg, message::NODE_DELETE_REQ, req_id, node_addr, del_time);

    n = (db::element::node *)node_addr;
    G->wait_for_updates(del_time - 1);
    G->delete_node(n, del_time);

    message::prepare_message(*msg, message::NODE_DELETE_ACK, req_id);
    G->send_coord(msg->buf);
}

// create a graph edge
void
handle_create_edge(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    void *n1;
    std::unique_ptr<common::meta_element> n2;
    uint64_t creat_time;
    size_t e;
    msg->unpack_edge_create(&req_id, &n1, &n2, &creat_time);

    G->wait_for_updates(creat_time - 1);
    e = (size_t) G->create_edge(n1, std::move(n2),
            creat_time);

    message::prepare_message(*msg, message::EDGE_CREATE_ACK, req_id, e);
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
    message::unpack_message(*msg, message::EDGE_DELETE_REQ, req_id, node_addr,
            edge_addr, del_time);

    n = (db::element::node *)node_addr;
    e = (db::element::edge *)edge_addr;
    G->wait_for_updates(del_time - 1);
    G->delete_edge(n, e, del_time);

    message::prepare_message(*msg, message::EDGE_DELETE_ACK, req_id);
    G->send_coord(msg->buf);
}

// add edge property
void
handle_add_edge_property(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t req_id;
    db::element::node *n;
    db::element::edge *e;
    void *node_addr, *edge_addr;
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
    message::unpack_message(*msg, message::EDGE_DELETE_PROP, req_id, node_addr,
            edge_addr, prop_del_time);

    n = (db::element::node *)node_addr;
    e = (db::element::edge *)edge_addr;
    G->wait_for_updates(prop_del_time - 1);
    G->delete_all_edge_property(n, e, key, prop_del_time);
}

// reachability request starting from src_nodes to dest_node
void
handle_reachable_request(db::graph *G, std::unique_ptr<message::message> msg)
{
    void *dest_node; // destination node handle
    std::unique_ptr<po6::net::location> prev_loc; //previous server's location
    std::shared_ptr<po6::net::location> dest_loc; //target node's location
    size_t coord_req_id, // central coordinator req id
        prev_req_id, // previous server's req counter
        my_batch_req_id, // this server's request id
        my_outgoing_req_id; // each forwarded batched req id
    auto edge_props = std::make_shared<std::vector<common::property>>();
    auto vector_clock = std::make_shared<std::vector<uint64_t>>();
    uint64_t myclock_recd;

    db::element::node *n; // node pointer reused for each source node
    bool reached = false; // indicates if we have reached destination node
    void *reach_node = NULL; // if reached destination, immediate preceding neighbor
    bool propagate_req = false; // need to propagate request onward
    std::unordered_map<po6::net::location, std::vector<size_t>> msg_batch; // batched messages to propagate
    std::unique_ptr<std::vector<size_t>> src_nodes;
    std::vector<size_t>::iterator src_iter;
    std::unique_ptr<std::vector<size_t>> deleted_nodes(new std::vector<size_t>());
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>());
        
    // get the list of source nodes to check for reachability, as well as the single sink node
    src_nodes = msg->unpack_reachable_prop(&prev_loc, &dest_node, &dest_loc,
        &coord_req_id, &prev_req_id, &edge_props, &vector_clock, myid);

    // wait till all updates for this shard arrive
    myclock_recd = vector_clock->at(myid-1);
    G->wait_for_updates(myclock_recd);

    // iterating over src_nodes
    for (src_iter = src_nodes->begin(); src_iter < src_nodes->end(); src_iter++)
    {
        static int node_ctr = 0;
        // because the coordinator placed the node's address in the message, 
        // we can just cast it back to a pointer
        n = (db::element::node *)(*src_iter);
        n->update_mutex.lock();
        if (n->get_del_time() <= myclock_recd)
        {
            // trying to traverse deleted node
            deleted_nodes->push_back(*src_iter);
            del_times->push_back(n->get_del_time());
        } else if (n == dest_node && *G->myloc == *dest_loc) {
            reached = true;
            reach_node = (void *)n;
        } else if (!G->mark_visited(n, coord_req_id)) {
            if (n->cache.entry_exists(*dest_loc, dest_node))
            {
                // we have a cached result
                if (n->cache.get_cached_value(*dest_loc, dest_node))
                {
                    // is cached and is reachable
                    reached = true;
                    reach_node = (void *)n;
                } 
            } else {
                std::vector<db::element::edge *>::iterator iter;
		        // check the properties of each out-edge
                for (iter = n->out_edges.begin(); iter < n->out_edges.end(); iter++)
                {
                    db::element::edge *e = *iter;
                    uint64_t nbrclock_recd = vector_clock->at(e->nbr->get_shard_id());
                    bool traverse_edge = e->get_creat_time() <= myclock_recd  
                        && e->get_del_time() > myclock_recd // edge created and deleted in acceptable timeframe
                        && e->nbr->get_del_time() > nbrclock_recd; // nbr not deleted
                    int i;
                    for (i = 0; i < edge_props->size() && traverse_edge; i++) // checking edge properties
                    {
                        if (!e->has_property(edge_props->at(i))) {
                            traverse_edge = false;
                            break;
                        }
                    }
                    if (traverse_edge)
                    {
                        propagate_req = true;
                        // Continue propagating reachability request
                        msg_batch[*e->nbr->get_loc_ptr()].push_back((size_t)e->nbr->get_addr());
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
    
    //send messages
    if (reached)
    {   //need to send back ack
        msg->prep_reachable_rep(prev_req_id, true, (size_t) reach_node,
            G->myloc, std::move(deleted_nodes), std::move(del_times));
        G->send(std::move(prev_loc), msg->buf);
    } else if (propagate_req) {
        // the destination is not reachable locally in one hop from this server, so
        // now we have to contact all the reachable neighbors and see if they can reach
        std::unordered_map<po6::net::location, std::vector<size_t>>::iterator loc_iter;
        auto request = std::make_shared<batch_request>(); //c++11 auto magic
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
            outgoing_req_id_counter_mutex.lock();
            my_outgoing_req_id = outgoing_req_id_counter++;
            pending_batch[my_outgoing_req_id] = request;
            outgoing_req_id_counter_mutex.unlock();
            msg->prep_reachable_prop(&loc_iter->second, G->myloc, (size_t)dest_node, 
                dest_loc, coord_req_id, my_outgoing_req_id, edge_props, vector_clock);
            if (loc_iter->first == *G->myloc)
            {   //no need to send message since it is local
                std::unique_ptr<db::thread::unstarted_thread> t;
                t.reset(new db::thread::unstarted_thread(handle_reachable_request, G, std::move(msg)));
                thread_pool.add_request(std::move(t));
            } else {
                G->send(loc_iter->first, msg->buf);
            }
        }
        request->prev_loc = std::move(prev_loc);
        request->id = prev_req_id;
        request->src_nodes = std::move(src_nodes);
        request->dest_addr = dest_node;
        request->dest_loc = std::move(dest_loc);
        // store deleted nodes for sending back in reachable reply
        request->del_nodes = std::move(deleted_nodes);
        request->del_times = std::move(del_times);
        assert(request->del_nodes->size() == request->del_times->size());
        request->mutex.unlock();
        msg_batch.clear();
    } else {
        //need to send back nack
        msg->prep_reachable_rep(prev_req_id, false, 0, G->myloc,
            std::move(deleted_nodes), std::move(del_times));
        G->send(std::move(prev_loc), msg->buf);
    }
}

// handle reply for a previously forwarded reachability request
// if this is the first positive or last negative reply, propagate to previous
// server immediately;
// otherwise wait for other replies
void
handle_reachable_reply(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t my_outgoing_req_id, my_batch_req_id, prev_req_id;
    std::shared_ptr<batch_request> request;
    bool reachable_reply;
    std::unique_ptr<po6::net::location> prev_loc, reach_loc;
    size_t reach_node, prev_reach_node;
    db::element::node *n;
    std::unique_ptr<std::vector<size_t>> del_nodes(new std::vector<size_t>());
    std::unique_ptr<std::vector<uint64_t>> del_times(new std::vector<uint64_t>());
    size_t num_del_nodes;

    reach_loc = msg->unpack_reachable_rep(&my_outgoing_req_id,
        &reachable_reply, &reach_node,
        &num_del_nodes, &del_nodes, &del_times);
    assert(num_del_nodes == del_nodes->size());
    outgoing_req_id_counter_mutex.lock();
    request = std::move(pending_batch[my_outgoing_req_id]);
    pending_batch.erase(my_outgoing_req_id);
    outgoing_req_id_counter_mutex.unlock();
    request->mutex.lock();
    --request->num;
    prev_loc.reset(new po6::net::location(*request->prev_loc));
    prev_req_id = request->id;
    
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
                /*
                 * TODO turned off for now since deletion is possible
                if (reachable_reply)
                {
                    if ((e->nbr->get_addr() == (void *)reach_node) &&
                        (*e->nbr->get_loc_ptr() == *reach_loc))
                    {
                        n->cache.insert_entry(*request->dest_loc, request->dest_addr, true);
                        prev_reach_node = (size_t)n;
                        break;
                    }
                }
                */
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
            G->myloc, std::move(request->del_nodes), std::move(request->del_times));
        if (*prev_loc == *G->myloc)
        { 
            //no need to send msg over network
            std::unique_ptr<db::thread::unstarted_thread> t;
            t.reset(new db::thread::unstarted_thread(handle_reachable_reply, G, std::move(msg)));
            thread_pool.add_request(std::move(t));
        } else {
            G->send(std::move(prev_loc), msg->buf);
        }
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

inline void
add_clustering_response(db::graph *G, clustering_request *request, size_t to_add, std::unique_ptr<message::message> msg)
{
    request->mutex.lock();
    request->edges += to_add;
    request->responses_left--;

    bool kill = (request->responses_left == 0);
    request->mutex.unlock();
    if (kill)
    {
        message::prepare_message(*msg, message::CLUSTERING_REPLY, request->id,
                request->edges, request->possible_edges);
        G->send(std::move(request->coordinator_loc), msg->buf);
        delete request;
    }
}

inline size_t
find_num_valid_neighbors(db::graph *G, db::element::node *n,
std::unordered_map<po6::net::location, std::unordered_set<size_t>> &nbrs,
std::vector<common::property> &edge_props, uint64_t myclock_recd,
std::vector<uint64_t> &vector_clock)
{
    size_t num_valid_nbrs = 0;
    for(db::element::edge *e : n->out_edges)
    {
        po6::net::location loc = e->nbr->get_loc();
        if (nbrs.count(loc) < 1)
            break;

        size_t node_ptr = (size_t) e->nbr->get_addr();
        if (nbrs[loc].count(node_ptr) > 0){
            uint64_t nbrclock_recd = vector_clock[e->nbr->get_shard_id()];
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

void
handle_clustering_request(db::graph *G, std::unique_ptr<message::message> msg)
{
    size_t node_ptr;
    db::element::node *main_node; // pointer to node to calc clustering coefficient for
    clustering_request *request = new clustering_request();

    std::vector<common::property> edge_props;
    std::vector<uint64_t> vector_clock;
    int total_nbrs = 0;

    message::unpack_message(*msg, message::CLUSTERING_REQ, node_ptr,
            request->coordinator_loc, request->id, edge_props, vector_clock);
    main_node = (db::element::node *) node_ptr;
    uint64_t myclock_recd = vector_clock[myid-1];
    //myclock_recd = vector_clock->at(myid-1);
    change_property_times(edge_props, myclock_recd);

    // wait till all updates for this shard arrive
    G->wait_for_updates(myclock_recd);
    refresh_node_neighbors(G, main_node, vector_clock);

    if (main_node->get_del_time() <= myclock_recd)
    {
        // this node doesn't exist in graph anymore
    }
    std::vector<db::element::edge *>::iterator iter;
    // check the properties of each out-edge
    for (iter = main_node->out_edges.begin(); iter < main_node->out_edges.end(); iter++)
    {
        db::element::edge *e = *iter;
        uint64_t nbrclock_recd = vector_clock[e->nbr->get_shard_id()];
        bool use_edge = e->get_creat_time() <= myclock_recd  
            && e->get_del_time() > myclock_recd // edge created and deleted in acceptable timeframe
            && e->nbr->get_del_time() > nbrclock_recd; // nbr not deleted
        int i;
        for (i = 0; i < edge_props.size() && use_edge; i++) // checking edge properties
        {
            if (!e->has_property(edge_props[i])) {
                use_edge = false;
                break;
            }
        }
        if (use_edge)
        {
            total_nbrs++;
            po6::net::location loc = e->nbr->get_loc();
            void *elem_addr = e->nbr->get_addr();
            (request->nbrs)[loc].insert((size_t) elem_addr);
        }
    }

    request->possible_edges = total_nbrs*(total_nbrs-1);
    if (total_nbrs <= 1) // cant compute coeff
    {
        request->responses_left = 1;
        add_clustering_response(G, request, 0, std::move(msg));
        return;
    }
    else 
    {
        request->responses_left = request->nbrs.size();
    }
    //std::unordered_set<size_t> *local_nbrs;

    for (std::pair<po6::net::location,std::unordered_set<size_t>> p : request->nbrs)
    {
        if (p.first == *G->myloc)
        {   //neighbors on local machine
          //  local_nbrs = &p.second;
        } else {
            msg.reset(new message::message(message::CLUSTERING_PROP));
            message::prepare_message(*msg, message::CLUSTERING_PROP, (size_t)
                    request, *G->myloc, edge_props, vector_clock, request->nbrs);
            G->send(p.first, msg->buf);
        }
    }
    // after we have sent batches to other shards who need to compute,
    // sequentially compute for local neighbors
    //if (local_nbrs != NULL){
    size_t nbr_count = 0;
    if (request->nbrs.count(*G->myloc) > 0)
    {
        for (size_t node_ptr : request->nbrs[*G->myloc])
        {
            nbr_count += find_num_valid_neighbors(G, (db::element::node *) node_ptr,
            request->nbrs, edge_props, myclock_recd, vector_clock);
        }
        add_clustering_response(G, request, nbr_count, std::move(msg));
    }
}

void
handle_clustering_prop(db::graph *G, std::unique_ptr<message::message> msg){
    size_t return_req_ptr;

    std::unordered_map<po6::net::location, std::unordered_set<size_t>> nbrs;
    std::vector<uint64_t> vector_clock;
    std::vector<common::property> edge_props;
    po6::net::location reply_loc;

    message::unpack_message(*msg, message::CLUSTERING_PROP, return_req_ptr,
            reply_loc, edge_props, vector_clock, nbrs);
    uint64_t myclock_recd = vector_clock[myid-1];
    change_property_times(edge_props, myclock_recd);


    size_t nbr_count = 0;
    for (size_t node_ptr : nbrs[*G->myloc])
    {
        nbr_count += find_num_valid_neighbors(G, (db::element::node *) node_ptr,
                nbrs, edge_props, myclock_recd, vector_clock);
    }

    msg.reset(new message::message(message::CLUSTERING_PROP_REPLY));
    message::prepare_message(*msg, message::CLUSTERING_PROP_REPLY, return_req_ptr, nbr_count);
    G->send(std::move(reply_loc), msg->buf);
}

void
handle_clustering_prop_reply(db::graph *G, std::unique_ptr<message::message> msg){
    clustering_request *req;
    size_t req_ptr;
    size_t to_add;

    message::unpack_message(*msg, message::CLUSTERING_PROP_REPLY, req_ptr,
    to_add);    
    req = (clustering_request *) req_ptr;
    add_clustering_response(G, req, to_add, std::move(msg));
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
                thread_pool.add_request(std::move(thr), true);
                break;

            case message::NODE_DELETE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_delete_node, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr), true);
                break;

            case message::EDGE_CREATE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_create_edge, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr), true);
                break;
            
            case message::EDGE_DELETE_REQ:
                thr.reset(new db::thread::unstarted_thread(handle_delete_edge, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr), true);
                break;

            case message::EDGE_ADD_PROP:
                thr.reset(new db::thread::unstarted_thread(handle_add_edge_property, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr), true);
                break;

            case message::EDGE_DELETE_PROP:
                thr.reset(new db::thread::unstarted_thread(handle_delete_edge_property, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr), true);
                break;

            case message::REACHABLE_PROP:
                thr.reset(new db::thread::unstarted_thread(handle_reachable_request, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::REACHABLE_REPLY:
                thr.reset(new db::thread::unstarted_thread(handle_reachable_reply, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::CLUSTERING_REQ:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_request, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::CLUSTERING_PROP:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_prop, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::CLUSTERING_PROP_REPLY:
                thr.reset(new
                db::thread::unstarted_thread(handle_clustering_prop_reply, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::NODE_REFRESH_REQ:
                thr.reset(new
                db::thread::unstarted_thread(handle_refresh_request, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            case message::NODE_REFRESH_REPLY:
                thr.reset(new
                db::thread::unstarted_thread(handle_refresh_response, G, std::move(rec_msg)));
                thread_pool.add_request(std::move(thr));
                break;

            default:
                std::cerr << "unexpected msg type " << (message::CLIENT_REPLY ==
                code) << std::endl;
        }
    }
}

int
main(int argc, char* argv[])
{
    if (argc != 2) 
    {
        std::cerr << "Usage: " << argv[0] << " <myid> " << std::endl;
        return -1;
    }

    std::cout << "Weaver: shard instance " << myid << std::endl;
    
    myid = atoi(argv[1]);
    port = COORD_PORT + myid;
    outgoing_req_id_counter = 0;
    incoming_req_id_counter = 0;

    db::graph G(SHARD_IPADDR, port);
    
    runner(&G);

    return 0;
} //end main

#endif
