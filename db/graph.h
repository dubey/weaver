/*
 * ===============================================================
 *    Description:  The part of a graph stored on a particular 
                    server
 *
 *        Created:  Tuesday 16 October 2012 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __GRAPH__
#define __GRAPH__

#include <stdlib.h>
#include <vector>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <po6/net/location.h>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>
#include <busybee_sta.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"
#include "cache/cache.h"
#include "threadpool/threadpool.h"

namespace db
{
    // Pending batched request
    class batch_request
    {
        public:
        int prev_loc; // prev server's id
        size_t coord_id; // coordinator's req id
        size_t prev_id; // prev server's req id
        int num; // number of onward requests
        bool reachable;
        std::unique_ptr<std::vector<size_t>> src_nodes;
        size_t dest_addr; // dest node's handle
        int dest_loc; // dest node's server id
        std::unique_ptr<std::vector<size_t>> del_nodes; // deleted nodes
        std::unique_ptr<std::vector<uint64_t>> del_times; // delete times corr. to del_nodes
        po6::threads::mutex mutex;

        batch_request()
        {
            prev_id = 0;
            num = 0;
            reachable = false;
            dest_addr = 0;
        }

        batch_request(const batch_request &tup)
        {
            prev_loc = tup.prev_loc;
            prev_id = tup.prev_id;
            num = tup.num;
            reachable = tup.reachable;
            src_nodes.reset(new std::vector<size_t>(*tup.src_nodes));
            dest_addr = tup.dest_addr;
            dest_loc = tup.dest_loc;
        }
    };

    // Pending clustering request
    class clustering_request
    {
        public:
            int coordinator_loc;
            size_t id; // coordinator's req id
            // key is shard, value is set of neighbors on that shard
            std::unordered_map<int, std::unordered_set<size_t>> nbrs; 
            size_t edges; // numerator in calculation
            size_t possible_edges; // denominator in calculation
            size_t responses_left; // 1 response per shard with neighbor
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
            refresh_request(size_t num_shards, db::element::node *n);
        private:
            po6::threads::mutex finished_lock;
            po6::threads::cond finished_cond;
        public:
            db::element::node *node;
            size_t responses_left; // 1 response per shard with neighbor
            bool finished;


        void wait_on_responses()
        {
            finished_lock.lock();
            while (!finished)
            {
                finished_cond.wait();
            }

            finished_lock.unlock();
        }

        // updates the delete times for a node's neighbors based on a refresh response
        void add_response(std::vector<std::pair<size_t, uint64_t>> &deleted_nodes, int from_loc)
        {
            node->update_mutex.lock();
            for (std::pair<size_t,uint64_t> &p : deleted_nodes)
            {
                for (db::element::edge *e : node->out_edges)
                {
                    if (from_loc == e->nbr->get_loc() && p.first == (size_t) e->nbr->get_addr()) {
                        e->nbr->update_del_time(p.second);
                    }
                }
            }
            node->update_mutex.unlock();
            finished_lock.lock();
            responses_left--;
            if (responses_left == 0) {
                finished =  true;
                finished_cond.broadcast();
            }
            finished_lock.unlock();
        }
    };

    refresh_request :: refresh_request(size_t num_shards, db::element::node * n)
        : finished_cond(&finished_lock)
        , node(n)
        , responses_left(num_shards)
        , finished(false)
    {
    }

    class graph
    {
        public:
            graph(int my_id, const char* ip_addr, int port);

        private:
            po6::threads::mutex update_mutex, request_mutex;
            uint64_t my_clock, my_arrived_clock;
            po6::threads::cond pending_update_cond;
            po6::threads::cond pending_update_arrival_cond;

        public:
            int node_count;
            int num_shards;
            std::shared_ptr<po6::net::location> myloc;
            std::shared_ptr<po6::net::location> coord;
            po6::net::location **shard_locs; // po6 locations for each shard
            cache::reach_cache cache; // reachability cache
            busybee_sta bb; // Busybee instance used for sending messages
            busybee_sta bb_recv; // Busybee instance used for receiving messages
            po6::threads::mutex bb_lock; // Busybee lock
            int myid;
            // For reachability requests
            size_t outgoing_req_id_counter;
            po6::threads::mutex outgoing_req_id_counter_mutex, visited_mutex;
            std::unordered_map<size_t, std::shared_ptr<batch_request>> pending_batch;
            // TODO need clean up of old done requests
            std::unordered_set<size_t> done_requests; // requests which need to be killed
            db::thread::pool thread_pool;
            std::unordered_map<size_t, std::vector<size_t>> visit_map_odd, visit_map_even; // visited ids -> nodes
            bool visit_map;
            std::unordered_map<size_t, uint64_t> req_count; // testing
            
        public:
            element::node* create_node(uint64_t time, bool migrate);
            std::pair<bool, element::edge*> create_edge(size_t n1, std::unique_ptr<common::meta_element> n2, uint64_t time);
            bool create_reverse_edge(size_t local_node, size_t remote_node, int remote_loc);
            std::pair<bool, std::unique_ptr<std::vector<size_t>>> delete_node(element::node *n, uint64_t del_time);
            std::pair<bool, std::unique_ptr<std::vector<size_t>>> delete_edge(element::node *n, element::edge *e, uint64_t del_time);
            void refresh_edge(element::node *n, element::edge *e, uint64_t del_time);
            bool add_edge_property(element::node *n, element::edge *e,
                std::unique_ptr<common::property> prop, uint64_t time);
            std::pair<bool, std::unique_ptr<std::vector<size_t>>> delete_all_edge_property(element::node *n, element::edge *e, uint32_t key, uint64_t time);
            bool check_request(size_t req_id);
            void add_done_request(size_t req_id);
            void broadcast_done_request(size_t req_id); 
            bool mark_visited(element::node *n, size_t req_counter);
            void remove_visited(element::node *n, size_t req_counter);
            void record_visited(size_t coord_req_id, const std::vector<size_t>& nodes);
            size_t get_cache(size_t local_node, size_t dest_loc, size_t dest_node);
            void add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_id);
            void transient_add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_id);
            void remove_cache(size_t req_id);
            void commit_cache(size_t req_id);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(po6::net::location *loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(int loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            void wait_for_updates(uint64_t recd_clock);
            void wait_for_arrived_updates(uint64_t recd_clock);
            void sync_clocks();
            void propagate_request(std::vector<size_t> *nodes, std::shared_ptr<batch_request> request, 
                int prop_loc, size_t dest_node, int dest_loc, size_t coord_req_id, 
                std::shared_ptr<std::vector<common::property>> edge_props,
                std::shared_ptr<std::vector<uint64_t>> vector_clock, const std::vector<size_t>& ignore_cache);
    };

    inline
    graph :: graph(int my_id, const char* ip_addr, int port)
        : my_clock(0)
        , pending_update_cond(&update_mutex)
        , pending_update_arrival_cond(&update_mutex)
        , node_count(0)
        , num_shards(NUM_SHARDS)
        , myloc(new po6::net::location(ip_addr, port))
        , coord(new po6::net::location(COORD_IPADDR, COORD_REC_PORT))
        , cache(NUM_SHARDS)
        , bb(myloc->address, myloc->port + SEND_PORT_INCR, 0)
        , bb_recv(myloc->address, myloc->port, 0)
        , myid(my_id)
        , outgoing_req_id_counter(0)
        , thread_pool(NUM_THREADS)
        , visit_map(true)
    {
        int i, inport;
        po6::net::location *temp_loc;
        std::string ipaddr;
        std::ifstream file(SHARDS_DESC_FILE);
        shard_locs = (po6::net::location **)malloc(sizeof(po6::net::location *) * num_shards);
        i = 0;
        while (file >> ipaddr >> inport)
        {
            temp_loc = new po6::net::location(ipaddr.c_str(), inport);
            shard_locs[i] = temp_loc;
            ++i;
        }
        file.close();
    }

    inline element::node*
    graph :: create_node(uint64_t time, bool migrate = false)
    {
        element::node* new_node = new element::node(myloc, time);
        if (!migrate) {
            update_mutex.lock();
            my_clock++;
            my_arrived_clock++;
            assert(my_clock == time);
            pending_update_cond.broadcast();
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
        }
#ifdef DEBUG
        std::cout << "Creating node, addr = " << (void*) new_node 
                << " and node count " << (++node_count) << std::endl;
#endif
        return new_node;
    }

    inline std::pair<bool, element::edge*>
    graph :: create_edge(size_t n1, std::unique_ptr<common::meta_element> n2, uint64_t time)
    {
        std::pair<bool, element::edge*> ret;
        element::node *local_node = (element::node *)n1;
        local_node->update_mutex.lock();
        if (local_node->in_transit) {
            local_node->migr_request->mutex.lock();
            while (!local_node->migr_request->informed_coord)
            {
                local_node->migr_request->cond.wait();
            }
            local_node->migr_request->num_pending_updates++;
            local_node->migr_request->mutex.unlock();
            local_node->update_mutex.unlock();
            update_mutex.lock();
            my_arrived_clock++;
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            ret.first = false;
        } else {
            size_t remote_node = (size_t)n2->get_addr();
            int remote_loc = n2->get_loc();
            //XXX Can we make new edge pointer rather than reuse metaelement, and avoid duplication of time?
            // Also does element need to store location?
            element::edge *new_edge = new element::edge(myloc, time, std::move(n2));
            local_node->add_edge(new_edge, true);
            local_node->update_mutex.unlock();
            update_mutex.lock();
            my_clock++; // TODO check first if the node is in transit
            my_arrived_clock++;
            assert(my_clock == time);
            pending_update_cond.broadcast();
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            //XXX is this ok? What invariant?
            message::message msg(message::REVERSE_EDGE_CREATE);
            message::prepare_message(msg, message::REVERSE_EDGE_CREATE, n1, myid, remote_node);
            send(remote_loc, msg.buf);
#ifdef DEBUG
            std::cout << "Creating edge, addr = " << (void *) new_edge << std::endl;
#endif
            ret.first = true;
            ret.second = new_edge;
        }
        return ret;
    }

    inline bool
    graph :: create_reverse_edge(size_t local_node, size_t remote_node, int remote_loc)
    {
        std::unique_ptr<common::meta_element> remote;
        element::node *n = (element::node *)local_node;
        n->update_mutex.lock();
        if (n->in_transit) {
            n->migr_request->mutex.lock();
            while (!n->migr_request->informed_coord)
            {
                n->migr_request->cond.wait();
            }
            n->migr_request->num_pending_updates++;
            n->migr_request->mutex.unlock();
            n->update_mutex.unlock();
        } else {
            remote.reset(new common::meta_element(remote_loc, 0, 0, (void*)remote_node));
            element::edge *new_edge = new element::edge(myloc, my_clock, std::move(remote));
            n->add_edge(new_edge, false);
            n->update_mutex.unlock();
        }
    }

    inline std::pair<bool, std::unique_ptr<std::vector<size_t>>>
    graph :: delete_node(element::node *n, uint64_t del_time)
    {
        std::pair<bool, std::unique_ptr<std::vector<size_t>>> ret;
        n->update_mutex.lock();
        if (n->in_transit) {
            n->migr_request->mutex.lock();
            while (!n->migr_request->informed_coord)
            {
                n->migr_request->cond.wait();
            }
            n->migr_request->num_pending_updates++;
            ret.second = std::move(n->purge_cache());
            n->migr_request->mutex.unlock();
            n->update_mutex.unlock();
            update_mutex.lock();
            my_arrived_clock++;
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            ret.first = false;
        } else {
            n->update_del_time(del_time);
            ret.second = std::move(n->purge_cache());
            n->update_mutex.unlock();
            update_mutex.lock();
            my_clock++;
            my_arrived_clock++;
            assert(my_clock == del_time);
            pending_update_cond.broadcast();
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            ret.first = true;
        }
        return ret;
    }

    inline std::pair<bool, std::unique_ptr<std::vector<size_t>>>
    graph :: delete_edge(element::node *n, element::edge *e, uint64_t del_time)
    {
        std::pair<bool, std::unique_ptr<std::vector<size_t>>> ret;
        n->update_mutex.lock();
        if (n->in_transit) {
            while (!n->migr_request->informed_coord)
            {
                n->migr_request->cond.wait();
            }
            n->migr_request->num_pending_updates++;
            ret.second = std::move(n->purge_cache());
            n->update_mutex.unlock();
            update_mutex.lock();
            my_arrived_clock++;
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            ret.first = false;
        } else {
            e->update_del_time(del_time);
            ret.second = std::move(n->purge_cache());
            n->update_mutex.unlock();
            update_mutex.lock();
            my_clock++;
            my_arrived_clock++;
            assert(my_clock == del_time);
            pending_update_cond.broadcast();
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            ret.first = true;
        }
        return ret;
    }

    inline void
    graph :: refresh_edge(element::node *n, element::edge *e, uint64_t del_time)
    {
        // TODO caching and edge refreshes
        n->update_mutex.lock();
        e->update_del_time(del_time);
        n->update_mutex.unlock();
    }

    inline bool
    graph :: add_edge_property(element::node *n, element::edge *e,
        std::unique_ptr<common::property> prop, uint64_t time)
    {
        n->update_mutex.lock();
        if (n->in_transit) {
            n->migr_request->mutex.lock();
            while (!n->migr_request->informed_coord)
            {
                n->migr_request->cond.wait();
            }
            n->migr_request->num_pending_updates++;
            n->migr_request->mutex.unlock();
            n->update_mutex.unlock();
            update_mutex.lock();
            my_arrived_clock++;
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            return false;
        } else {
            e->add_property(*prop);
            n->update_mutex.unlock();
            update_mutex.lock();
            my_clock++;
            my_arrived_clock++;
            assert(my_clock == time);
            pending_update_cond.broadcast();
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            return true;
        }
    }

    inline std::pair<bool, std::unique_ptr<std::vector<size_t>>>
    graph :: delete_all_edge_property(element::node *n, element::edge *e, uint32_t key, uint64_t time)
    {
        std::pair<bool, std::unique_ptr<std::vector<size_t>>> ret;
        n->update_mutex.lock();
        if (n->in_transit) {
            while (!n->migr_request->informed_coord)
            {
                n->migr_request->cond.wait();
            }
            n->migr_request->num_pending_updates++;
            ret.second = std::move(n->purge_cache());
            n->update_mutex.unlock();
            update_mutex.lock();
            my_arrived_clock++;
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            ret.first = false;
        } else {
            e->delete_property(key, time);
            ret.second = std::move(n->purge_cache());
            n->update_mutex.unlock();
            update_mutex.lock();
            my_clock++;
            my_arrived_clock++;
            assert(my_clock == time);
            pending_update_cond.broadcast();
            pending_update_arrival_cond.broadcast();
            update_mutex.unlock();
            ret.first = true;
        }
    }

    inline bool
    graph :: check_request(size_t req_id)
    {
        bool ret;
        request_mutex.lock();
        ret = (done_requests.find(req_id) != done_requests.end());
        request_mutex.unlock();
        return ret;
    }

    inline void
    graph :: add_done_request(size_t req_id)
    {
        request_mutex.lock();
        done_requests.insert(req_id);
        request_mutex.unlock();
    }

    inline void
    graph :: broadcast_done_request(size_t req_id)
    {
        int i;
        message::message msg(message::REACHABLE_DONE);
        for (i = 0; i < num_shards; i++)
        {
            msg.prep_done_request(req_id);
            if (i == myid) {
                continue;
            }
            send(i, msg.buf);
        }
    }

    inline bool
    graph :: mark_visited(element::node *n, size_t req_counter)
    {
        return n->check_and_add_seen(req_counter);
    }

    inline void
    graph :: remove_visited(element::node *n, size_t req_counter)
    {
        n->remove_seen(req_counter);
    }

    inline void 
    graph :: record_visited(size_t coord_req_id, const std::vector<size_t>& nodes)
    {
        visited_mutex.lock();
        if (visit_map) {
            visit_map_even[coord_req_id].insert(visit_map_even[coord_req_id].end(), nodes.begin(), nodes.end());
        } else {
            visit_map_odd[coord_req_id].insert(visit_map_odd[coord_req_id].end(), nodes.begin(), nodes.end());
        }
        visited_mutex.unlock();
    }
    
    inline size_t 
    graph :: get_cache(size_t local_node, size_t dest_loc, size_t dest_node)
    {
        return cache.get_req_id(dest_loc, dest_node, local_node);
    }

    inline void 
    graph :: add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_id)
    {
        element::node *n = (element::node *)local_node;
        if (cache.insert_entry(dest_loc, dest_node, local_node, req_id)) {
            n->add_cached_req(req_id);
        }
    }

    inline void
    graph :: transient_add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_id)
    {
        element::node *n = (element::node *)local_node;
        if (cache.transient_insert_entry(dest_loc, dest_node, local_node, req_id)) {
            n->add_cached_req(req_id);
        }
    }

    inline void
    graph :: remove_cache(size_t req_id)
    {
        std::unique_ptr<std::unordered_set<size_t>> caching_nodes1 = std::move(cache.remove_entry(req_id));
        std::unique_ptr<std::unordered_set<size_t>> caching_nodes2 = std::move(cache.remove_transient_entry(req_id));
        if (caching_nodes1) {
            for (auto iter = caching_nodes1->begin(); iter != caching_nodes1->end(); iter++)
            {
                element::node *n = (element::node *)(*iter);
                n->update_mutex.lock();
                n->remove_cached_req(req_id);
                n->update_mutex.unlock();
            }
        } else if (caching_nodes2) {
            for (auto iter = caching_nodes2->begin(); iter != caching_nodes2->end(); iter++)
            {
                element::node *n = (element::node *)(*iter);
                n->update_mutex.lock();
                n->remove_cached_req(req_id);
                n->update_mutex.unlock();
            }
        }
    }

    inline void
    graph :: commit_cache(size_t req_id)
    {
        cache.commit(req_id);
    }

    inline busybee_returncode
    graph :: send(po6::net::location loc, std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(loc, msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
            //assert(ret == BUSYBEE_SUCCESS); //TODO what?
        }
        bb_lock.unlock();
        return ret;
    }
    
    inline busybee_returncode
    graph :: send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(*loc, msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
            //assert(ret == BUSYBEE_SUCCESS); //TODO what?
        }
        bb_lock.unlock();
        return ret;
    }
    
    inline busybee_returncode
    graph :: send(po6::net::location *loc, std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(*loc, msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
            //assert(ret == BUSYBEE_SUCCESS); //TODO what?
        }
        bb_lock.unlock();
        return ret;
    }

    inline busybee_returncode
    graph :: send(int loc, std::auto_ptr<e::buffer> msg)
    {
        if (loc == -1) {
            return send_coord(msg);
        }
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(*shard_locs[loc], msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        bb_lock.unlock();
        return ret;
    }

    inline busybee_returncode
    graph :: send_coord(std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(*coord, msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
            //assert(ret == BUSYBEE_SUCCESS); //TODO
        }
        bb_lock.unlock();
        return ret;
    }

    inline void
    graph :: wait_for_updates(uint64_t recd_clock)
    {
        update_mutex.lock();
        while (recd_clock > my_clock)
        {
            pending_update_cond.wait();
        }
        update_mutex.unlock();
    }

    inline void
    graph :: wait_for_arrived_updates(uint64_t recd_clock)
    {
        update_mutex.lock();
        while (recd_clock > my_arrived_clock)
        {
            pending_update_arrival_cond.wait();
        }
        update_mutex.unlock();
    }

    inline void
    graph :: sync_clocks()
    {
        update_mutex.lock();
        my_clock = my_arrived_clock;
        pending_update_cond.broadcast();
        update_mutex.unlock();
    }
    
    // caution: assuming we hold the request->mutex
    inline void 
    graph :: propagate_request(std::vector<size_t> *nodes, std::shared_ptr<batch_request> request, 
        int prop_loc, size_t dest_node, int dest_loc, size_t coord_req_id, 
        std::shared_ptr<std::vector<common::property>> edge_props,
        std::shared_ptr<std::vector<uint64_t>> vector_clock, const std::vector<size_t>& ignore_cache)
    {
        message::message msg(message::REACHABLE_PROP);
        size_t my_outgoing_req_id;
        //adding this as a pending request
        request->num++;
        //request in the message
        outgoing_req_id_counter_mutex.lock();
        my_outgoing_req_id = outgoing_req_id_counter++;
        pending_batch[my_outgoing_req_id] = request;
        outgoing_req_id_counter_mutex.unlock();
        msg.prep_reachable_prop(nodes, myid, dest_node, dest_loc, 
            coord_req_id, my_outgoing_req_id, edge_props, vector_clock, ignore_cache);
        // no local messages possible, so have to send via network
        send(prop_loc, msg.buf);
    }

} //namespace db

#endif //__GRAPH__
