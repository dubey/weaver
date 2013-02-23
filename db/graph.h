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
    class bool_wrapper
    {
        public:
            bool bval;
            bool_wrapper() {
                bval = false;
            }
    };

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

    class graph
    {
        public:
            graph(int my_id, const char* ip_addr, int port);

        private:
            po6::threads::mutex update_mutex, request_mutex;
            uint64_t my_clock;
            po6::threads::cond pending_update_cond;

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
            po6::threads::mutex outgoing_req_id_counter_mutex;
            std::unordered_map<size_t, std::shared_ptr<batch_request>> pending_batch;
            // TODO need clean up of old done requests
            std::unordered_map<size_t, bool_wrapper> done_requests; // requests which need to be killed
            db::thread::pool thread_pool;
            
        public:
            element::node* create_node(uint64_t time);
            element::edge* create_edge(void *n1,
                std::unique_ptr<common::meta_element> n2, uint64_t time);
            std::unique_ptr<std::vector<size_t>> delete_node(element::node *n, uint64_t del_time);
            std::unique_ptr<std::vector<size_t>> delete_edge(element::node *n, element::edge *e, uint64_t del_time);
            void add_edge_property(element::node *n, element::edge *e,
                std::unique_ptr<common::property> prop, uint64_t time);
            void delete_all_edge_property(element::node *n, element::edge *e, uint32_t key, uint64_t time);
            bool check_request(size_t req_id);
            void add_done_request(size_t req_id);
            void broadcast_done_request(size_t req_id); 
            bool mark_visited(element::node *n, size_t req_counter);
            void remove_visited(element::node *n, size_t req_counter);
            size_t get_cache(size_t local_node, size_t dest_loc, size_t dest_node);
            void add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_id);
            void remove_cache(size_t req_id);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(po6::net::location *loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(int loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            void wait_for_updates(uint64_t recd_clock);
    };

    inline
    graph :: graph(int my_id, const char* ip_addr, int port)
        : my_clock(0)
        , pending_update_cond(&update_mutex)
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
    graph :: create_node(uint64_t time)
    {
        element::node* new_node = new element::node(myloc, time);
        update_mutex.lock();
        my_clock++;
        assert(my_clock == time);
        pending_update_cond.broadcast();
        update_mutex.unlock();
        
        //std::cout << "Creating node, addr = " << (void*) new_node 
        //        << " and node count " << (++node_count) << std::endl;
        return new_node;
    }

    inline element::edge*
    graph :: create_edge(void *n1,
        std::unique_ptr<common::meta_element> n2,
        uint64_t time)
    {
        element::node *local_node = (element::node *)n1;
        element::edge *new_edge = new element::edge(myloc, time, std::move(n2));
        local_node->update_mutex.lock();
        local_node->add_edge(new_edge);
        local_node->update_mutex.unlock();
        update_mutex.lock();
        my_clock++;
        assert(my_clock == time);
        pending_update_cond.broadcast();
        update_mutex.unlock();

        //std::cout << "Creating edge, addr = " << (void *) new_edge << std::endl;
        return new_edge;
    }

    inline std::unique_ptr<std::vector<size_t>>
    graph :: delete_node(element::node *n, uint64_t del_time)
    {
        std::unique_ptr<std::vector<size_t>> cached_req_ids;
        n->update_mutex.lock();
        n->update_del_time(del_time);
        cached_req_ids = std::move(n->purge_cache());
        n->update_mutex.unlock();
        update_mutex.lock();
        my_clock++;
        assert(my_clock == del_time);
        pending_update_cond.broadcast();
        update_mutex.unlock();
        return cached_req_ids;
    }

    inline std::unique_ptr<std::vector<size_t>>
    graph :: delete_edge(element::node *n, element::edge *e, uint64_t del_time)
    {
        std::unique_ptr<std::vector<size_t>> cached_req_ids;
        n->update_mutex.lock();
        e->update_del_time(del_time);
        cached_req_ids = std::move(n->purge_cache());
        n->update_mutex.unlock();
        update_mutex.lock();
        my_clock++;
        assert(my_clock == del_time);
        pending_update_cond.broadcast();
        update_mutex.unlock();
        return cached_req_ids;
    }

    inline void
    graph :: add_edge_property(element::node *n, element::edge *e,
        std::unique_ptr<common::property> prop, uint64_t time)
    {
        n->update_mutex.lock();
        e->add_property(*prop);
        n->update_mutex.unlock();
        update_mutex.lock();
        my_clock++;
        assert(my_clock == time);
        pending_update_cond.broadcast();
        update_mutex.unlock();
    }

    inline void
    graph :: delete_all_edge_property(element::node *n, element::edge *e, uint32_t key, uint64_t time)
    {
        n->update_mutex.lock();
        e->delete_property(key, time);
        n->update_mutex.unlock();
        update_mutex.lock();
        my_clock++;
        assert(my_clock == time);
        pending_update_cond.broadcast();
        update_mutex.unlock();
    }

    inline bool
    graph :: check_request(size_t req_id)
    {
        bool ret;
        request_mutex.lock();
        if (done_requests[req_id].bval) {
            ret = true;
        } else {
            done_requests.erase(req_id);
            ret = false;
        }
        request_mutex.unlock();
        return ret;
    }

    inline void
    graph :: add_done_request(size_t req_id)
    {
        request_mutex.lock();
        done_requests[req_id].bval = true;
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
    graph :: remove_cache(size_t req_id)
    {
        std::unique_ptr<std::unordered_set<size_t>> caching_nodes = std::move(cache.remove_entry(req_id));
        for (auto iter = caching_nodes->begin(); iter != caching_nodes->end(); iter++)
        {
            element::node *n = (element::node *)(*iter);
            n->update_mutex.lock();
            n->remove_cached_req(req_id);
            n->update_mutex.unlock();
        }
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

} //namespace db

#endif //__GRAPH__
