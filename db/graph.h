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

#include <iostream>

#include <sstream>
#include <stdlib.h>
#include <vector>
#include <po6/net/location.h>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>
#include <busybee_sta.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"
#include "threadpool/threadpool.h"

namespace db
{
    // Pending batched request
    class batch_request
    {
        public:
        std::unique_ptr<po6::net::location> prev_loc; // prev server's port
        size_t coord_id; // coordinator's req id
        size_t prev_id; // prev server's req id
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
            prev_id = 0;
            num = 0;
            reachable = false;
            dest_addr = NULL;
        }

        batch_request(const batch_request &tup)
        {
            prev_loc.reset(new po6::net::location(*tup.prev_loc));
            prev_id = tup.prev_id;
            num = tup.num;
            reachable = tup.reachable;
            src_nodes.reset(new std::vector<size_t>(*tup.src_nodes));
            dest_addr = tup.dest_addr;
            dest_loc.reset(new po6::net::location(*tup.dest_loc));
        }
    };

    class graph
    {
        public:
            graph(int my_id, const char* ip_addr, in_port_t port);

        private:
            po6::threads::mutex update_mutex;
            uint64_t my_clock;
            po6::threads::cond pending_update_cond;

        public:
            int node_count;
            std::shared_ptr<po6::net::location> myloc;
            std::shared_ptr<po6::net::location> coord;
            busybee_sta bb; // Busybee instance used for sending messages
            busybee_sta bb_recv; // Busybee instance used for receiving messages
            po6::threads::mutex bb_lock;
            int myid;
            size_t outgoing_req_id_counter;
            po6::threads::mutex outgoing_req_id_counter_mutex;
            std::unordered_map<size_t, std::shared_ptr<batch_request>> pending_batch;
            db::thread::pool thread_pool;
            
        public:
            element::node* create_node(uint64_t time);
            element::edge* create_edge(void *n1,
                std::unique_ptr<common::meta_element> n2, uint64_t time);
            void delete_node(element::node *n, uint64_t del_time);
            void delete_edge(element::node *n, element::edge *e, uint64_t del_time);
            void add_edge_property(element::node *n, element::edge *e,
                std::unique_ptr<common::property> prop, uint64_t time);
            void delete_all_edge_property(element::node *n, element::edge *e, uint32_t key, uint64_t time);
            bool mark_visited(element::node *n, size_t req_counter);
            bool remove_visited(element::node *n, size_t req_counter);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            void wait_for_updates(uint64_t recd_clock);
    };

    inline
    graph :: graph(int my_id, const char* ip_addr, in_port_t port)
        : my_clock(0)
        , pending_update_cond(&update_mutex)
        , node_count(0)
        , myloc(new po6::net::location(ip_addr, port))
        , coord(new po6::net::location(COORD_IPADDR, COORD_REC_PORT))
        , bb(myloc->address, myloc->port + SEND_PORT_INCR, 0)
        , bb_recv(myloc->address, myloc->port, 0)
        , myid(my_id)
        , outgoing_req_id_counter(0)
        , thread_pool(NUM_THREADS)
    {
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

    inline void
    graph :: delete_node(element::node *n, uint64_t del_time)
    {
        n->update_mutex.lock();
        n->update_del_time(del_time);
        n->update_mutex.unlock();
        update_mutex.lock();
        my_clock++;
        assert(my_clock == del_time);
        pending_update_cond.broadcast();
        update_mutex.unlock();
    }

    inline void
    graph :: delete_edge(element::node *n, element::edge *e, uint64_t del_time)
    {
        n->update_mutex.lock();
        e->update_del_time(del_time);
        n->update_mutex.unlock();
        update_mutex.lock();
        my_clock++;
        assert(my_clock == del_time);
        pending_update_cond.broadcast();
        update_mutex.unlock();
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
    graph :: mark_visited(element::node *n, size_t req_counter)
    {
        n->check_and_add_seen(req_counter);
    }

    inline bool
    graph :: remove_visited(element::node *n, size_t req_counter)
    {
        n->remove_seen(req_counter);
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
