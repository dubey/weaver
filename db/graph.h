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
#include "element/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"

#define NUM_THREADS 4

namespace db
{
    class graph
    {
        public:
            graph(const char* ip_addr, in_port_t port);

        private:
            std::vector<element::node *> V;
            std::vector<element::edge *> E;
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
            element::node* create_node(uint64_t time);
            element::edge* create_edge(void *n1,
                std::unique_ptr<common::meta_element> n2, uint64_t time);
            void delete_node(element::node *n, uint64_t del_time);
            void delete_edge(element::node *n, element::edge *e, uint64_t del_time);
            bool mark_visited(element::node *n, size_t req_counter);
            bool remove_visited(element::node *n, size_t req_counter);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            void wait_for_updates(uint64_t recd_clock);
    };

    inline
    graph :: graph(const char* ip_addr, in_port_t port)
        : my_clock(0)
        , pending_update_cond(&update_mutex)
        , node_count(0)
        , myloc(new po6::net::location(ip_addr, port))
        , coord(new po6::net::location(COORD_IPADDR, COORD_REC_PORT))
        , bb(myloc->address, myloc->port + SEND_PORT_INCR, 0)
        , bb_recv(myloc->address, myloc->port, 0)
    {
    }

    inline element::node*
    graph :: create_node(uint64_t time)
    {
        element::node* new_node = new element::node(myloc, time);
        update_mutex.lock();
        my_clock++;
        assert(my_clock == time); // TODO coordinator needs to ensure this
        V.push_back(new_node);
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
        update_mutex.lock();
        my_clock++;
        assert(my_clock == time);
        local_node->update_mutex.lock();
        local_node->add_edge(new_edge);
        local_node->update_mutex.unlock();
        E.push_back(new_edge);
        pending_update_cond.broadcast();
        update_mutex.unlock();

        //std::cout << "Creating edge, addr = " << (void *) new_edge << std::endl;
        return new_edge;
    }

    inline void
    graph :: delete_node(element::node *n, uint64_t del_time)
    {
        update_mutex.lock();
        my_clock++;
        assert(my_clock == del_time);
        n->update_mutex.lock();
        n->update_del_time(del_time);
        n->update_mutex.unlock();
        pending_update_cond.broadcast();
        update_mutex.unlock();
    }

    inline void
    graph :: delete_edge(element::node *n, element::edge *e, uint64_t del_time)
    {
        update_mutex.lock();
        my_clock++;
        assert(my_clock == del_time);
        n->update_mutex.lock();
        e->update_del_time(del_time);
        n->update_mutex.unlock();
        pending_update_cond.broadcast();
        update_mutex.unlock();
    }

    inline bool
    graph :: mark_visited(element::node *n, size_t req_counter)
    {
        uint32_t key = 0; //visited key
        element::property p(key, req_counter);
        n->check_and_add_property(p);
    }

    inline bool
    graph :: remove_visited(element::node *n, size_t req_counter)
    {
        uint32_t key = 0; //visited key
        element::property p(key, req_counter);
        n->remove_property(p);
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
