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
#include <busybee_sta.h>

#include "element/property.h"
#include "element/meta_element.h"
#include "element/node.h"
#include "element/edge.h"

#define SEND_PORT_INCR 1000
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
            po6::threads::mutex elem_lock;

        public:
            int node_count;
            po6::net::location myloc;
            busybee_sta bb;
            busybee_sta bb_recv;
            po6::threads::mutex bb_lock;
            element::node* create_node(uint32_t time);
            element::edge* create_edge(std::unique_ptr<element::meta_element> n1,
                                        std::unique_ptr<element::meta_element> n2, 
                                        uint32_t direction, 
                                        uint32_t time);
            bool mark_visited(element::node *n, uint32_t req_counter);
            bool remove_visited(element::node *n, uint32_t req_counter);
            //void delete_node (element::node 
            //bool find_node (element::node **n);
            //bool find_edge (element::edge **e);
            //std::vector<element::node *> transclosure (element::node *start);
    
    }; //class graph

    inline
    graph :: graph(const char* ip_addr, in_port_t port)
        : node_count (0)
        , myloc(ip_addr, port)
        , bb(myloc.address, myloc.port + SEND_PORT_INCR, 0)
        , bb_recv(myloc.address, myloc.port, 0)
    {
    }

    inline element::node*
    graph :: create_node(uint32_t time)
    {
        element::node* new_node = new element::node(myloc, time, NULL);
        elem_lock.lock();
        V.push_back(new_node);
        elem_lock.unlock();
        
        //std::cout << "Creating node, addr = " << (void*) new_node 
        //        << " and node count " << (++node_count) << std::endl;
        return new_node;
    }

    inline element::edge*
    graph :: create_edge(std::unique_ptr<element::meta_element> n1,
                          std::unique_ptr<element::meta_element> n2,
                          uint32_t direction, 
                          uint32_t time)
    {
        element::node *local_node = (element::node *) n1->get_addr();
        element::edge *new_edge;
        elem_lock.lock();
        if(direction == 0) 
        {
            new_edge = new element::edge(myloc, time, NULL, *n1, *n2);
            local_node->out_edges.push_back(new_edge->get_meta_element());
        } else if (direction == 1)
        {
            new_edge = new element::edge(myloc, time, NULL, *n2, *n1);
            local_node->in_edges.push_back(new_edge->get_meta_element());
        } else
        {
            std::cerr << "edge direction error: " << direction << std::endl;
            elem_lock.unlock();
            return NULL;
        }
        E.push_back(new_edge);
        elem_lock.unlock();

        //std::cout << "Creating edge, addr = " << (void *) new_edge << std::endl;
        return new_edge;
    }

    inline bool
    graph :: mark_visited(element::node *n, uint32_t req_counter)
    {
        uint32_t key = 0; //visited key
        /*char key[] = "v\0";
        char *value = (char *) malloc (10);
        memset (value, '\0', 10);
        std::stringstream out;
        out << req_counter;
        strncpy (value, out.str().c_str(), out.str().length());
        std::cout << "string of req counter " << key
                  << "," << value << " " ;
        */
        element::property p(key, req_counter);
        elem_lock.lock();
        if (n->has_property(p)) 
        {
            elem_lock.unlock();
            return true;
        } else 
        {
            n->add_property(p);
            elem_lock.unlock();
            return false;
        }
    }

    inline bool
    graph :: remove_visited(element::node *n, uint32_t req_counter)
    {
        uint32_t key = 0; //visited key
        element::property p(key, req_counter);
        elem_lock.lock();
        n->remove_property(p);
        elem_lock.unlock();
    }

} //namespace db

#endif //__GRAPH__
