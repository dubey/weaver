/*
 * ===============================================================
 *    Description:  Repeating pattern of graph updates with
 *                  interspersed reachability requests
 *
 *        Created:  01/23/2013 11:31:49 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <thread>
#include <time.h>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "client/client.h"

static size_t repetitive_nodes[10];
static size_t repetitive_edges[10];
static bool check_reachable = false;
static bool end_program = false;
static po6::threads::mutex synch_mutex;
static po6::threads::cond synch_cond(&synch_mutex);
static auto edge_props = std::make_shared<std::vector<common::property>>();
static int n1,n2,n3,n4;

void
check_reachability()
{
    client c(CLIENT_PORT+1);
    int i, j;
    while (true) {
        synch_mutex.lock();
        while (!check_reachable && !end_program) {
            synch_cond.wait();
        }
        if (end_program) {
            synch_mutex.unlock();
            return;
        }
        for (i = 0; i < 4; i++) {
            for (j = 0; j < 4; j++) {
                if (i==j) {
                    continue;
                }
                bool reach = c.reachability_request(repetitive_nodes[i], repetitive_nodes[j], edge_props);
                if ((i==n1 && j==n2) || (i==n3 && j==n4)) {
                    assert(reach);
                } else {
                    assert(!reach);
                }
            }
        }
        check_reachable = false;
        synch_cond.signal();
        synch_mutex.unlock();
    }
}

void
signal_reachable(int num1, int num2, int num3, int num4)
{
    synch_mutex.lock();
    n1 = num1;
    n2 = num2;
    n3 = num3;
    n4 = num4;
    check_reachable = true;
    synch_cond.signal();
    while (check_reachable && !end_program) {
        synch_cond.wait();
    }
    synch_mutex.unlock();
}

void 
delete_edges(client *c, int num1, int num2)
{
    c->delete_edge(repetitive_nodes[num1], repetitive_edges[0]);
    c->delete_edge(repetitive_nodes[num2], repetitive_edges[1]);
}

void
create_edges(client *c, int num1, int num2, int num3, int num4)
{
    repetitive_edges[0] = c->create_edge(repetitive_nodes[num1], repetitive_nodes[num2]);
    repetitive_edges[1] = c->create_edge(repetitive_nodes[num3], repetitive_nodes[num4]);
}

timespec diff(timespec start, timespec end)
{
        timespec temp;
        if ((end.tv_nsec-start.tv_nsec)<0) {
            temp.tv_sec = end.tv_sec-start.tv_sec-1;
            temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
        } else {
            temp.tv_sec = end.tv_sec-start.tv_sec;
            temp.tv_nsec = end.tv_nsec-start.tv_nsec;
        }
        return temp;
}
void
repetitive_stress_client()
{
    client c(CLIENT_PORT);
    int i, j;
    std::thread *t;
    timespec t1, t2, dif;
    for (i = 0; i < 10; i++) {
        repetitive_nodes[i] = c.create_node();
    }
    t = new std::thread(check_reachability);
    t->detach();
    
    clock_gettime(CLOCK_MONOTONIC, &t1);
    for (i = 0; i < 10000; i++) {
        clock_gettime(CLOCK_MONOTONIC, &t2);
        dif = diff(t1, t2);
        std::cout << "Test: i = " << i << ", ";
        std::cout << dif.tv_sec << ":" << dif.tv_nsec << std::endl;
        t1 = t2;
        create_edges(&c,0,1,2,3);
        common::property prop(42, 84, 0);
        c.add_edge_prop(repetitive_nodes[0], repetitive_edges[0], prop.key, prop.value);
        c.add_edge_prop(repetitive_nodes[2], repetitive_edges[1], prop.key, prop.value);
        edge_props->push_back(prop);
        signal_reachable(0,1,2,3);
        c.del_edge_prop(repetitive_nodes[0], repetitive_edges[0], prop.key);
        signal_reachable(2,3,-1,-1);
        edge_props->clear();
        signal_reachable(0,1,2,3);
        delete_edges(&c,0,2);
        signal_reachable(-1,-1,-1,-1); // nothing reachable
        create_edges(&c,0,3,2,1);
        signal_reachable(0,3,2,1);
        delete_edges(&c,0,2);
        create_edges(&c,0,3,2,1);
        signal_reachable(0,3,2,1);
        delete_edges(&c,0,2);
        create_edges(&c,0,3,2,1);
        delete_edges(&c,0,2);
        create_edges(&c,0,1,2,3);
        signal_reachable(0,1,2,3);
        delete_edges(&c,0,2);
        signal_reachable(-1,-1,-1,-1); // nothing reachable
    }

    // releasing locks, killing all threads
    end_program = true;
    synch_cond.broadcast();
}
