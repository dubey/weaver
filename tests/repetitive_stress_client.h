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

#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "client/client.h"

static size_t repetitive_nodes[10];
static size_t repetitive_edges[10];
static bool check_reachable = false;
static po6::threads::mutex synch_mutex;
static po6::threads::cond synch_cond(&synch_mutex);
static int n1,n2,n3,n4;

void
check_reachability()
{
    client c(CLIENT_PORT+1);
    int i, j;
    while (true)
    {
        synch_mutex.lock();
        while (!check_reachable) {
            synch_cond.wait();
        }
        for (i = 0; i < 4; i++)
        {
            for (j = 0; j < 4; j++)
            {
                if (i==j) {
                    continue;
                }
                bool reach = c.reachability_request(repetitive_nodes[i], repetitive_nodes[j]);
                std::cout << "i " << i << " j " << j << std::endl;
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
    while (check_reachable) {
        synch_cond.wait();
    }
    synch_mutex.unlock();
}

void
repetitive_stress_client()
{
    client c(CLIENT_PORT);
    int i, j;
    std::thread *t;
    for (i = 0; i < 10; i++)
    {
        repetitive_nodes[i] = c.create_node();
    }
    repetitive_edges[0] = c.create_edge(repetitive_nodes[0], repetitive_nodes[1]);
    repetitive_edges[1] = c.create_edge(repetitive_nodes[2], repetitive_nodes[3]);
    t = new std::thread(check_reachability);
    t->detach();
    signal_reachable(0,1,2,3);
    c.delete_edge(repetitive_nodes[0], repetitive_edges[0]);
    c.delete_edge(repetitive_nodes[2], repetitive_edges[1]);
    signal_reachable(-1,-1,-1,-1); // nothing reachable
    repetitive_edges[0] = c.create_edge(repetitive_nodes[0], repetitive_nodes[3]);
    repetitive_edges[1] = c.create_edge(repetitive_nodes[2], repetitive_nodes[1]);
    signal_reachable(0,3,2,1);
    c.delete_edge(repetitive_nodes[0], repetitive_edges[0]);
    c.delete_edge(repetitive_nodes[2], repetitive_edges[1]);
    signal_reachable(-1,-1,-1,-1); // nothing reachable
}
