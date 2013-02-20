/*
 * ===============================================================
 *    Description:  Multiple client threads stress test
 *                  reachability request
 *
 *        Created:  01/23/2013 05:25:46 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <thread>
#include <po6/threads/mutex.h>

#include "client/client.h"

#define NUM_NODES 1000
#define NUM_EDGES (2*NUM_NODES)
#define NUM_REQUESTS 100
#define NUM_EDGE_DEL (NUM_NODES/20)

static size_t nodes[NUM_NODES];
static size_t edges[NUM_EDGES];
static size_t edge_leading_nodes[NUM_EDGES];
static po6::threads::mutex big_lock;

void
delete_random_edges()
{
    int i;
    client c(CLIENT_PORT+1);
    for (i = 0; i < NUM_EDGE_DEL; i++)
    {
        std::cout << "Deleting edge " << (i*19) << std::endl;
        c.delete_edge(edge_leading_nodes[i*19], edges[i*19]);
    }
}

void
multiple_stress_client()
{
    client c(CLIENT_PORT);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    int i;
    std::thread *t;
    srand(42); // magic seed
    for (i = 0; i < NUM_NODES; i++)
    {
        nodes[i] = c.create_node();
    }
    for (i = 0; i < NUM_EDGES; i++)
    {
        int first = rand() % NUM_NODES;
        int second = rand() % NUM_NODES;
        while (second == first)
        {
            second = rand() % NUM_NODES;
        }
        edges[i] = c.create_edge(nodes[first], nodes[second]);
        edge_leading_nodes[i] = nodes[first];
    }
    t = new std::thread(delete_random_edges);
    t->detach();
    long seed;
    for (seed = 0; seed < 10000; seed += 100)
    {
        std::cout << "Seed " << seed << std::endl;
        srand(seed);
        for (i = 0; i < NUM_REQUESTS; i++)
        {
            int first = rand() % NUM_NODES;
            int second = rand() % NUM_NODES;
            while (second == first)
            {
                second = rand() % NUM_NODES;
            }
            std::cout << "Starting... ";
            std::cout << "Done! Req " << i << " result "
                << c.reachability_request(nodes[first], nodes[second], edge_props) << std::endl;
        }
    }
}
