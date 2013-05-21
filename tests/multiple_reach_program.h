/*
 * ===============================================================
 *    Description:  Multiple reachability requests.
 *
 *        Created:  04/30/2013 02:10:39 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <thread>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>
 
#include "client/client.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"
#include "test_base.h"

#define NODES 10000
#define EDGES 15000
#define REQUESTS 1000

void
multiple_reach_prog()
{
    client c(CLIENT_PORT);
    int i;
    std::thread *t;
    timespec t1, t2, dif;
    std::vector<uint64_t> nodes, edges;
    srand(time(NULL));
    for (i = 0; i < NODES; i++) {
        std::cout << "Creating node " << (i+1) << std::endl;
        nodes.emplace_back(c.create_node());
    }
    for (i = 0; i < EDGES; i++) {
        int first = rand() % NODES;
        int second = rand() % NODES;
        while (second == first) {
            second = rand() % NODES;
        }
        std::cout << "Creating edge " << (i+1) << std::endl;
        edges.emplace_back(c.create_edge(nodes[first], nodes[second]));
    }
    std::cout << "Created graph\n";
    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = -1;
    
    clock_gettime(CLOCK_MONOTONIC, &t1);
    for (i = 0; i < REQUESTS; i++) {
        clock_gettime(CLOCK_MONOTONIC, &t2);
        dif = diff(t1, t2);
        std::cout << "Test: i = " << i << ", ";
        std::cout << dif.tv_sec << ":" << dif.tv_nsec << std::endl;
        t1 = t2;
        int first = rand() % NODES;
        int second = rand() % NODES;
        while (second == first) {
            second = rand() % NODES;
        }
        std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        rp.dest = nodes[second];
        initial_args.emplace_back(std::make_pair(nodes[first], rp));
        node_prog::reach_params *res = c.run_node_program(node_prog::REACHABILITY, initial_args);
        std::cout << "Request " << i << ", from source " << nodes[first] << " to dest " << nodes[second];
        std::cout << ". Reachable = " << res->reachable << std::endl;
    }
}
