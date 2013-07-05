/*
 * ===============================================================
 *    Description:  Basic infrastructure for tests.
 *
 *        Created:  05/17/2013 03:19:45 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __TEST_BASE__
#define __TEST_BASE__

#include <time.h>
#include "client/client.h"

struct test_graph
{
    client *c;
    uint64_t seed, num_nodes, num_edges;
    std::vector<uint64_t> nodes;
    std::vector<uint64_t> edges;
    bool exit;

    test_graph(client *cl, uint64_t s, uint64_t num1, uint64_t num2, bool extra_node, bool to_exit)
        : c(cl)
        , seed(s)
        , num_nodes(num1)
        , num_edges(num2)
        , exit(to_exit)
    {
        uint64_t i;
        srand(seed);
        for (i = 0; i < num_nodes; i++) { 
            DEBUG << "Creating node " << (i+1) << std::endl;
            nodes.emplace_back(c->create_node());
        }
        if (extra_node) {
            nodes.emplace_back(c->create_node());
        }
        for (i = 0; i < num_edges; i++) {
            int first = rand() % num_nodes;
            int second = rand() % num_nodes;
            while (second == first) {
                second = rand() % num_nodes;
            }
            DEBUG << "Creating edge " << (i+1) << std::endl;
            edges.emplace_back(c->create_edge(nodes[first], nodes[second]));
        }
        DEBUG << "Created graph" << std::endl;
        c->commit_graph();
        DEBUG << "Committed graph" << std::endl;
    }

    inline void
    end_test()
    {
        for (uint64_t n: nodes) {
            c->delete_node(n);
        }
        if (exit) {
            c->exit_weaver();
        }
    }
};

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

#endif
