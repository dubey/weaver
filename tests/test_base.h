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

#define TX_SIZE 1000

#include "common/clock.h"
#include "client/client.h"

struct test_graph
{
    client::client *c;
    uint64_t seed, num_nodes, num_edges;
    std::vector<uint64_t> nodes;
    std::unordered_map<uint64_t, std::vector<uint64_t>> edge_list;
    bool exit;

    test_graph(client::client *cl, uint64_t s, uint64_t num1, uint64_t num2, bool extra_node, bool to_exit)
        : c(cl)
        , seed(s)
        , num_nodes(num1)
        , num_edges(num2)
        , exit(to_exit)
    {
        uint64_t i;
        uint64_t tx = 12345678;
        srand(seed);
        assert(num_nodes % TX_SIZE == 0);
        std::vector<uint64_t> empty_vector;
        nodes.reserve(num_nodes);
        for (i = 0; i < num_nodes; i++) {
            if (i % TX_SIZE == 0) {
                DEBUG << "Creating node " << (i+1) << std::endl;
            }
            if (i % TX_SIZE == 0) {
                tx = c->begin_tx();
            }
            uint64_t n = c->create_node(tx);
            nodes.emplace_back(n);
            edge_list.emplace(i, empty_vector);
            if (i % TX_SIZE == (TX_SIZE-1)) {
                c->end_tx(tx);
            }
        }
        if (extra_node) {
            tx = c->begin_tx();
            nodes.emplace_back(c->create_node(tx));
            c->end_tx(tx);
        }
        assert(num_edges % TX_SIZE == 0);
        for (i = 0; i < num_edges; i++) {
            int first = rand() % num_nodes;
            int second = rand() % num_nodes;
            while (second == first) {
                second = rand() % num_nodes;
            }
            if (i % TX_SIZE == 0) {
                DEBUG << "Creating edge " << (i+1) << std::endl;
            }
            if (i % TX_SIZE == 0) {
                tx = c->begin_tx();
            }
            c->create_edge(tx, nodes[first], nodes[second]);
            edge_list[first].emplace_back(second);
            if (i % TX_SIZE == (TX_SIZE-1)) {
                c->end_tx(tx);
            }
        }
        DEBUG << "Created graph" << std::endl;
        std::ofstream graph_out;
        graph_out.open("graph.rec");
        for (auto &x: edge_list) {
            graph_out << x.first;
            for (auto e: x.second) {
                graph_out << " " << e;
            }
            graph_out << std::endl;
        }
        graph_out.close();
        // TODO write graph to file here
    }

    inline void
    end_test()
    {
        //for (uint64_t n: nodes) {
        //    c->delete_node(n);
        //}
        //if (exit) {
        //    c->exit_weaver();
        //}
    }
};

#endif
