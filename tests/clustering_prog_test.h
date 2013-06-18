/*
 * ===============================================================
 *    Description:  Basic graph db node program.
 *
 *        Created:  01/23/2013 01:20:10 PM
 *
 *         Author:  Greg Hill, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"
#include <vector>
#include "node_prog/node_prog_type.h"
#include "node_prog/clustering_program.h"

void
clustering_prog_test()
{
    client c(CLIENT_PORT);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    int i;
    int testcount = 0;
    size_t central_node = c.create_node();
    size_t num_nodes = 200;
    size_t star_nodes[num_nodes];
    size_t star_edges[num_nodes*2];
    size_t edges_per_node = 6;
    size_t ring_edges[num_nodes*edges_per_node];

    std::vector<std::pair<uint64_t, node_prog::clustering_params>> initial_args;
    initial_args.emplace_back(std::make_pair(central_node, node_prog::clustering_params()));
    initial_args[0].second.is_center = true;
    initial_args[0].second.outgoing = true;
    std::unique_ptr<node_prog::clustering_params> res;

    for (i = 0; i < num_nodes; i++) {
        star_nodes[i] = c.create_node();
    }
    for (i = 0; i < num_nodes; i++) {
        star_edges[i] = c.create_edge(central_node, star_nodes[i]);
    }
    //connect star nodes back to center. Shouldn't change coefficient
    res = c.run_node_program(node_prog::CLUSTERING, initial_args);
    assert(res->clustering_coeff == 0);
    DEBUG << "completed test " << ++testcount << std::endl;

    for (i = 0; i < num_nodes; i++) {
        star_edges[i+num_nodes] = c.create_edge(star_nodes[i], central_node);
    }
    res = c.run_node_program(node_prog::CLUSTERING, initial_args);
    assert(res->clustering_coeff == 0);
    DEBUG << "completed test " << ++testcount << std::endl;

    size_t numerator;
    double denominator = (double) ((num_nodes)*(num_nodes-1));
    double calculated_coeff;
    for (int node_skip = 1; node_skip <= edges_per_node; node_skip++) {
        for (i = 0; i < num_nodes; i++) {
            ring_edges[i+((node_skip-1)*num_nodes)] =
                c.create_edge(star_nodes[i], star_nodes[(i+node_skip)%num_nodes]);
            numerator = ((node_skip-1)*num_nodes+i+1);
           res = c.run_node_program(node_prog::CLUSTERING, initial_args);
           assert(res->clustering_coeff == (numerator/denominator));
            DEBUG << "completed test " << ++testcount << std::endl;
        }
    }
    DEBUG << "starting clustering tests with deletion" <<  std::endl;
    //delete some of the original edges and nodes of star graph
    for (i = 0; i < (num_nodes-edges_per_node); i++) {
        denominator = (double) ((num_nodes-i-1)*(num_nodes-i-2));
        numerator = edges_per_node*(num_nodes-i-1);
        //account for edges pointing to already deleted nodes
        for (int j = 0; j <= i && j < edges_per_node; j++) {
            numerator -= (edges_per_node-j);
        }
        if ((i % 2) == 0) {
            c.delete_edge(central_node, star_edges[i]);
        } else {
            c.delete_node(star_nodes[i]);
        }
        res = c.run_node_program(node_prog::CLUSTERING, initial_args);
        //DEBUG << "expected " << numerator << "/" << denominator << " = " << (numerator/denominator) << " but got " << res->clustering_coeff <<  std::endl;
        assert(res->clustering_coeff == (numerator/denominator));
        DEBUG << "completed test " << ++testcount << std::endl;
    }
    DEBUG << "completed all clustering tests" <<  std::endl;

}
