/*
 * ===============================================================
 *    Description:  Basic graph db clustering coefficient calc test
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

void
clustering_test()
{
    client c(CLIENT_PORT);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    int i;
    size_t central_node = c.create_node();
    size_t num_nodes = 100;
    size_t star_nodes[num_nodes];
    size_t star_edges[num_nodes*2];
    size_t edges_per_node = 6;
    size_t ring_edges[num_nodes*edges_per_node];
    for (i = 0; i < num_nodes; i++) {
        star_nodes[i] = c.create_node();
    }
    for (i = 0; i < num_nodes; i++) {
        star_edges[i] = c.create_edge(central_node, star_nodes[i]);
    }
    //connect star nodes back to center. Shouldn't change coefficient
    assert(c.local_clustering_coefficient(central_node, edge_props) == 0);
    for (i = 0; i < num_nodes; i++) {
        star_edges[i+num_nodes] = c.create_edge(star_nodes[i], central_node);
    }
    assert(c.local_clustering_coefficient(central_node, edge_props) == 0);

    size_t numerator;
    double denominator = (double) ((num_nodes)*(num_nodes-1));
    double calculated_coeff;
    for (int node_skip = 1; node_skip <= edges_per_node; node_skip++) {
        for (i = 0; i < num_nodes; i++) {
            ring_edges[i+((node_skip-1)*num_nodes)] =
                c.create_edge(star_nodes[i], star_nodes[(i+node_skip)%num_nodes]);
            numerator = ((node_skip-1)*num_nodes+i+1);
            calculated_coeff = c.local_clustering_coefficient(central_node, edge_props);
            assert(calculated_coeff == numerator/denominator);
        }
    }
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
        assert(c.local_clustering_coefficient(central_node, edge_props) == numerator/denominator);
    }
}
