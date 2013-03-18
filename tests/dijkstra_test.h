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
dijkstra_test()
{
    client c(CLIENT_PORT);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    uint32_t weight_label = 0xACC;
    int i;

    size_t nodes[6];
    size_t edges[8];
    for (i = 0; i < 6; i++)
    {
        nodes[i] = c.create_node();
    }

    edges[0] = c.create_edge(nodes[0], nodes[1]);
    c.add_edge_prop(nodes[0], nodes[1], weight_label, 6);

    edges[1] = c.create_edge(nodes[0], nodes[2]);
    c.add_edge_prop(nodes[0], nodes[2], weight_label, 5);

    edges[2] = c.create_edge(nodes[1], nodes[3]);
    c.add_edge_prop(nodes[1], nodes[3], weight_label, 6);

    edges[3] = c.create_edge(nodes[1], nodes[4]);
    c.add_edge_prop(nodes[1], nodes[4], weight_label, 7);

    edges[4] = c.create_edge(nodes[2], nodes[4]);
    c.add_edge_prop(nodes[2], nodes[4], weight_label, 6);

    edges[5] = c.create_edge(nodes[3], nodes[2]);
    c.add_edge_prop(nodes[3], nodes[2], weight_label, 6);

    edges[6] = c.create_edge(nodes[3], nodes[5]);
    c.add_edge_prop(nodes[3], nodes[5], weight_label, 8);

    edges[7] = c.create_edge(nodes[4], nodes[5]);
    c.add_edge_prop(nodes[4], nodes[5], weight_label, 6);

/*
    auto retpair = c.shortest_path_request(nodes[0], nodes[5], weight_label, edge_props);
    assert(retpair.first = true);
    assert(retpair.second = 17);
    std::cout << "Shortest path good" << std::endl;
    retpair = c.widest_path_request(nodes[0], nodes[5], weight_label, edge_props);
    assert(retpair.first = true);
    assert(retpair.second = 6);
    std::cout << "Widest path good" << std::endl;
    */
}
