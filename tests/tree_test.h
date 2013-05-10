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
#include "node_prog/dijkstra_program.h"

#define TREE_HEIGHT 15 // dont set to 1
//#define BRANCHING_FACTOR 2

void
tree_test()
{
    client c(CLIENT_PORT);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    uint32_t weight_label = 0xACC;

    uint64_t total_nodes = 1 << TREE_HEIGHT;
    uint64_t nodes[total_nodes];
    uint64_t edges[total_nodes];
    uint64_t i;
    for (i = 0; i < total_nodes - 1; i++) {
        nodes[i] = c.create_node();
        std::cout << "added node " << i<< std::endl;
    }

    for (i = 0; i < (total_nodes >> 1) - 1; i++) {
        edges[2*i] = c.create_edge(nodes[i], nodes[2*i + 1]);
        c.add_edge_prop(nodes[i], edges[2*i], weight_label, 2*i + 1);
        std::cout << "added edge of weight " << 2*i+1<< std::endl;

        edges[2*i+1] = c.create_edge(nodes[i], nodes[2*i + 2]);
        c.add_edge_prop(nodes[i], edges[2*i + 1], weight_label, 2*i + 2);

        std::cout << "added edge of weight " << 2*i+2<< std::endl;
    }
    uint64_t super_sink = c.create_node();
    std::cout << "added super sink "<< std::endl;
    uint64_t sink_edges[(total_nodes >> 1)];
    for (i = (total_nodes >> 1)-1; i < total_nodes - 1; i++) {
        sink_edges[i] = c.create_edge(nodes[i], super_sink);
        c.add_edge_prop(nodes[i], sink_edges[i], weight_label, 0);
        std::cout << "added sink edge " << i << std::endl;
    }

    std::vector<std::pair<uint64_t, node_prog::dijkstra_params>> initial_args;
    initial_args.emplace_back(std::make_pair(nodes[0], node_prog::dijkstra_params()));
    initial_args[0].second.adding_nodes = false;
    initial_args[0].second.is_widest_path = false;
    initial_args[0].second.source_handle = nodes[0];
    initial_args[0].second.dest_handle = super_sink;
    initial_args[0].second.edge_weight_name = weight_label;
    node_prog::dijkstra_params* res = c.run_node_program(node_prog::DIJKSTRA, initial_args);

    std::cout << "path of cost " << res->cost <<" wanted" << (1 << TREE_HEIGHT) - TREE_HEIGHT - 1 << std::endl;
    assert(res->cost == (1 << TREE_HEIGHT) - TREE_HEIGHT - 1);
    delete res;

    /*
    initial_args[0].second.is_widest_path = true;
    res = c.run_node_program(node_prog::DIJKSTRA, initial_args);

    std::cout << "path of width " << res->cost <<" is" << std::endl;
    assert(res->cost == 6);
    delete res;

    std::cout << "Widest path good" << std::endl;
    */
}
