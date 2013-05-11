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

#define TREE_HEIGHT 18 // dont set to 1

inline 
uint64_t path_cost(uint64_t start_node_idx){
    uint64_t max = 1 << TREE_HEIGHT;
    uint64_t height = 0;
    while ((max | start_node_idx) > start_node_idx) 
    {
        max >>= 1;
        height++;
    }
    return ((1 << height)-2)* start_node_idx;
}

void
tree_test()
{
    client c(CLIENT_PORT);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    uint32_t weight_label = 0xACC;

    uint64_t total_nodes = (1 << TREE_HEIGHT); // I want to 1-index nodes
    uint64_t nodes[total_nodes+1];
    uint64_t edges[total_nodes+1];
    uint64_t i;
    for (i = 1; i < total_nodes; i++) {
        nodes[i] = c.create_node();
        std::cout << "added node " << i<< std::endl;
    }

    for (i = 1; i < (total_nodes >> 1); i++) {
        edges[2*i] = c.create_edge(nodes[i], nodes[2*i]);
        c.add_edge_prop(nodes[i], edges[2*i], weight_label, 2*i);
        std::cout << "added edge of weight " << 2*i<< std::endl;

        edges[2*i+1] = c.create_edge(nodes[i], nodes[2*i + 1]);
        c.add_edge_prop(nodes[i], edges[2*i + 1], weight_label, 2*i + 1);

        std::cout << "added edge of weight " << 2*i+1<< std::endl;
    }
    uint64_t super_sink = c.create_node();
    std::cout << "added super sink "<< std::endl;
    uint64_t sink_edges[(total_nodes >> 1)];
    for (i = (total_nodes >> 1); i < total_nodes; i++) {
        sink_edges[i] = c.create_edge(nodes[i], super_sink);
        c.add_edge_prop(nodes[i], sink_edges[i], weight_label, 0);
        std::cout << "added sink edge from node " << i << std::endl;
    }

    std::cout << "about to start dijkstra tests" << std::endl;
    std::vector<std::pair<uint64_t, node_prog::dijkstra_params>> initial_args;
    // run starting at all nodes but bottom row
    for (i = 1; i < (total_nodes >> 1); i++) {
        initial_args.emplace_back(std::make_pair(nodes[i], node_prog::dijkstra_params()));
        initial_args[0].second.adding_nodes = false;
        initial_args[0].second.is_widest_path = false;
        initial_args[0].second.source_handle = nodes[i];
        initial_args[0].second.dest_handle = super_sink;
        initial_args[0].second.edge_weight_name = weight_label;
        node_prog::dijkstra_params* res = c.run_node_program(node_prog::DIJKSTRA, initial_args);

        std::cout << "path of cost " << res->cost <<" wanted" << path_cost(i) << std::endl;
        assert(res->cost == path_cost(i));
        delete res;
        initial_args.clear();
    }

    /*
    initial_args[0].second.is_widest_path = true;
    res = c.run_node_program(node_prog::DIJKSTRA, initial_args);

    std::cout << "path of width " << res->cost <<" is" << std::endl;
    assert(res->cost == 6);
    delete res;

    std::cout << "Widest path good" << std::endl;
    */
}
