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

inline 
uint64_t path_cost(uint64_t start_node_idx, int tree_height)
{
    uint64_t max = 1 << tree_height;
    uint64_t height = 0;
    while ((max | start_node_idx) > start_node_idx) // finds height difference (distance of MSBs)
    {
        max >>= 1;
        height++;
    }
    return ((1 << height)-2)* start_node_idx;
}

void
dijkstra_tree_test(bool to_exit)
{
    client c(CLIENT_ID);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    const uint32_t weight_label = 0xACC;

    uint64_t total_nodes = (1 << TREE_HEIGHT); // I want to 1-index nodes
    uint64_t nodes[total_nodes+1];
    uint64_t edges[total_nodes+1];
    uint64_t i;
    for (i = 1; i < total_nodes; i++) {
        nodes[i] = c.create_node();
        DEBUG << "added node " << i << std::endl;
    }

    for (i = 1; i < (total_nodes >> 1); i++) {
        edges[2*i] = c.create_edge(nodes[i], nodes[2*i]);
        c.add_edge_prop(nodes[i], edges[2*i], weight_label, 2*i);
        DEBUG << "added edge " << edges[2*i] << " of weight " << 2*i<< std::endl;

        edges[2*i+1] = c.create_edge(nodes[i], nodes[2*i + 1]);
        c.add_edge_prop(nodes[i], edges[2*i + 1], weight_label, 2*i + 1);

        DEBUG << "added edge " << edges[2*i + 1] << " of weight " << 2*i+1<< std::endl;
    }
    uint64_t super_sink = c.create_node();
    DEBUG << "added super sink "<< std::endl;
    uint64_t sink_edges[(total_nodes >> 1)];
    for (i = (total_nodes >> 1); i < total_nodes; i++) {
        sink_edges[i] = c.create_edge(nodes[i], super_sink);
        c.add_edge_prop(nodes[i], sink_edges[i], weight_label, 0);
        DEBUG << "added sink edge from node " << i << std::endl;
    }

    DEBUG << "about to start dijkstra tests" << std::endl;
    std::vector<std::pair<uint64_t, node_prog::dijkstra_params>> initial_args;
    // run starting at all nodes but bottom row
    for (i = 1; i < (total_nodes >> 1); i++) {
        initial_args.emplace_back(std::make_pair(nodes[i], node_prog::dijkstra_params()));
        initial_args[0].second.adding_nodes = false;
        initial_args[0].second.is_widest_path = false;
        initial_args[0].second.src_handle = nodes[i];
        initial_args[0].second.dst_handle = super_sink;
        initial_args[0].second.edge_weight_key = weight_label;
        DEBUG << "about to run dijkstra for source " << i << std::endl;
        std::unique_ptr<node_prog::dijkstra_params> res = c.run_node_program(node_prog::DIJKSTRA, initial_args);

        DEBUG << "path of cost " << res->cost <<" wanted" << path_cost(i, TREE_HEIGHT) << std::endl;
        assert(res->cost == path_cost(i, TREE_HEIGHT));
        initial_args.clear();
    }

    DEBUG << "about to test dijkstra after deleting some nodes" << std::endl;
    // delete nodes up from the bottom left, then test from top node
    for (int height = TREE_HEIGHT; height > 1; height--) {
        uint64_t delete_idx = 1 << (height - 1);
        c.delete_node(nodes[delete_idx]);

        initial_args.emplace_back(std::make_pair(nodes[1], node_prog::dijkstra_params()));
        initial_args[0].second.adding_nodes = false;
        initial_args[0].second.is_widest_path = false;
        initial_args[0].second.src_handle = nodes[1];
        initial_args[0].second.dst_handle = super_sink;
        initial_args[0].second.edge_weight_key = weight_label;
        std::unique_ptr<node_prog::dijkstra_params> res = c.run_node_program(node_prog::DIJKSTRA, initial_args);

        uint64_t alternate_route_node = (1 << (height - 1))+1;
        uint64_t expected_cost = path_cost(1, height-1) + alternate_route_node + path_cost(alternate_route_node, TREE_HEIGHT);
        DEBUG << "path of cost " << res->cost <<" wanted " << expected_cost << " though node " << alternate_route_node << std::endl;
        assert(res->cost == expected_cost);
        initial_args.clear();
    }
    if (to_exit)
        c.exit_weaver();
}
