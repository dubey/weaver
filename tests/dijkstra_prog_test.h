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

void
dijkstra_prog_test()
{
    client c(CLIENT_PORT);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    uint32_t weight_label = 0xACC;
    int i;

    size_t nodes[6];
    size_t edges[8];
    DEBUG << "adding first node " << std::endl;
    for (i = 0; i < 6; i++) {
        nodes[i] = c.create_node();
        DEBUG << "added node " << i<< std::endl;
    }

    DEBUG << "adding first edge " << std::endl;
    edges[0] = c.create_edge(nodes[0], nodes[1]);
    DEBUG << "adding first prop " << std::endl;
    c.add_edge_prop(nodes[0], edges[0], weight_label, 6);

    edges[1] = c.create_edge(nodes[0], nodes[2]);
    c.add_edge_prop(nodes[0], edges[1], weight_label, 5);

    edges[2] = c.create_edge(nodes[1], nodes[3]);
    c.add_edge_prop(nodes[1], edges[2], weight_label, 6);

    edges[3] = c.create_edge(nodes[1], nodes[4]);
    c.add_edge_prop(nodes[1], edges[3], weight_label, 7);

    edges[4] = c.create_edge(nodes[2], nodes[4]);
    c.add_edge_prop(nodes[2], edges[4], weight_label, 6);

    edges[5] = c.create_edge(nodes[3], nodes[2]);
    c.add_edge_prop(nodes[3], edges[5], weight_label, 6);

    edges[6] = c.create_edge(nodes[3], nodes[5]);
    c.add_edge_prop(nodes[3], edges[6], weight_label, 8);

    edges[7] = c.create_edge(nodes[4], nodes[5]);
    c.add_edge_prop(nodes[4], edges[7], weight_label, 6);

    DEBUG << " starting path requests " << std::endl;

    DEBUG << "nodes[0] = " << nodes[0] <<std::endl;
    DEBUG << "nodes[1] = " << nodes[1] <<std::endl;
    DEBUG << "nodes[2] = " << nodes[2] <<std::endl;
    DEBUG << "nodes[3] = " << nodes[3] <<std::endl;
    DEBUG << "nodes[4] = " << nodes[4] <<std::endl;
    DEBUG << "nodes[5] = " << nodes[5] <<std::endl;

    DEBUG << "want shortest path from node " << nodes[0] << " to " << nodes[5] << std::endl;
    std::vector<std::pair<uint64_t, node_prog::dijkstra_params>> initial_args;
    initial_args.emplace_back(std::make_pair(nodes[0], node_prog::dijkstra_params()));
    initial_args[0].second.adding_nodes = false;
    initial_args[0].second.is_widest_path = false;
    initial_args[0].second.source_handle = nodes[0];
    initial_args[0].second.dest_handle = nodes[5];
    initial_args[0].second.edge_weight_name = weight_label;
    node_prog::dijkstra_params* res = c.run_node_program(node_prog::DIJKSTRA, initial_args);

    DEBUG << "path of cost " << res->cost <<" is" << std::endl;
    assert(res->cost == 17);
    for (auto label : res->final_path){
        DEBUG << label.first << " cost: " << label.second << std::endl;
    }
    DEBUG << "path end" << std::endl;
    delete res;
    DEBUG << "Shortest path good, checking widest" << std::endl;

    initial_args[0].second.is_widest_path = true;
    res = c.run_node_program(node_prog::DIJKSTRA, initial_args);

    DEBUG << "path of width " << res->cost <<" is" << std::endl;
    assert(res->cost == 6);
    for (auto label : res->final_path){
        DEBUG << label.first << " cost: " << label.second << std::endl;
    }
    DEBUG << "path end" << std::endl;
    delete res;
    DEBUG << "Widest path good" << std::endl;
}
