/*
 * ===============================================================
 *    Description:  Basic graph db reachability node program.
 *
 *        Created:  01/23/2013 01:20:10 PM
 *
 *         Author:  Ayush Dubey, Greg Hill, 
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"
#include <vector>
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"

void
node_prog_test()
{
    client c(CLIENT_PORT);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    int i;

    size_t nodes[6];
    size_t edges[8];
    std::cout << "adding first node " << std::endl;
    for (i = 0; i < 6; i++) {
        nodes[i] = c.create_node();
        std::cout << "added node " << i<< std::endl;
    }
    std::cout << "adding edges" << std::endl;
    for (i = 0; i < 5; i++) {
        edges[i] = c.create_edge(nodes[i], nodes[i+1]);
    }

    std::cout << " starting reach requests " << std::endl;

    std::cout << "nodes[0] = " << nodes[0] <<std::endl;
    std::cout << "nodes[1] = " << nodes[1] <<std::endl;
    std::cout << "nodes[2] = " << nodes[2] <<std::endl;
    std::cout << "nodes[3] = " << nodes[3] <<std::endl;
    std::cout << "nodes[4] = " << nodes[4] <<std::endl;
    std::cout << "nodes[5] = " << nodes[5] <<std::endl;

    std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
    initial_args.emplace_back(std::make_pair(nodes[0], node_prog::reach_params()));
    node_prog::reach_params &params = initial_args[0].second;
    params.prev_node.loc = -1;
    params.dest.handle = nodes[4];
    node_prog::reach_params *res = c.run_node_program(node_prog::REACHABILITY, initial_args);
    std::cout << "Got back reachability reply " << res->reachable << std::endl;
    delete res;
    /*
    auto retpair = c.shortest_path_request(nodes[0], nodes[5], weight_label, edge_props);
    std::cout <<retpair.first <<std::endl;
    //assert(retpair.first == 17);
    std::cout << "path is" << std::endl;
    for (auto label : retpair.second){
        std::cout << label.first << " cost: " << label.second << std::endl;
    }
    std::cout << "path end" << std::endl;
    //std::cout <<retpair.second <<std::endl;
    //assert(retpair.second == 17);
    std::cout << "Shortest path good" << std::endl;
    */

    /*
    auto retpair = c.widest_path_request(nodes[0], nodes[5], weight_label, edge_props);
    //assert(retpair.first == 6);
    std::cout <<retpair.first <<std::endl;
    std::cout << "path is" << std::endl;
    for (auto label : retpair.second){
        std::cout << label.first << " cost: " << label.second << std::endl;
    }
    std::cout << "path end" << std::endl;
    //assert(retpair.second == 6);
    std::cout << "Widest path good" << std::endl;
    */
}
