/*
 * ===============================================================
 *    Description:  Issue multiple queries between two nodes
 *                  that are not reachable, forcing traversal of
 *                  entire graph.
 *
 *        Created:  06/17/2013 03:36:55 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"
#include "test_base.h"

#define URP_REQUESTS 1000

void
unreachable_reach_prog(bool to_exit)
{
    client c(CLIENT_ID);
    int i, num_nodes, num_edges;
    std::ofstream seed_file;
    uint64_t seed = time(NULL);
    WDEBUG << "seed " << seed << std::endl;
    seed_file.open("seed.rec");
    seed_file << seed;
    seed_file.close();

    // creating graph
    std::ifstream count_in;
    count_in.open("node_count.rec");
    count_in >> num_nodes;
    count_in.close();
    num_edges = (int)(5.5 * (double)num_nodes); // dense => lots of traversal
    test_graph g(&c, seed, num_nodes, num_edges, true, to_exit);

    // requests
    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    
    std::ofstream file, req_time;
    file.open("requests.rec");
    for (i = 0; i < URP_REQUESTS; i++) {
        int first = rand() % num_nodes;
        file << first << " " << num_nodes << std::endl;
        std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        rp.dest = g.nodes[num_nodes];
        initial_args.emplace_back(std::make_pair(g.nodes[first], rp));
        WDEBUG << "Request " << i << ", from source " << g.nodes[first] << " to dest " << g.nodes[num_nodes] << "." << std::endl;
        std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
        assert(!res->reachable);
    }
    file.close();
    g.end_test();
}
