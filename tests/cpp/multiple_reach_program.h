/*
 * ===============================================================
 *    Description:  Multiple reachability requests.
 *
 *        Created:  04/30/2013 02:10:39 PM
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

#define MRP_REQUESTS 1000

// issue multiple random reachability requests in a random graph
// parameter 'dense' decides if the graph is dense (true) or sparse (false)
void
multiple_reach_prog(bool dense, bool to_exit)
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
    if (dense) {
        num_edges = (int)(5.5 * (double)num_nodes);
    } else {
        num_edges = (int)(1.5 * (double)num_nodes);
    }
    test_graph g(&c, seed, num_nodes, num_edges, false, to_exit);

    // enable migration
    c.start_migration();

    // starting requests
    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    
    std::ofstream file, req_time;
    int first, second;
    file.open("requests.rec");
    for (i = 0; i < MRP_REQUESTS; i++) {
        first = rand() % num_nodes;
        second = rand() % num_nodes;
        while (second == first) {
            second = rand() % num_nodes;
        }
        file << first << " " << second << std::endl;
        std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        rp.dest = g.nodes[second];
        initial_args.emplace_back(std::make_pair(g.nodes[first], rp));
        std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
        WDEBUG << "Request " << i << ", from source " << g.nodes[first] << " to dest " << g.nodes[second]
            << ". Reachable = " << res->reachable << std::endl;
    }
    file.close();
    g.end_test();
}

void
multiple_sparse_reachability(bool to_exit)
{
    multiple_reach_prog(false, to_exit);
}

void
multiple_dense_reachability(bool to_exit)
{
    multiple_reach_prog(true, to_exit);
}
