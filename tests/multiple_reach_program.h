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

#define MRP_REQUESTS 500

// issue multiple random reachability requests in a random graph
// parameter 'dense' decides if the graph is dense (true) or sparse (false)
void
multiple_reach_prog(bool dense, bool to_exit)
{
    client c(CLIENT_ID);
    int i, num_nodes, num_edges;
    timespec first, t1, t2, dif;
    std::ofstream seed_file;
    uint64_t seed = time(NULL);
    DEBUG << "seed " << seed << std::endl;
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

    // starting requests
    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    
    std::ofstream file, req_time;
    file.open("requests.rec");
    req_time.open("time.rec");
    clock_gettime(CLOCK_MONOTONIC, &t1);
    first = t1;
    for (i = 0; i < MRP_REQUESTS; i++) {
        clock_gettime(CLOCK_MONOTONIC, &t2);
        dif = diff(t1, t2);
        DEBUG << "Test: i = " << i << ", " << dif.tv_sec << ":" << dif.tv_nsec << std::endl;
        if (i % 10 == 0) {
            dif = diff(first, t2);
            req_time << dif.tv_sec << '.' << dif.tv_nsec << std::endl;
        }
        t1 = t2;
        int first = rand() % num_nodes;
        int second = rand() % num_nodes;
        while (second == first) {
            second = rand() % num_nodes;
        }
        file << first << " " << second << std::endl;
        std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        rp.dest = g.nodes[second];
        initial_args.emplace_back(std::make_pair(g.nodes[first], rp));
        std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
        DEBUG << "Request " << i << ", from source " << g.nodes[first] << " to dest " << g.nodes[second]
            << ". Reachable = " << res->reachable << std::endl;
    }
    file.close();
    req_time.close();
    dif = diff(first, t2);
    DEBUG << "Total time taken " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    std::ofstream stat_file;
    stat_file.open("stats.rec", std::ios::out | std::ios::app);
    stat_file << num_nodes << " " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    stat_file.close();
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
