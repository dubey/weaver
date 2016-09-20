/*
 * ===============================================================
 *    Description:  Issue reachability requests with destination
 *                  chosen from a small subset of the nodes, to
 *                  highlight benefit of caching results.
 *
 *        Created:  07/02/2013 11:05:01 AM
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

#define MC_REQUESTS 3000

// parameter 'dense' decides if the graph is dense (true) or sparse (false)
void
multiple_caching_prog(bool dense, bool to_exit)
{
    client c(CLIENT_ID);
    int i, num_nodes, num_edges;
    timespec start, t1, t2, dif;
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

    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    rp.hops = 0;
    
    // enable migration now
    // c.start_migration();

    // perform reachability requests in which the destination is
    // chosen from a small subset of nodes, for good caching
    std::ofstream file, req_time;
    int first, second;
    file.open("requests.rec");
    req_time.open("time.rec");
    clock_gettime(CLOCK_MONOTONIC, &t1);
    start = t1;
    for (i = 0; i < MC_REQUESTS; i++) {
        clock_gettime(CLOCK_MONOTONIC, &t2);
        dif = diff(t1, t2);
        WDEBUG << "Test: i = " << i << ", " << dif.tv_sec << ":" << dif.tv_nsec << std::endl;
        if (i % 10 == 0) {
            dif = diff(start, t2);
            req_time << dif.tv_sec << '.' << dif.tv_nsec << std::endl;
        }
        t1 = t2;
        first = rand() % 5;
        first += num_nodes/2;
        second = rand() % 5;
        while (second == first) {
            second = rand() % 5;
        }
        file << first << " " << second << std::endl;
        rp.dest = g.nodes[second];
        std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        initial_args.emplace_back(std::make_pair(g.nodes[first], rp));
        std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
    }
    file.close();
    req_time.close();
    dif = diff(start, t2);
    WDEBUG << "Total time taken " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    std::ofstream stat_file;
    stat_file.open("stats.rec", std::ios::out | std::ios::app);
    stat_file << num_nodes << " " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    stat_file.close();
    g.end_test();
}

void
multiple_sparse_caching(bool to_exit)
{
    multiple_caching_prog(false, to_exit);
}

void
multiple_dense_caching(bool to_exit)
{
    multiple_caching_prog(true, to_exit);
}
