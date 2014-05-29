/*
 * ===============================================================
 *    Description:  Issue same/similar requests on a random graph
 *                  repeatedly; check if nodes on the request
 *                  path end up on the same shard
 *
 *        Created:  06/20/2013 02:05:01 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/clock.h"
#include "client/client.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"
#include "node_prog/n_hop_reach_program.h"
#include "test_base.h"

#define ML_REQUESTS 100
#define ML_TRIES_FACTOR 200

std::vector<std::pair<uint64_t, uint64_t>>
find_long_hop(test_graph &g)
{
    std::vector<uint32_t> hops;
    std::vector<std::pair<uint64_t, uint64_t>> npair;
    int first, second;
    node_prog::reach_params rp;
    std::unique_ptr<node_prog::reach_params> res;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    rp.hops = 0;
    int num_tries = ML_REQUESTS;// g.nodes.size() / ML_TRIES_FACTOR;
    for (int i = 0; i < num_tries; i++)
    {
        first = rand() % g.num_nodes;
        second = rand() % g.num_nodes;
        while (second == first) {
            second = rand() % g.num_nodes;
        }
        npair.emplace_back(std::make_pair(g.nodes[first], g.nodes[second]));
        WDEBUG << "Found request pair " << i << " of initial src-dest search" << std::endl;
    }
    return npair;
}

// parameter 'dense' decides if the graph is dense (true) or sparse (false)
void
multiple_locality_prog(bool dense, bool to_exit)
{
    client::client c(CLIENT_ID+2, 0);
    int i, num_nodes, num_edges;
    uint64_t seed = time(NULL);

    // creating graph
    std::ifstream count_in;
    count_in.open("node_count.rec");
    count_in >> num_nodes;
    count_in.close();
    if (dense) {
        num_edges = (int)(10 * (double)num_nodes);
    } else {
        num_edges = (int)(1.5 * (double)num_nodes);
    }
    test_graph g(&c, seed, num_nodes, num_edges, false, to_exit);

    // find a suitable src-dest pair which has a long(ish) path
    auto npair = find_long_hop(g);
    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    rp.hops = 0;
    std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args(1, std::make_pair(0, rp));
    //node_prog::n_hop_reach_params rp;
    //std::vector<std::pair<uint64_t, node_prog::n_hop_reach_params>> initial_args(1);
    //initial_args[0].second.returning = false;
    //initial_args[0].second.reachable = false;
    //initial_args[0].second.prev_node.loc = COORD_ID;
    //initial_args[0].second.hops = 0;
    //initial_args[0].second.max_hops = 2;
    //initial_args[0].second.dest = g.nodes[0];
    
    // enable migration now
    c.start_migration();

    // repeatedly perform same request
    std::ofstream file, req_time;
    file.open("requests.rec");
    req_time.open("time_weaver.rec");
    uint64_t start, cur, prev, diff;
    wclock::weaver_timer timer;
    start = timer.get_time_elapsed();
    prev = start;
    for (i = 0; i < ML_REQUESTS; i++) {
        cur = timer.get_time_elapsed();
        diff = cur - prev;
        WDEBUG << "Test: i = " << i << ", " << diff << std::endl;
        req_time << diff << std::endl;
        prev = cur;
        initial_args[0].first = npair[i].first;
        initial_args[0].second.dest = npair[i].second;
        initial_args[0].second.path.clear();
        node_prog::reach_params res = c.run_reach_program(initial_args);
    }
    file.close();
    req_time.close();
    diff = cur - start;
    WDEBUG << "Total time taken " << diff << std::endl;
    //std::ofstream stat_file;
    //stat_file.open("stats.rec", std::ios::out | std::ios::app);
    //stat_file << num_nodes << " " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    //stat_file.close();
    //g.end_test();
}

void
multiple_sparse_locality(bool to_exit)
{
    multiple_locality_prog(false, to_exit);
}

void
multiple_dense_locality(bool to_exit)
{
    multiple_locality_prog(true, to_exit);
}
