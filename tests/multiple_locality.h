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

#include "client/client.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"
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
        //std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        //rp.dest = g.nodes[second];
        //initial_args.emplace_back(std::make_pair(g.nodes[first], rp));
        //res = g.c->run_node_program(node_prog::REACHABILITY, initial_args);
        npair.emplace_back(std::make_pair(first, second));
        //hops.emplace_back(res->hops);
        DEBUG << "Done request " << i << " of initial src-dest search" << std::endl;
    }
    return npair;
    //uint32_t target_hops = (*std::max_element(hops.begin(), hops.end()))/2;
    //int ret_index = 0, i;
    //for (i = 1; i < num_tries; i++) {
    //    if (hops.at(i) > hops.at(ret_index)
    //     && hops.at(i) < target_hops) {
    //        ret_index = i;
    //    }
    //}
    //DEBUG << "Going to start locality test with pair " << g.nodes[npair.at(ret_index).first]
    //    << "," << g.nodes[npair.at(ret_index).second] << " and hop count = " << hops.at(ret_index) << std::endl;
    //return npair.at(ret_index);
}

// parameter 'dense' decides if the graph is dense (true) or sparse (false)
void
multiple_locality_prog(bool dense, bool to_exit)
{
    client::client c(CLIENT_ID, 0);
    int i, num_nodes, num_edges;
    timespec t;
    uint64_t seed = time(NULL);
    //std::ofstream seed_file;
    //DEBUG << "seed " << seed << std::endl;
    //seed_file.open("seed.rec");
    //seed_file << seed;
    //seed_file.close();

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
    
    // enable migration now
    //c.start_migration();

    // repeatedly perform same request
    std::ofstream file, req_time;
    file.open("requests.rec");
    req_time.open("time_weaver.rec");
    uint64_t start, cur, prev, diff;
    start = wclock::get_time_elapsed(t);
    prev = start;
    for (i = 0; i < ML_REQUESTS; i++) {
        cur = wclock::get_time_elapsed(t);
        diff = cur - prev;
        DEBUG << "Test: i = " << i << ", " << diff << std::endl;
        req_time << diff << std::endl;
        prev = cur;
        file << npair[i].first << " " << npair[i].second << std::endl;
        std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        rp.dest = g.nodes[npair[i].second];
        initial_args.emplace_back(std::make_pair(g.nodes[npair[i].first], rp));
        std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
        assert(res->reachable);
    }
    file.close();
    req_time.close();
    diff = cur - start;
    DEBUG << "Total time taken " << diff << std::endl;
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
