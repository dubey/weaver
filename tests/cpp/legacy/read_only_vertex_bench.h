/*
 * ===============================================================
 *    Description:  Read-only multi-client benchmark which only
 *                  reads 1 vertex per query.
 *
 *        Created:  04/07/2014 08:51:26 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <thread>
#include <chrono>
#include <random>
#include <po6/threads/mutex.h>
#include <busybee_utils.h>

#include "common/clock.h"
#include "client/client.h"

using cl::client;

void
exec_reads(std::default_random_engine &generator,
    std::uniform_int_distribution<uint64_t> &distribution,
    client &cl,
    uint64_t num_requests,
    wclock::weaver_timer &timer,
    std::vector<uint64_t> &timestamps)
{
    uint64_t t;
    uint64_t first = timer.get_time_elapsed();
    timestamps.emplace_back(first);
    for (uint64_t i = 0; i < num_requests; i++) {
        node_prog::read_node_props_params rp, return_params;
        std::string n = "BLOCKID_" + std::to_string(distribution(generator));

        std::vector<std::pair<std::string, node_prog::read_node_props_params>> args(1, std::make_pair(n, rp));
        //cl.read_node_props_program(args, return_params);

        t = timer.get_time_elapsed();
        timestamps.emplace_back(t);
    }

    //std::cout << "Time taken for " << num_requests << " requests = " << (t-first)/(1000000) << std::endl;
}

void
run_read_only_vertex_bench(const std::string &output_fname, uint64_t num_nodes, uint64_t num_requests)
{
    po6::net::ipaddr ip;
    busybee_discover(&ip);
    uint64_t pid = getpid();

    std::random_device rd;
    std::default_random_engine generator(rd());
    std::uniform_int_distribution<uint64_t> distribution(1, num_nodes-1);
    client cl("128.84.167.101", 2002, "/home/dubey/installs/etc/weaver.yaml");
    wclock::weaver_timer timer;
    std::vector<uint64_t> timestamps;
    timestamps.reserve(num_requests+1);

    exec_reads(generator, distribution, cl, num_requests, timer, timestamps);

    std::ofstream file;
    std::uniform_int_distribution<uint64_t> fname_dist(1, UINT64_MAX-1);
    uint64_t name_rand = fname_dist(generator);
    file.open(output_fname + "/" + std::to_string(pid) + "_" + std::to_string(name_rand), std::ofstream::out);
    for (uint64_t t: timestamps) {
        file << t << std::endl;
    }
    file.close();
}
