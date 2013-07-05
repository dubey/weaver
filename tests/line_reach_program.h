/*
 * ===============================================================
 *    Description:  Multiple reachability requests.
 *
 *        Created:  06/11/2013 03:00:39 PM
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
#include "test_base.h"

#define LRP_REQUESTS 15000

void
line_reach_prog(bool to_exit)
{
    client c(CLIENT_ID);
    int i, num_nodes;
    timespec start, t1, t2, dif;
    std::vector<uint64_t> nodes, edges;
    srand(time(NULL));
    std::ifstream count_in;
    std::ofstream count_out;
    count_in.open("node_count.rec");
    count_in >> num_nodes;
    count_in.close();
    for (i = 0; i < num_nodes; i++) {
        DEBUG << "Creating node " << (i+1) << std::endl;
        nodes.emplace_back(c.create_node());
    }
    for (i = 0; i < num_nodes-1; i++) {
        DEBUG << "Creating edge " << (i+1) << std::endl;
        edges.emplace_back(c.create_edge(nodes[i], nodes[i+1]));
    }
    DEBUG << "Created graph\n";
    c.commit_graph();
    DEBUG << "Committed graph\n";
    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    
    std::ofstream file, req_time;
    file.open("requests.rec");
    req_time.open("time.rec");
    wclock::get_clock(&t1);
    start = t1;
    for (i = 0; i < LRP_REQUESTS; i++) {
        wclock::get_clock(&t2);
        dif = diff(t1, t2);
        DEBUG << "Test: i = " << i << ", ";
        DEBUG << dif.tv_sec << ":" << dif.tv_nsec << std::endl;
        if (i % 10 == 0) {
            dif = diff(start, t2);
            req_time << dif.tv_sec << '.' << dif.tv_nsec << std::endl;
        }
        t1 = t2;
        int first = 0;
        int second = num_nodes-1;
        file << first << " " << second << std::endl;
        std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        rp.dest = nodes[second];
        initial_args.emplace_back(std::make_pair(nodes[first], rp));
        std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
        assert(res->reachable);
    }
    file.close();
    req_time.close();
    dif = diff(start, t2);
    DEBUG << "Total time taken " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    std::ofstream stat_file;
    stat_file.open("stats.rec", std::ios::out | std::ios::app);
    stat_file << num_nodes << " " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    stat_file.close();
    if (to_exit)
        c.exit_weaver();
}
