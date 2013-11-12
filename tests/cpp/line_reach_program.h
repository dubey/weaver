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

#define LRP_REQUESTS 2000

void
line_reach_prog(bool)
{
    client::client c(CLIENT_ID, 0);
    int i, num_nodes;
    std::vector<uint64_t> nodes, edges;
    srand(time(NULL));
    std::ifstream count_in;
    //std::ofstream count_out;
    count_in.open("node_count.rec");
    count_in >> num_nodes;
    count_in.close();
    uint64_t tx_id;
    for (i = 0; i < num_nodes; i++) {
        WDEBUG << "Creating node " << (i+1) << std::endl;
        tx_id = c.begin_tx();
        nodes.emplace_back(c.create_node(tx_id));
        c.end_tx(tx_id);
    }
    for (i = 0; i < num_nodes-1; i++) {
        WDEBUG << "Creating edge " << (i+1) << std::endl;
        tx_id = c.begin_tx();
        edges.emplace_back(c.create_edge(tx_id, nodes[i], nodes[i+1]));
        c.end_tx(tx_id);
    }
    WDEBUG << "Created graph\n";
    //c.commit_graph();
    //WDEBUG << "Committed graph\n";
    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    
    //std::ofstream file;
    std::ofstream req_time;
    //file.open("requests.rec");
    req_time.open("time.rec");
    timespec t;
    uint64_t start, t1, t2, diff;
    start = wclock::get_time_elapsed(t);
    t1 = start;
    // start migration
    c.start_migration();
    for (i = 0; i < LRP_REQUESTS; i++) {
        t2 = wclock::get_time_elapsed(t);
        diff = t2 - t1;
        WDEBUG << "Test: i = " << i << ", " << diff << std::endl;
        if (i % 10 == 0) {
            diff = t2 - start;
            req_time << diff << std::endl;
        }
        t1 = t2;
        int first = 0;
        int second = num_nodes-1;
        //file << first << " " << second << std::endl;
        std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
        rp.dest = nodes[second];
        initial_args.emplace_back(std::make_pair(nodes[first], rp));
        std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
        assert(res->reachable);
    }
    //file.close();
    req_time.close();
    WDEBUG << "Total time taken " << diff << std::endl;
    //std::ofstream stat_file;
    //stat_file.open("stats.rec", std::ios::out | std::ios::app);
    //stat_file << num_nodes << " " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    //stat_file.close();
    //if (to_exit)
    //    c.exit_weaver();
}
