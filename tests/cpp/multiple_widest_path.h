/*
 * ===============================================================
 *    Description:  Multiple widest path requests.
 *
 *        Created:  06/05/2013 05:35:52 PM
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
#include "node_prog/dijkstra_program.h"
#include "test_base.h"

#define WP_REQUESTS 20

void
multiple_wp_prog(bool to_exit)
{
    client c(CLIENT_ID);
    int i, num_nodes, num_edges;
    const uint32_t weight_label = 42;
    timespec first, t1, t2, dif;
    std::vector<uint64_t> nodes, edges;
    srand(time(NULL));
    std::ifstream count_in;
    std::ofstream count_out;
    count_in.open("node_count.rec");
    count_in >> num_nodes;
    count_in.close();
    num_edges = (int)(5.0 * (double)num_nodes);
    for (i = 0; i < num_nodes; i++) {
        WDEBUG << "Creating node " << (i+1) << std::endl;
        nodes.emplace_back(c.create_node());
    }
    for (i = 0; i < num_edges; i++) {
        int first = rand() % num_nodes;
        int second = rand() % num_nodes;
        while (second == first) {
            second = rand() % num_nodes;
        }
        WDEBUG << "Creating edge " << (i+1) << std::endl;
        edges.emplace_back(c.create_edge(nodes[first], nodes[second]));
        c.add_edge_prop(nodes[first], edges[edges.size()-1], weight_label, rand() % 100);
    }
    WDEBUG << "Created graph\n";
    c.commit_graph();
    WDEBUG << "Committed graph\n";

    node_prog::dijkstra_params dp;
    dp.adding_nodes = false;
    dp.is_widest_path = true;
    dp.edge_weight_key = weight_label;
    std::ofstream file, req_time;
    file.open("requests.rec");
    req_time.open("time.rec");
    wclock::get_clock(&t1);
    first = t1;
    for (i = 0; i < WP_REQUESTS; i++) {
        wclock::get_clock(&t2);
        dif = diff(t1, t2);
        WDEBUG << "Test: i = " << i << ", " << dif.tv_sec << ":" << dif.tv_nsec << std::endl;
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
        std::vector<std::pair<uint64_t, node_prog::dijkstra_params>> initial_args;
        dp.src_handle = nodes[first];
        dp.dst_handle = nodes[second];
        initial_args.emplace_back(std::make_pair(nodes[first], dp));
        std::unique_ptr<node_prog::dijkstra_params> res = c.run_node_program(node_prog::DIJKSTRA, initial_args);
        WDEBUG << "Request " << i << ", from source " << nodes[first] << " to dest " << nodes[second]
            << ". cost of wp = " << res->cost << std::endl;
    }
    file.close();
    req_time.close();
    dif = diff(first, t2);
    WDEBUG << "Total time taken " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    std::ofstream stat_file;
    stat_file.open("stats.rec", std::ios::out | std::ios::app);
    stat_file << num_nodes << " " << dif.tv_sec << "." << dif.tv_nsec << std::endl;
    stat_file.close();
    if (to_exit)
        c.exit_weaver();
}
