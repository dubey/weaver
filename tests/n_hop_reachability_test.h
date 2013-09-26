/*
 * ===============================================================
 *    Description:  Simple reachability program test.
 *
 *        Created:  09/11/2013 01:48:38 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/n_hop_reach_program.h"
#define LINE_LENGTH 42

void line_n_hop_reachability(client::client &c) {
    // make line of length line_length
    std::cout << "line reachability makign graph" << std::endl;
    uint64_t tx_id = c.begin_tx();
    size_t nodes[LINE_LENGTH];
    for (int i = 0; i < LINE_LENGTH; i++) {
        nodes[i] = c.create_node(tx_id);
    }
    std::cout << "sending node transation" << std::endl;
    c.end_tx(tx_id);
    std::cout << "made nodes" << std::endl;

    tx_id = c.begin_tx();
    for (int i = 0; i < LINE_LENGTH-1; i++) {
        c.create_edge(tx_id, nodes[i], nodes[i+1]);
    }
    c.end_tx(tx_id);

    for (int max_hops = 1; max_hops < LINE_LENGTH-1; max_hops++) {
        for (int dist = 1; dist < LINE_LENGTH-1; dist++) {
            node_prog::n_hop_reach_params rp;
            std::vector<std::pair<uint64_t, node_prog::n_hop_reach_params>> initial_args;
            rp.returning = false;
            rp.reachable = false;
            rp.prev_node.loc = COORD_ID;
            rp.hops = 0;
            rp.max_hops = max_hops;
            rp.dest = nodes[dist];
            initial_args.emplace_back(std::make_pair(nodes[0], rp));
            std::cout << "line reachability, nodes " << dist << "hops apart with max hops " << max_hops;
            std::unique_ptr<node_prog::n_hop_reach_params> res = c.run_node_program(node_prog::N_HOP_REACHABILITY, initial_args);
            std::cout << "... was " << res->reachable << std::endl << std::endl;
            assert(res->reachable == (max_hops >= dist));
        }
    }
}

void
n_hop_reachability_test()
{
    client::client c(CLIENT_ID, 0);
    line_n_hop_reachability(c);
}
