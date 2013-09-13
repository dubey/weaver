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
#include "node_prog/reach_program.h"
#define LINE_LENGTH 100
#define CLIQUE_SIZE 100

void line_reachability(client::client &c) {
    // make line of length line_length
    uint64_t tx_id = c.begin_tx();
    size_t nodes[LINE_LENGTH];
    for (int i = 0; i < LINE_LENGTH; i++) {
        nodes[i] = c.create_node(tx_id);
    }
    c.end_tx(tx_id);

    tx_id = c.begin_tx();
    for (int i = 0; i < LINE_LENGTH-1; i++) {
        c.create_edge(tx_id, nodes[i], nodes[i+1]);
    }
    c.end_tx(tx_id);

    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    rp.dest = nodes[LINE_LENGTH-1];

    std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
    initial_args.emplace_back(std::make_pair(nodes[0], rp));
    DEBUG << "starting line reachability" << std::endl;
    std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
    assert(res->reachable);

    DEBUG << "finished positive line reachability" << std::endl;
    // try to go from second node in line back to first (not possible because singly linked)
    rp.dest = nodes[0];
    initial_args.clear();
    initial_args.emplace_back(std::make_pair(nodes[1], rp));
     res = c.run_node_program(node_prog::REACHABILITY, initial_args);
    assert(!res->reachable);
    DEBUG << "finished negative line reachability" << std::endl;
}

void clique_reachability(client::client &c) {
    // make clique of size clique_size
    size_t nodes[CLIQUE_SIZE];
    uint64_t tx_id = c.begin_tx();
    for (int i = 0; i < CLIQUE_SIZE; i++) {
        nodes[i] = c.create_node(tx_id);
    }
    c.end_tx(tx_id);

    DEBUG << "created clique nodes" << std::endl;
    for (int i = 0; i < CLIQUE_SIZE; i++) {
        tx_id = c.begin_tx();
        for (int j = 0; j < CLIQUE_SIZE; j++) {
            if (i != j) {
                c.create_edge(tx_id, nodes[i], nodes[j]);
            }
        }
        c.end_tx(tx_id);
    }
    DEBUG << "created clique edges" << std::endl;

    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
    initial_args.emplace_back(std::make_pair(nodes[0], rp));

    int skip_by = CLIQUE_SIZE / 5;
    assert(skip_by > 0);
    for (int i = 0; i < CLIQUE_SIZE; i+=skip_by) {
        for (int j = 0; j < CLIQUE_SIZE; j+=skip_by) {
            if (i != j) {
            initial_args[0].first = nodes[i];
            initial_args[0].second.dest = nodes[j];
            DEBUG << "running clique reachability from " << i << " to " << j << std::endl;
            std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
            assert(res->reachable);
            }
        }
    }

    DEBUG << "adding extra node and edge for negative reachability test" << std::endl;
    tx_id = c.begin_tx();
    uint64_t extra_node = c.create_node(tx_id);
    c.create_edge(tx_id, extra_node, nodes[0]);
    c.end_tx(tx_id);
    initial_args[0].first = extra_node;
    initial_args[0].second.dest = nodes[CLIQUE_SIZE-1];
    std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
    assert(res->reachable);
    initial_args[0].first = nodes[0];
    initial_args[0].second.dest = extra_node;
    res = c.run_node_program(node_prog::REACHABILITY, initial_args);
    assert(!res->reachable);

}

void
new_reachability_test()
{
    client::client c(CLIENT_ID, 0);
    line_reachability(c);
    clique_reachability(c);
}
