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

void
new_reachability_test()
{
    client::client c(CLIENT_ID);
    uint64_t tx_id = c.begin_tx();
    size_t node1 = c.create_node(tx_id);
    size_t node2 = c.create_node(tx_id);
    size_t edge = c.create_edge(tx_id, node1, node2);
    DEBUG << "Created node1 " << node1 << ", node1 " << node2 << ", and edge from node1 to node2 " << edge << std::endl;
    c.end_tx(tx_id);

    node_prog::reach_params rp;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    rp.dest = node2;

    std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
    initial_args.emplace_back(std::make_pair(node1, rp));
    std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
    assert(res->reachable);
}
