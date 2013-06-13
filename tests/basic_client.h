/*
 * ===============================================================
 *    Description:  Basic graph db test
 *
 *        Created:  01/23/2013 01:20:10 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"
#include "node_prog/reach_program.h"

bool
execute_request(uint64_t n1, uint64_t n2, client &c)
{
    bool ret;
    node_prog::reach_params rp, *res;
    std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
    rp.mode = false;
    rp.reachable = false;
    rp.prev_node.loc = COORD_ID;
    rp.dest = n2;
    initial_args.emplace_back(std::make_pair(n1, rp));
    res = c.run_node_program(node_prog::REACHABILITY, initial_args);
    ret = res->reachable;
    delete res;
    return ret;
}

void
basic_client_test()
{
    client c(CLIENT_ID);
    auto edge_props = std::make_shared<std::vector<common::property>>();
    size_t nodes[10];
    size_t edges[10];
    int i;
    // create nodes
    for (i = 0; i < 10; i++) {
        nodes[i] = c.create_node();
    }
    // request 1
    edges[0] = c.create_edge(nodes[0], nodes[1]);
    edges[1] = c.create_edge(nodes[1], nodes[2]);
    assert(execute_request(nodes[0], nodes[2], c));
    // request 2
    c.delete_edge(nodes[0], edges[0]);
    c.delete_edge(nodes[1], edges[1]);
    assert(!execute_request(nodes[0], nodes[2], c));
    // requests 3-5
    edges[0] = c.create_edge(nodes[0], nodes[2]);
    edges[1] = c.create_edge(nodes[1], nodes[3]);
    assert(execute_request(nodes[0], nodes[2], c));
    assert(execute_request(nodes[1], nodes[3], c));
    assert(!execute_request(nodes[3], nodes[1], c));
}
