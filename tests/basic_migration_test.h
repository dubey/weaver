/*
 * ===============================================================
 *    Description:  Repetitively send messages between two
 *                  nodes and check that they end up on the same
 *                  shard.
 *
 *        Created:  06/13/2013 01:09:44 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"

inline void
basic_migration_test()
{
    client c(CLIENT_ID);
    uint64_t n1, n2;
    std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args;
    n1 = c.create_node();
    n2 = c.create_node();
    assert(c.get_node_loc(n1) != c.get_node_loc(n2));
    initial_args.emplace_back(std::make_pair(n1, node_prog::reach_params()));
    initial_args.at(0).second.mode = false;
    initial_args.at(0).second.reachable = false;
    initial_args.at(0).second.prev_node.loc = COORD_ID;
    initial_args.at(0).second.dest = n2;
    for (int i = 0; i < 10000; i++) {
        node_prog::reach_params *res = c.run_node_program(node_prog::REACHABILITY, initial_args);
        assert(res->reachable);
        delete res;
    }
    assert(c.get_node_loc(n1) == c.get_node_loc(n2));
}
