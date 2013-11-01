/*
 * ===============================================================
 *    Description:  Checking strong consistency with mix of reads
 *                  writes.
 *
 *        Created:  09/13/2013 04:04:16 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <po6/threads/cond.h>

#include "client/client.h"
#include "node_prog/reach_program.h"

#define CONSISTENT_CLIQUE_SZ 100

static po6::threads::mutex edge_mutex;
static bool creating_edges = true;
static po6::threads::cond edge_cond(&edge_mutex);

static
void create_clique(client::client &c)
{
    // make clique of size clique_size
    size_t nodes[CLIQUE_SIZE];
    uint64_t tx_id = c.begin_tx();
    for (int i = 0; i < CLIQUE_SIZE; i++) {
        nodes[i] = c.create_node(tx_id);
    }
    c.end_tx(tx_id);
    WDEBUG << "created clique nodes" << std::endl;
    tx_id = c.begin_tx();
    for (int i = 0; i < CLIQUE_SIZE; i++) {
        for (int j = 0; j < CLIQUE_SIZE; j++) {
            if (i != j) {
                c.create_edge(tx_id, nodes[i], nodes[j]);
            }
        }
    }
    edge_mutex.lock();
    creating_edges = false;
    edge_cond.signal();
    edge_mutex.unlock();
    c.end_tx(tx_id);
    WDEBUG << "created clique edges" << std::endl;

}

void clique_reachability(client::client &c, bool reachable)
{
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
            WDEBUG << "running clique reachability from " << i << " to " << j << std::endl;
            std::unique_ptr<node_prog::reach_params> res = c.run_node_program(node_prog::REACHABILITY, initial_args);
            assert(res->reachable == reachable);
            }
        }
    }
}

void try_reach(client::client &c)
{
    edge_mutex.lock();
    while(!creating_edges) {
        edge_cond.wait();
    }
    // creating edges concurrently, all reach requests should fail
    clique_reachability(c, false);
    while(creating_edges) {
        // wait for edge creation to complete
        edge_cond.wait();
    }
    // create edges done, all reach requests should pass
    clique_reachability(c, true);
    // TODO to complete, think thoroughly about this test
}
