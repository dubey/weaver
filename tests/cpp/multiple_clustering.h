/*
 * ===============================================================
 *    Description:  Multiple clustering progrs.
 *
 *        Created:  10/09/2013 10:46:20 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"
#include <vector>
#include "node_prog/node_prog_type.h"
#include "node_prog/clustering_program.h"
#include "test_base.h"

#define MC_OPS_PER_CLIENT 10000
#define MC_NUM_CLIENTS 5
#define MC_PERCENT_READS 99
#define MC_EDGES_TO_NODES 100

static po6::threads::mutex gr_mutex;

void
single_clustering(uint64_t client_id, test_graph *g)
{
    uint64_t vt_id = client_id % NUM_VTS;
    client::client c(client_id, vt_id);

    std::vector<std::pair<uint64_t, node_prog::clustering_params>> initial_args;
    initial_args.emplace_back(std::make_pair(0, node_prog::clustering_params()));
    initial_args[0].second.is_center = true;
    initial_args[0].second.outgoing = true;
    initial_args[0].second.vt_id = vt_id;
    std::unique_ptr<node_prog::clustering_params> res;

    uint64_t num_ops = 0;
    while (num_ops < MC_OPS_PER_CLIENT) {
        // writes
        for (int j = 0; j < 100-MC_PERCENT_READS; j++) {
            gr_mutex.lock();
            uint64_t nbrs[10];
            for (int i = 0; i < 10; i++) {
                nbrs[i] = g->nodes[rand() % g->nodes.size()];
            }
            gr_mutex.unlock();
            uint64_t tx = c.begin_tx();
            uint64_t new_node = c.create_node(tx);
            for (int i = 0; i < 10; i++) {
                c.create_edge(tx, new_node, nbrs[i]);
            }
            c.end_tx(tx);
            gr_mutex.lock();
            g->nodes.emplace_back(new_node);
            gr_mutex.unlock();
            num_ops++;
        }
        // reads
        for (int j = 0; j < MC_PERCENT_READS; j++) {
            gr_mutex.lock();
            uint64_t num_nodes = g->nodes.size();
            uint64_t center = rand() % g->nodes.size();
            initial_args[0].first = g->nodes[center];
            gr_mutex.unlock();
            res = c.run_node_program(node_prog::CLUSTERING, initial_args);
            if (num_ops % 303 == 0) {
                DEBUG << "Client " << client_id << " completed test " << num_ops
                    << ", lcc = " << res->clustering_coeff << ", num_nodes = " << num_nodes << std::endl;
            }
            num_ops++;
        }
    }
}

void
multiple_clustering()
{
    uint64_t num_nodes = 1000;
    uint64_t num_edges = MC_EDGES_TO_NODES * num_nodes;
    client::client c(MC_NUM_CLIENTS + CLIENT_ID, 0);
    test_graph *g = new test_graph(&c, time(NULL), num_nodes, num_edges, false, false);
    std::thread *t[MC_NUM_CLIENTS];

    timespec ts;
    uint64_t start = wclock::get_time_elapsed_millis(ts);
    for (int i = 0; i < MC_NUM_CLIENTS; i++) {
        t[i] = new std::thread(single_clustering, i + CLIENT_ID, g);
    }
    for (int i = 0; i < MC_NUM_CLIENTS; i++) {
        t[i]->join();
    }
    uint64_t end = wclock::get_time_elapsed_millis(ts);
    DEBUG << "Time taken = " << (end-start) << std::endl;
    double div = MC_NUM_CLIENTS * MC_OPS_PER_CLIENT;
    DEBUG << "Per op = " << (end-start)/div << std::endl;
}
