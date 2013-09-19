/*
 * ===============================================================
 *    Description:  Multiple clients doing transactions with
 *                  multiple timestampers.
 *
 *        Created:  09/18/2013 01:46:12 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"
#include "test_base.h"

#define SC_NUM_CLIENTS 3
static uint64_t sc_num_clients;
#define SC_CLIENT_OFF 100
#define SC_NUM_NODES 1000
#define SC_NUM_EDGES (10*SC_NUM_NODES)

void
scale_client(int client_id, std::vector<uint64_t> *tx_times)
{
    client::client c(client_id + SC_CLIENT_OFF, client_id);
    uint64_t tx_id, n1, n2;
    //uint64_t first, second;
    uint64_t start, cur, prev, diff;
    timespec t;
    start = wclock::get_time_elapsed(t);
    prev = start;
    for (int i = 0; i < SC_NUM_NODES; i++) {
        tx_id = c.begin_tx();
        n1 = c.create_node(tx_id);
        n2 = c.create_node(tx_id);
        c.create_edge(tx_id, n1, n2);
        c.end_tx(tx_id);
        cur = wclock::get_time_elapsed(t);
        diff = cur - prev;
        tx_times->emplace_back(diff);
    }
    //for (int i = 0; i < SC_NUM_EDGES) {
    //    first = rand() % SC_NUM_NODES;
    //    second = rand() % SC_NUM_NODES;
    //    while (second == first) {
    //        second = rand() % SC_NUM_NODES;
    //    }
    //    tx_id = c.begin_tx();
    //    c.create_(tx_id);
    //    c.end_tx(tx_id);
    //}
}

void
scale_test()
{
    std::ifstream ncli;
    ncli.open("sc_num_clients.rec");
    ncli >> sc_num_clients;
    ncli.close();
    std::vector<uint64_t> tx_times[sc_num_clients];
    std::thread *t[sc_num_clients];
    for (uint64_t i = 0; i < sc_num_clients; i++) {
        t[i] = new std::thread(scale_client, i, &tx_times[i]);
    }
    for (uint64_t i = 0; i < sc_num_clients; i++) {
        t[i]->join();
        assert(tx_times[i].size() == SC_NUM_NODES);
    }
    double avg_tx_time = 0;
    for (uint64_t i = 0; i < sc_num_clients; i++) {
        for (uint64_t j = 0; j < SC_NUM_NODES-1; j++) {
            avg_tx_time += tx_times[i][j];
        }
    }
    avg_tx_time /= (SC_NUM_NODES * sc_num_clients);
}
