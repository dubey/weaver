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
#include <po6/threads/mutex.h>
#include "test_base.h"

//static uint64_t sc_num_clients;
#define SC_CLIENT_OFF 100
//#define SC_NUM_NODES 1000
#define OPS_PER_CLIENT 10000
#define PERCENT_READS 0
#define NUM_CLIENTS 10
#define NUM_NEW_EDGES 10

static double stats[OPS_PER_CLIENT][NUM_CLIENTS];
static po6::threads::mutex monitor;
static std::vector<uint64_t> nodes = std::vector<uint64_t>();

static void add_node(uint64_t n) {
    monitor.lock();
    nodes.emplace_back(n);
    monitor.unlock();
}

static void getRandomNodes(size_t num, std::vector<uint64_t>& toFill) {
    std::vector<size_t> idxs = std::vector<size_t>(num);
    monitor.lock();
    assert(nodes.size() >= num);
    assert(toFill.empty());
    while (idxs.size() < num) {
        size_t toAdd = rand() % nodes.size();
        // don't have duplicates
        bool add = true;
        for (auto i : idxs)
            add = add && (i != toAdd);

        if (add)
            idxs.push_back(toAdd);
    }
    for(auto i : idxs)
        toFill.push_back(nodes.at(i));
}

void
scale_client(int client_id, std::vector<uint64_t>& tx_times)
{
    client::client c(client_id + SC_CLIENT_OFF, client_id % NUM_VTS);
    uint64_t tx_id, n1;
    if (client_id == 0) {
        tx_id = c.begin_tx();
        for(int i = 0; i < NUM_NEW_EDGES ; i++) {
            add_node(c.create_node(tx_id));
        }
        c.end_tx(tx_id);
    }
    //uint64_t first, second;
    int num_ops = 0;
    while (num_ops < OPS_PER_CLIENT) {
        DEBUG << "Client " << client_id << " finished " << num_ops << " ops" << std::endl;
        // do writes
        for (int j = 0; j < 100-PERCENT_READS; j++) {
            std::vector<uint64_t> out_nbrs = std::vector<uint64_t>();
            getRandomNodes(NUM_NEW_EDGES/2, out_nbrs);
            std::vector<uint64_t> in_nbrs = std::vector<uint64_t>();
            getRandomNodes(NUM_NEW_EDGES/2, in_nbrs);

            //System.out.println("node " + node + " with nieghbors " + out_nbrs + " and " + in_nbrs);
           // long start = System.nanoTime();
            tx_id = c.begin_tx();
            n1 = c.create_node(tx_id);
            for (uint64_t nbr : out_nbrs)
                c.create_edge(tx_id, n1, nbr);
            for (uint64_t nbr : in_nbrs)
                c.create_edge(tx_id, nbr, n1);
            c.end_tx(tx_id);

            //long end = System.nanoTime();
            //stats[num_ops][proc] = (end-start) / 1e6;
            num_ops++;
            add_node(n1);
        }
        // do reads
        for (int j = 0; j < PERCENT_READS; j++) {
            std::cerr << "reads not implemented yet" << std::endl;
        }
    }

/*
    uint64_t start, cur, prev, diff;
    timespec t;
    start = wclock::get_time_elapsed(t);
    prev = start;
    DEBUG << "Client " << client_id << " starting" << std::endl;
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
    DEBUG << "Client " << client_id << " finishing" << std::endl;
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
    */
}

void
scale_test()
{
    std::ifstream ncli;
    /*
    ncli.open("sc_num_clients.rec");
    ncli >> sc_num_clients;
    ncli.close();
    */
    std::vector<uint64_t> tx_times[NUM_CLIENTS];
    std::thread *t[NUM_CLIENTS];
    for (uint64_t i = 0; i < NUM_CLIENTS; i++) {
        t[i] = new std::thread(scale_client, i, &tx_times[i]);
    }
    for (uint64_t i = 0; i < NUM_CLIENTS; i++) {
        t[i]->join();
        assert(tx_times[i].size() == OPS_PER_CLIENT);
    }
    /*
    double avg_tx_time = 0;
    for (uint64_t i = 0; i < NUM_CLIENTS; i++) {
        for (uint64_t j = 0; j < SC_NUM_NODES-1; j++) {
            avg_tx_time += tx_times[i][j];
        }
    }
    */
}
