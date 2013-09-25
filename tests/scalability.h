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
#define OPS_PER_CLIENT 1000
#define PERCENT_READS 0
#define NUM_CLIENTS 10
#define NUM_NEW_EDGES 10

//static double stats[OPS_PER_CLIENT][NUM_CLIENTS];
static po6::threads::mutex monitor;
static std::vector<uint64_t> nodes = std::vector<uint64_t>();

static void add_node(uint64_t n) {
    monitor.lock();
    nodes.emplace_back(n);
//    std::cout << nodes.size() << " is size of nodes" << std::endl;
    monitor.unlock();
}

static void getRandomNodes(const size_t num, std::vector<uint64_t>& toFill) {
    std::vector<size_t> idxs = std::vector<size_t>();
    monitor.lock();
    assert(nodes.size() >= num);
    assert(toFill.empty());
    assert(idxs.empty());
    while (idxs.size() < num) {
        size_t toAdd = rand() % nodes.size();
        // don't have duplicates
        bool add = true;
        for (auto i : idxs) {
            if (toAdd == i) {
                add = false;
                break;
            }
        }
        if (add) {
            //std::cout << "randomly picked node " << toAdd << std::endl;
            idxs.push_back(toAdd);
        }
    }
    for(auto i : idxs) {
        toFill.push_back(nodes.at(i));
    }
    monitor.unlock();
    assert(toFill.size() == num);
}

void
scale_client(int client_id, std::vector<uint64_t>*)
{
    client::client c(client_id + SC_CLIENT_OFF, client_id % NUM_VTS);
    uint64_t tx_id, n1;
    //timespec t;
    //uint64_t start, cur;
    int num_ops = 0;
    while (num_ops < OPS_PER_CLIENT) {
        // do writes
        for (int j = 0; j < 100-PERCENT_READS; j++) {
            std::vector<uint64_t> out_nbrs = std::vector<uint64_t>();
            getRandomNodes(NUM_NEW_EDGES/2, out_nbrs);
            std::vector<uint64_t> in_nbrs = std::vector<uint64_t>();
            getRandomNodes(NUM_NEW_EDGES/2, in_nbrs);

            //std::ostringstream towrite;
            //start = wclock::get_time_elapsed(t);
            tx_id = c.begin_tx();
            n1 = c.create_node(tx_id);
            //towrite << n1 << " has neighbors:";
            for (uint64_t nbr : out_nbrs) {
                c.create_edge(tx_id, n1, nbr);
                //towrite << " " << nbr;
            }
            //towrite << " out and in ";
            for (uint64_t nbr : in_nbrs) {
                c.create_edge(tx_id, nbr, n1);
                //towrite <<  " " <<  nbr;
            }
            //towrite << "\n";
            //std::cout << towrite.str();
            c.end_tx(tx_id);
            //cur = wclock::get_time_elapsed(t);
            //tx_times->emplace_back(cur-start);

            num_ops++;
            add_node(n1);
        }
        // do reads
        for (int j = 0; j < PERCENT_READS; j++) {
            std::cerr << "reads not implemented yet" << std::endl;
        }
        DEBUG << "Client " << client_id << " finished " << num_ops << " ops" << std::endl;
    }
}

void
scale_test()
{
    /*
    std::ifstream ncli;
    ncli.open("sc_num_clients.rec");
    ncli >> sc_num_clients;
    ncli.close();
    */
    client::client c(NUM_CLIENTS + SC_CLIENT_OFF, 0);
    uint64_t tx_id;
    // make first 10 nodes
    tx_id = c.begin_tx();
    for(int i = 0; i < NUM_NEW_EDGES ; i++) {
        add_node(c.create_node(tx_id));
    }
    c.end_tx(tx_id);

    std::vector<uint64_t> tx_times[NUM_CLIENTS];
    std::thread *t[NUM_CLIENTS];
    timespec ts;
    uint64_t start = wclock::get_time_elapsed_millis(ts);
    for (uint64_t i = 0; i < NUM_CLIENTS; i++) {
        t[i] = new std::thread(scale_client, i, &tx_times[i]);
    }
    for (uint64_t i = 0; i < NUM_CLIENTS; i++) {
        t[i]->join();
        //assert(tx_times[i].size() == OPS_PER_CLIENT);
    }
    uint64_t end = wclock::get_time_elapsed_millis(ts);
    DEBUG << "Time taken = " << (end - start) << std::endl;
    //std::ofstream stats;
    //stats.open("throughputlatency.rec");
    //for (uint64_t i = 0; i < NUM_CLIENTS; i++) {
    //    for (uint64_t j = 0; j < OPS_PER_CLIENT; j++) {
    //        stats <<  tx_times[i][j]/1e6 << std::endl;
    //    }
    //}
    //stats.close();
}
