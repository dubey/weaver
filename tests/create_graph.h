/*
 * ===============================================================
 *    Description:  Simple create node and edge test.
 *
 *        Created:  09/11/2013 01:48:38 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <thread>

#include "client/client.h"

void
create_elems(client::client *c)
{
    int i;
    size_t nodes[10];
    // create nodes
    for (i = 0; i < 10; i++) {
        uint64_t tx_id = c->begin_tx();
        nodes[i] = c->create_node(tx_id);
        DEBUG << "Created node, handle = " << nodes[i] << std::endl;
        c->end_tx(tx_id);
    }
    // edges
    uint64_t tx_id = c->begin_tx();
    for (i = 0; i < 10; i++) {
        c->create_edge(tx_id, nodes[i], nodes[(i+1)%10]);
    }
    c->end_tx(tx_id);
    for (i = 0; i < 10; i+=2) {
        uint64_t tx_id = c->begin_tx();
        nodes[i] = c->create_node(tx_id);
        DEBUG << "Created node, handle = " << nodes[i] << std::endl;
        nodes[i+1] = c->create_node(tx_id);
        DEBUG << "Created node, handle = " << nodes[i+1] << std::endl;
        c->end_tx(tx_id);
    }
}

void
create_graph_test()
{
    int i;
    if (NUM_VTS == 1) {
        DEBUG << "single vts" << std::endl;
        client::client c(CLIENT_ID, 0);
        create_elems(&c);
    } else {
        // multiple clients
        DEBUG << "multiple vts, multiple clients" << std::endl;
        client::client* cl[50];
        for (i = 0; i < 50; i++) {
            cl[i] = new client::client(CLIENT_ID+i, (i % 2 == 0) ? 0 : 1);
        }
        std::thread* t[50];
        for (i = 0; i < 50; i++) {
            t[i] = new std::thread(create_elems, cl[i]);
        }
        for (i = 0; i < 50; i++) {
            t[i]->join();
        }
    }
}
