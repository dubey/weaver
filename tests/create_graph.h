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

#include "client/client.h"

void
create_graph_test()
{
    client::client c(CLIENT_ID);
    size_t nodes[10];
    size_t edges[10];
    int i;
    // create nodes
    for (i = 0; i < 10; i++) {
        uint64_t tx_id = c.begin_tx();
        nodes[i] = c.create_node(tx_id);
        DEBUG << "Created node, handle = " << nodes[i] << std::endl;
        c.end_tx(tx_id);
    }
    // edges
    uint64_t tx_id = c.begin_tx();
    for (i = 0; i < 10; i++) {
        edges[i] = c.create_edge(tx_id, nodes[i], nodes[(i+1)%10]);
    }
    c.end_tx(tx_id);
    for (i = 0; i < 10; i+=2) {
        uint64_t tx_id = c.begin_tx();
        nodes[i] = c.create_node(tx_id);
        DEBUG << "Created node, handle = " << nodes[i] << std::endl;
        nodes[i+1] = c.create_node(tx_id);
        DEBUG << "Created node, handle = " << nodes[i+1] << std::endl;
        c.end_tx(tx_id);
    }
    DEBUG << "Created edge, handle = " << edges[0] << std::endl;
}
