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

void
basic_client_test()
{
    client c;
    size_t nodes[10];
    size_t edges[10];
    int i;
    for (i = 0; i < 10; i++)
    {
        nodes[i] = c.create_node();
    }
    edges[0] = c.create_edge(nodes[0], nodes[1]);
    edges[1] = c.create_edge(nodes[1], nodes[2]);
    assert(c.reachability_request(nodes[0], nodes[2]));
    c.delete_edge(nodes[1],edges[1]);
    //std::cout << "starting req2\n";
    assert(!c.reachability_request(nodes[0], nodes[2]));
}
