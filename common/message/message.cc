/*
 * ===============================================================
 *    Description:  Message tester
 *
 *        Created:  11/07/2012 03:49:13 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "message.h"

int
main (int argc, char *argv[])
{
    std::unique_ptr<po6::net::location> random (new po6::net::location ("127.0.0.1", 5200));
    std::unique_ptr<po6::net::location> random2 (new po6::net::location ("127.0.0.1", 5200));
    std::unique_ptr<po6::net::location> random3 (new po6::net::location ("127.0.0.1", 5200));
    void *first = (void *)238947328978763;
    void *second = (void *)23948230489224;
    uint32_t dir;
    message::message msg (message::EDGE_CREATE_REQ);
    message::message msg3 (message::REACHABLE_REPLY);
    message::message msg4 (message::REACHABLE_PROP);
    uint32_t temp;
    uint32_t r_count = 84;
    bool reachable;
    std::vector<size_t> src_nodes, srces;
    src_nodes.push_back ((size_t)first); src_nodes.push_back ((size_t)second);
    std::cout << "sizeof bool = " << sizeof (bool) << std::endl;
    
    int ret = msg.prep_edge_create ((size_t) first, (size_t) second,
        std::move(random), message::FIRST_TO_SECOND);
    int ret2 = msg4.prep_reachable_prop (src_nodes, std::move(random2), (size_t)second,
        std::move(random3), r_count, r_count);

    random = msg.unpack_edge_create (&first, &second, &dir);
    std::cout << "Unpacking got port number " <<
        random->port << " and dir " << dir << " and addr " <<
        random->get_addr() << std::endl;
    std::cout << "First = " << first << " second " << second << std::endl;

    std::unique_ptr<po6::net::location> src, dest;
    srces = msg4.unpack_reachable_prop (&src, &second, &dest, &r_count, &r_count);
    std::cout << "\nUnpacking msg 4, got srces len " << srces.size() <<
        " second " << second << " src " << src->get_addr() << " " << src->port <<  " r_count " << r_count <<
        '\n';
    for (temp = 0; temp < srces.size(); temp++)
    {
        std::cout << temp << " src is " << (void *) srces[temp] << std::endl;
    }
}
