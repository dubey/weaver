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
    message::message msg3 (message::REACHABLE_REPLY);
    message::message msg4 (message::REACHABLE_PROP);
    uint32_t temp;
    uint32_t r_count = 84;
    bool reachable;
    std::vector<size_t> src_nodes, srces;
    std::vector<uint64_t> vc;
    std::unique_ptr<std::vector<uint64_t>> vc_ptr;
    std::unique_ptr<std::vector<size_t>> dn1(new std::vector<size_t>());
    std::unique_ptr<std::vector<size_t>> dn2(new std::vector<size_t>());
    std::unique_ptr<std::vector<uint64_t>> dt1(new std::vector<uint64_t>());
    std::unique_ptr<std::vector<uint64_t>> dt2(new std::vector<uint64_t>());
    src_nodes.push_back ((size_t)first); src_nodes.push_back ((size_t)second);
    vc.push_back(24); vc.push_back(42);
    dn1->push_back((size_t)first); dn1->push_back((size_t)second);
    dt1->push_back(35); dt1->push_back(45);
    size_t rec_node;
    std::cout << "sizeof bool = " << sizeof (bool) << std::endl;
    
    //int ret = msg.prep_edge_create ((size_t) first, (size_t) second,
    //    std::move(random), message::FIRST_TO_SECOND);
    int ret2 = msg4.prep_reachable_prop (src_nodes, std::move(random2), (size_t)second,
        std::move(random3), r_count, r_count, vc);
    msg3.prep_reachable_rep(42, true, (size_t)first, std::move(random), std::move(dn1),
    std::move(dt1));

    /*
    random = msg.unpack_edge_create (&first, &second, &dir);
    std::cout << "Unpacking got port number " <<
        random->port << " and dir " << dir << " and addr " <<
        random->get_addr() << std::endl;
    std::cout << "First = " << first << " second " << second << std::endl;
    */

    std::unique_ptr<po6::net::location> src, dest;
    srces = msg4.unpack_reachable_prop (&src, &second, &dest, &r_count,
        &r_count, &vc_ptr);
    std::cout << "\nUnpacking msg 4, got srces len " << srces.size() <<
        " second " << second << " src " << src->get_addr() << " " << src->port <<  " r_count " << r_count <<
        '\n';
    for (temp = 0; temp < srces.size(); temp++)
    {
        std::cout << temp << " src is " << (void *) srces[temp] << std::endl;
    }
    for (temp = 0; temp < NUM_SHARDS; temp++)
    {
        std::cout << temp << " vc is " << (*vc_ptr)[temp] << std::endl;
    }

    msg3.unpack_reachable_rep(&r_count, &reachable, &rec_node, &rec_node, &dn2,
    &dt2);
    for (temp = 0; temp < dn2->size(); temp++)
    {
        std::cout << "del node " << (void*)dn2->at(temp) << " del time " <<
            dt2->at(temp) << std::endl;
    }
}
