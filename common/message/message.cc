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
    void *first = (void *)238947328978763;
    void *second = (void *)23948230489224;
    uint64_t time = 42;
    uint64_t time2 = time;
    uint64_t time3 = time;
    std::unique_ptr<po6::net::location> random(new po6::net::location("127.0.0.1", 5200));
    std::unique_ptr<po6::net::location> random2(new po6::net::location("127.0.0.1", 5200));
    std::unique_ptr<po6::net::location> random3(new po6::net::location("127.0.0.1", 5200));
    std::unique_ptr<po6::net::location> random4(new po6::net::location("127.0.0.1", 5200));
    std::unique_ptr<po6::net::location> random5(new po6::net::location("127.0.0.1", 5200));
    std::unique_ptr<po6::net::location> random6(new po6::net::location("127.0.0.1", 5200));
    uint32_t dir = 1;
    std::unique_ptr<db::element::meta_element> node1, node2;
    std::vector<size_t> src_nodes;
    std::unique_ptr<std::vector<size_t>> srces;
    std::vector<uint64_t> vc;
    std::unique_ptr<std::vector<uint64_t>> vc_ptr;
    std::unique_ptr<std::vector<size_t>> dn1(new std::vector<size_t>());
    std::unique_ptr<std::vector<size_t>> dn2(new std::vector<size_t>());
    std::unique_ptr<std::vector<uint64_t>> dt1(new std::vector<uint64_t>());
    std::unique_ptr<std::vector<uint64_t>> dt2(new std::vector<uint64_t>());
    uint32_t temp;
    uint32_t r_count = 84;
    bool reachable;
    size_t rec_node;
    std::unique_ptr<po6::net::location> src;
    std::shared_ptr<po6::net::location> dest;
    src_nodes.push_back((size_t)first); src_nodes.push_back((size_t)second);
    vc.push_back(24); vc.push_back(42);
    dn1->push_back((size_t)first); dn1->push_back((size_t)second);
    dt1->push_back(35); dt1->push_back(45);
    
    // node create
    message::message msg(message::ERROR);
    msg.prep_node_create(time);
    msg.unpack_node_create(&time);
    assert(time2 == time);

    // edge create
    msg.prep_edge_create((size_t)first, (size_t)second, std::move(random), time,
        time, (enum message::edge_direction)dir, time);
    msg.unpack_edge_create(&node1, &node2, std::move(random2), &dir, &time2);
    assert(dir == 1);
    assert(time2 == time);

    // reachable prop
    msg.prep_reachable_prop(src_nodes, std::move(random3), (size_t)second,
        std::move(random4), r_count, r_count, vc);
    srces = msg.unpack_reachable_prop(&src, &second, &dest, &r_count,
        &r_count, &vc_ptr);
    std::cout << "\nUnpacking msg reachable_prop, got srces len " << srces->size() <<
        " second " << second << " src " << src->get_addr() << " " << src->port <<  " r_count " << r_count <<
        '\n';
    for (temp = 0; temp < srces->size(); temp++)
    {
        std::cout << temp << " src is " << (void *)srces->at(temp) << std::endl;
    }
    for (temp = 0; temp < NUM_SHARDS; temp++)
    {
        std::cout << temp << " vc is " << (*vc_ptr)[temp] << std::endl;
    }
    
    // reachable rep
    msg.prep_reachable_rep(42, true, (size_t)first, std::move(random5), std::move(dn1),
        std::move(dt1));
    msg.unpack_reachable_rep(&r_count, &reachable, &rec_node, &rec_node, &dn2,
        &dt2);
    for (temp = 0; temp < dn2->size(); temp++)
    {
        std::cout << "del node " << (void*)dn2->at(temp) << " del time " <<
            dt2->at(temp) << std::endl;
    }
}
