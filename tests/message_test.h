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

#include "common/message.h"

void
message_test()
{
    void *first = (void *)238947328978763;
    void *second = (void *)23948230489224;
    void *first1 = (void *)238947328978763;
    void *second1 = (void *)23948230489224;
    uint64_t time = 42;
    uint64_t time2 = time;
    uint64_t time3 = time;
    std::unique_ptr<po6::net::location> random(new po6::net::location("127.0.0.1", 5200));
    std::shared_ptr<po6::net::location> random2(new po6::net::location("127.0.0.1", 5200));
    std::shared_ptr<po6::net::location> random3(new po6::net::location("127.0.0.1", 5200));
    std::shared_ptr<po6::net::location> random4(new po6::net::location("127.0.0.1", 5200));
    std::shared_ptr<po6::net::location> random5(new po6::net::location("127.0.0.1", 5200));
    std::shared_ptr<po6::net::location> random6(new po6::net::location("127.0.0.1", 5200));
    uint32_t dir = 1;
    std::unique_ptr<common::meta_element> node1, node2;
    std::vector<size_t> src_nodes;
    std::unique_ptr<std::vector<size_t>> srces;
    std::shared_ptr<std::vector<uint64_t>> vc(new std::vector<size_t>());
    std::shared_ptr<std::vector<uint64_t>> vc_ptr;
    size_t num_del_nodes;
    std::unique_ptr<std::vector<size_t>> dn1(new std::vector<size_t>());
    std::unique_ptr<std::vector<size_t>> dn2(new std::vector<size_t>());
    std::unique_ptr<std::vector<uint64_t>> dt1(new std::vector<uint64_t>());
    std::unique_ptr<std::vector<uint64_t>> dt2(new std::vector<uint64_t>());
    std::vector<size_t> dn;
    std::vector<uint64_t> dt;
    auto edge_props = std::make_shared<std::vector<common::property>>();
    auto edge_props_rec = std::make_shared<std::vector<common::property>>();
    uint32_t temp;
    size_t r_count = 84;
    bool reachable;
    size_t rec_node;
    size_t req_id = 42;
    size_t rec_id = req_id;
    std::unique_ptr<po6::net::location> src;
    std::shared_ptr<po6::net::location> dest;
    src_nodes.push_back((size_t)first); src_nodes.push_back((size_t)second);
    for (temp = 0; temp < NUM_SHARDS; temp++)
    {
        vc->push_back(42+temp);
    }
    dn1->push_back((size_t)first); dn1->push_back((size_t)second);
    dn = *dn1;
    dt1->push_back(35); dt1->push_back(45);
    dt = *dt1;
    
    // node create
    message::message msg(message::ERROR);
    msg.prep_node_create(rec_id, time);
    msg.unpack_node_create(&rec_id, &time);
    assert(rec_id == req_id);
    assert(time == time2);

    // edge create
    size_t s_first1;
    msg.prep_edge_create(rec_id, (size_t)first, (size_t)second, random2, time, time);
    msg.unpack_edge_create(&rec_id, &s_first1, &node2, &time2);
    assert(rec_id == req_id);
    assert(first == (void *)s_first1);
    assert(time2 == time);
    assert(*random == *node2->get_loc_ptr());

    // node delete
    // TODO

    // edge delete
    msg.prep_edge_delete(rec_id, (size_t)first, (size_t)second, time);
    msg.unpack_edge_delete(&rec_id, &first1, &second1, &time2);
    assert(rec_id == req_id);
    assert(first == first1);
    assert(second == second1);
    assert(time2 == time);

    // reachable prop
    common::property prop(42, 84, 1007);
    edge_props->push_back(prop);
    msg.prep_reachable_prop(&src_nodes, random3, (size_t)second,
        random4, r_count, r_count, edge_props, vc);
    srces = msg.unpack_reachable_prop(&src, &second1, &dest, &r_count,
        &r_count, &edge_props_rec, &vc_ptr, 1);
    assert(src_nodes == *srces);
    assert(second == second1);
    assert(*src == *random3);
    assert(*dest == *random4);
    assert(*vc_ptr == *vc);
    assert(*edge_props == *edge_props_rec);
    
    // reachable rep
    msg.prep_reachable_rep(42, true, (size_t)first, random5, std::move(dn1),
        std::move(dt1));
    msg.unpack_reachable_rep(&r_count, &reachable, &rec_node, &num_del_nodes, &dn2,
        &dt2);
    assert(r_count == 42);
    assert(reachable);
    assert(rec_node == (size_t)first);
    assert(num_del_nodes == dn2->size());
    assert(*dn2 == dn);
    assert(*dt2 == dt);
}
