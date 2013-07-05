/*
 * ===============================================================
 *    Description:  Testing node prog cache using reachability
 *                  program.
 *
 *        Created:  12/04/2012 11:09:50 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <iostream>
#include "db/cache/program_cache.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_run_db(db::graph*, message::message&)
{ }
template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_start_coord(coordinator::central*, std::shared_ptr<coordinator::pending_req>)
{ }

void
cache_test()
{
    cache::program_cache c;
    uint64_t src_node = 21;
    uint64_t dst_node = 134;
    uint64_t req_id = 42;
    node_prog::prog_type ptype = node_prog::REACHABILITY;
    std::shared_ptr<node_prog::reach_cache_value> rcv, rcv2;
    std::shared_ptr<node_prog::CacheValueBase> cvb;
    std::vector<std::shared_ptr<node_prog::CacheValueBase>> vec_cvb;
    std::unordered_set<uint64_t> ignore;
    std::vector<uint64_t> *dirty_list = new std::vector<uint64_t>();
    // sanity check, start with empty cache
    assert(!c.cache_exists(ptype, src_node, req_id));
    // simple put and get
    rcv.reset(new node_prog::reach_cache_value());
    rcv->reachable_node = dst_node;
    c.put_cache(req_id, ptype, src_node, rcv);
    c.commit(req_id);
    assert(c.cache_exists(ptype, src_node, req_id));
    cvb = c.single_get_cache(ptype, src_node, req_id);
    rcv2 = std::dynamic_pointer_cast<node_prog::reach_cache_value>(cvb);
    assert(rcv2->reachable_node == dst_node);
    assert(c.get_prog_type(req_id) == ptype);
    c.delete_cache(req_id);
    assert(!c.cache_exists(ptype, src_node, req_id));
    // multiple put and get
    for (uint64_t rid = 0; rid < 10; rid++) {
        rcv.reset(new node_prog::reach_cache_value());
        rcv->reachable_node = dst_node;
        c.put_cache(rid, ptype, src_node, rcv);
        c.commit(rid);
    }
    ignore.emplace(0);
    vec_cvb = c.get_cache(ptype, src_node, 10, dirty_list, ignore);
    assert(vec_cvb.size() == 9);
    vec_cvb = c.get_cache(ptype, src_node, 5, dirty_list, ignore);
    assert(vec_cvb.size() == 4);
    ignore.clear();
    vec_cvb = c.get_cache(ptype, src_node, 10, dirty_list, ignore);
    assert(vec_cvb.size() == 10);
    for (auto &cvb: vec_cvb) {
        rcv2 = std::dynamic_pointer_cast<node_prog::reach_cache_value>(cvb);
        assert(rcv2->reachable_node == dst_node);
    }
    delete dirty_list;
}
