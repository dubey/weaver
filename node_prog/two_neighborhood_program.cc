/*
 * ===============================================================
 *    Description:  Node program to read properties of a single node
 *
 *        Created:  Friday 17 January 2014 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef weaver_node_prog_two_neighborhood_program_cc 
#define weaver_node_prog_two_neighborhood_program_cc 

#include "two_neighborhood_program.h"

namespace node_prog
{
    std::pair<search_type, std::vector<std::pair<db::element::remote_node, two_neighborhood_params>>>
        two_neighborhood_node_program(
                node &n,
                db::element::remote_node &rn,
                two_neighborhood_params &params,
                std::function<two_neighborhood_state&()> state_getter,
                std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                    std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
                cache_response<Cache_Value_Base>*)
        {
            two_neighborhood_state &state = state_getter();
            std::vector<std::pair<db::element::remote_node, two_neighborhood_params>> next;
            if (params.outgoing) {
                assert(params.responses.empty());
                assert(state.responses_left == 0);
                switch (params.hops){
                    case 0:
                        state.prev_node = db::element::coordinator;
                        params.prev_node = rn;
                        params.hops = 1;
                        for (edge &e: n.get_edges()) {
                            next.emplace_back(std::make_pair(e.get_neighbor(), params));
                        }
                        state.responses_left = next.size();
                        break;
                    case 1:
                        if (state.visited) {
                            params.hops = 0;
                            params.outgoing = false;
                            next.emplace_back(std::make_pair(params.prev_node, params));
                        } else {
                            state.visited = true;
                            assert(state.responses.size() == 0);
                            params.hops = 2;
                            state.prev_node = params.prev_node;
                            params.prev_node = rn;

                            for (edge &e: n.get_edges()) {
                                next.emplace_back(std::make_pair(e.get_neighbor(), params));
                            }
                            state.responses_left = next.size();
                        }
                        break;
                    case 2:
                        if (!state.visited) {
                            state.visited = true;
                            for (property &prop : n.get_properties()) {
                                if (prop.get_key().compare(params.prop_key) == 0) {
                                    params.responses.emplace_back(rn.get_id(), prop.get_value());
                                }
                            }
                        }
                        params.hops = 1;
                        params.outgoing = false;
                        next.emplace_back(std::make_pair(params.prev_node, params));
                        break;
                }
            } else { // returning
                assert(state.visited);
                assert(params.hops == 0 or params.hops == 1);
                assert(params.hops == 0 || (params.hops == 1 && params.responses.size() < 2));

                state.responses.insert(state.responses.end(), params.responses.begin(), params.responses.end()); 

                assert(state.responses_left != 0);
                state.responses_left --;
                if (state.responses_left == 0) {
                    params.responses.clear();
                    params.responses.swap(state.responses);
                    params.hops--;
                    next.emplace_back(std::make_pair(params.prev_node, params));
                } else {
                    assert(next.size() == 0);
                }
            }
            return std::make_pair(search_type::BREADTH_FIRST, next); 
        }
}

#endif
