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

#define weaver_debug_
#include "two_neighborhood_program.h"

namespace node_prog
{
    inline bool
    check_cache_context(cache_response<two_neighborhood_cache_value> &cr)
    {
        std::vector<node_cache_context>& contexts = cr.get_context();
        if (contexts.size() == 0) {
            return true;
        }
        reach_cache_value &cv = *cr.get_value();
        // path not valid if broken by:
        for (node_cache_context& node_context : contexts)
        {
            if (node_context.node_deleted){  // node deletion
                WDEBUG  << "Cache entry invalid because of node deletion" << std::endl;
                return false;
            }
            // edge deletion, see if path was broken
            for (size_t i = 1; i < cv.path.size(); i++) {
                if (node_context.node == cv.path.at(i)) {
                    db::element::remote_node &path_next_node = cv.path.at(i-1);
                    for(auto &edge : node_context.edges_deleted){
                        if (edge.nbr == path_next_node) {
                            WDEBUG  << "Cache entry invalid because of edge deletion" << std::endl;
                            return false;
                        }
                    }
                    break; // path not broken here, move on
                }
            }
        }
        WDEBUG  << "Cache entry with context size " << contexts.size() << " valid" << std::endl;
        return true;
    }
    std::pair<search_type, std::vector<std::pair<db::element::remote_node, two_neighborhood_params>>>
        two_neighborhood_node_program(
                node &n,
                db::element::remote_node &rn,
                two_neighborhood_params &params,
                std::function<two_neighborhood_state&()> state_getter,
                std::function<void(std::shared_ptr<node_prog::two_neighborhood_cache_value>,
                    std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)> &add_cache_func,
                cache_response<two_neighborhood_cache_value> *cache_response)
        {
            if (MAX_CACHE_ENTRIES && params._search_cache  && cache_response != NULL && cache_response->get_value().props_key.compare(params.props_key) == 0) {
                assert(params.on_hop == 0 && params.outgoing);
                if (check_cache_context(*cache_response)) { // if context is valid

                } else {
                    cache_response->invalidate();
                }
            }

            params._search_cache = false;
            two_neighborhood_state &state = state_getter();
            std::vector<std::pair<db::element::remote_node, two_neighborhood_params>> next;
            if (params.outgoing) {
                assert(params.responses.empty());
                switch (params.on_hop){
                    case 0:
                        WDEBUG<< "GOT OUTGOING at hop 0 " << rn.get_id() << std::endl;
                        state.prev_node = db::element::coordinator;
                        state.one_hop_visited = true; // in case of self loops
                        params.prev_node = rn;
                        params.on_hop = 1;
                        for (edge &e: n.get_edges()) {
                            next.emplace_back(std::make_pair(e.get_neighbor(), params));
                            WDEBUG<< "at hop 0 sending to " << e.get_neighbor().get_id() << std::endl;
                        }
                        state.responses_left = next.size();
                        if (next.empty()) { // no neighbors
                            params.on_hop = 0;
                            params.outgoing = false;
                            next.emplace_back(std::make_pair(state.prev_node, params));
                        }
                        break;
                    case 1:
                        if (state.one_hop_visited) {
                            WDEBUG<< "GOT OUTGOING at hop 1 not going out " << rn.get_id() << std::endl;
                            params.on_hop = 0;
                            params.outgoing = false;
                            next.emplace_back(std::make_pair(params.prev_node, params));
                        } else {
                            WDEBUG<< "GOT OUTGOING at hop 1 " << rn.get_id() << std::endl;
                            assert(state.responses.size() == 0);
                            state.one_hop_visited = true;
                            state.prev_node = params.prev_node;
                            params.prev_node = rn;
                            params.on_hop = 2;

                            for (edge &e: n.get_edges()) {
                                next.emplace_back(std::make_pair(e.get_neighbor(), params));
                                WDEBUG<< "at hop 1 sending to " << e.get_neighbor().get_id() << std::endl;
                            }
                            state.responses_left = next.size();
                            if (next.empty()) { // no neighbors
                                params.on_hop = 0;
                                params.outgoing = false;
                                next.emplace_back(std::make_pair(state.prev_node, params));
                            }
                        }
                        break;
                    case 2:
                        if (!state.two_hop_visited) {
                            state.two_hop_visited = true;
                            WDEBUG<< "checkign for props at " << rn.get_id() << std::endl;
                            for (property &prop : n.get_properties()) {
                                if (prop.get_key().compare(params.prop_key) == 0) {
                                    params.responses.emplace_back(rn.get_id(), prop.get_value());
                                }
                            }
                        }
                        params.on_hop = 1;
                        params.outgoing = false;
                        next.emplace_back(std::make_pair(params.prev_node, params));
                        break;
                }
            } else { // returning
                assert(params.on_hop == 0 or params.on_hop == 1);
                WDEBUG<< "GOT " << params.responses.size() << " responses with on_hop " << params.on_hop << std::endl;
                assert(params.on_hop == 0 || params.on_hop == 1);

                state.responses.insert(state.responses.end(), params.responses.begin(), params.responses.end()); 

                assert(state.responses_left != 0);
                state.responses_left--;
                WDEBUG<< "GOT RETURNING at " << rn.get_id() << ", responses_left = " << state.responses_left << std::endl;
                if (state.responses_left == 0) {
                    params.responses.clear();
                    params.responses.swap(state.responses);
                    params.on_hop--;
                    next.emplace_back(std::make_pair(state.prev_node, params));
                    WDEBUG<< "sending to " << state.prev_node.get_id() << std::endl;
                } else {
                    assert(next.size() == 0);
                }
            }
            return std::make_pair(search_type::BREADTH_FIRST, next); 
        }
}

#endif
