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
        check_cache_context(std::vector<node_cache_context>& contexts, db::element::remote_node &center,
                std::vector<db::element::remote_node> &one_hops_to_check, std::vector<db::element::remote_node> &two_hops_to_check)
    {
        if (contexts.size() == 0) {
            return true;
        }

        for (node_cache_context& node_context : contexts)
        {
            if (node_context.node_deleted){  // node deletion
                WDEBUG  << "Cache entry invalid because of node deletion" << std::endl;
                return false;
            } else if (!node_context.edges_deleted.empty()) {
                WDEBUG  << "Cache entry invalid because of edge deletion" << std::endl;
                return false;
            }
            for(auto &new_edge : node_context.edges_added){
                if (node_context.node == center) {
                    one_hops_to_check.emplace_back(new_edge.nbr);
                } else  {
                    two_hops_to_check.emplace_back(new_edge.nbr);
                }
            }
        }
        WDEBUG  << "Cache entry with context size " << contexts.size() << " valid but need to read "
            << one_hops_to_check.size() << " 1 hop neghbors and " << two_hops_to_check.size() << " two hop neighbors to revalidate" << std::endl;
        return true;
    }

    inline void
        fill_minus_duplicates(std::vector<std::pair<uint64_t, std::string>> &from, std::vector<std::pair<uint64_t, std::string>> &to)
        {
            to.swap(from);
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
            std::vector<std::pair<db::element::remote_node, two_neighborhood_params>> next;
            two_neighborhood_state &state = state_getter();

            if (MAX_CACHE_ENTRIES && params._search_cache  && cache_response != NULL && cache_response->get_value()->prop_key.compare(params.prop_key) == 0) {
                WDEBUG  << "GOT CACHE" << std::endl;
                params._search_cache = false; // only search cache once
                assert(params.on_hop == 0 && params.outgoing);
                std::vector<db::element::remote_node> one_hops_to_check;
                std::vector<db::element::remote_node> two_hops_to_check;
                if (check_cache_context(cache_response->get_context(), rn, one_hops_to_check, two_hops_to_check)) { // if context is valid
                    params.cache_update = true;
                    params.on_hop = 1;
                    params.prev_node = rn;
                    for (auto to_check : one_hops_to_check) {
                            next.emplace_back(std::make_pair(to_check, params));
                    }
                    params.on_hop = 2;
                    for (auto to_check : two_hops_to_check) {
                            next.emplace_back(std::make_pair(to_check, params));
                    }
                    if (next.empty()) { // can reply now
                        params.responses = cache_response->get_value()->responses;
                        next.emplace_back(std::make_pair(db::element::coordinator, params));
                    } else { // waiting for new nodes to be checked
                        state.prev_node = db::element::coordinator;
                        state.one_hop_visited = true; // in case of self loops
                        state.responses = cache_response->get_value()->responses;
                        WDEBUG  << "GOT CACHE with size " << state.responses.size()<< std::endl;
                        state.responses_left = next.size();
                    }
                } else {
                    cache_response->invalidate();
                }
                return std::make_pair(search_type::DEPTH_FIRST, next); 
            }

            params._search_cache = false;
            if (params.outgoing) {
                assert(params.responses.empty());
                switch (params.on_hop){
                    case 0:
                        //WDEBUG<< "GOT OUTGOING at hop 0 " << rn.get_id() << std::endl;
                        state.prev_node = db::element::coordinator;
                        state.one_hop_visited = true; // in case of self loops
                        params.prev_node = rn;
                        params.on_hop = 1;
                        for (edge &e: n.get_edges()) {
                            next.emplace_back(std::make_pair(e.get_neighbor(), params));
                            //WDEBUG<< "at hop 0 sending to " << e.get_neighbor().get_id() << std::endl;
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
                            //WDEBUG<< "GOT OUTGOING at hop 1 not going out " << rn.get_id() << std::endl;
                            params.on_hop = 0;
                            params.outgoing = false;
                            next.emplace_back(std::make_pair(params.prev_node, params));
                        } else {
                            //WDEBUG<< "GOT OUTGOING at hop 1 " << rn.get_id() << std::endl;
                            assert(state.responses.size() == 0);
                            state.one_hop_visited = true;
                            state.prev_node = params.prev_node;
                            params.prev_node = rn;
                            params.on_hop = 2;

                            for (edge &e: n.get_edges()) {
                                next.emplace_back(std::make_pair(e.get_neighbor(), params));
                                //WDEBUG<< "at hop 1 sending to " << e.get_neighbor().get_id() << std::endl;
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
                            //WDEBUG<< "checkign for props at " << rn.get_id() << std::endl;
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
                //WDEBUG<< "GOT " << params.responses.size() << " responses with on_hop " << params.on_hop << std::endl;
                assert(params.on_hop == 0 || params.on_hop == 1);

                state.responses.insert(state.responses.end(), params.responses.begin(), params.responses.end()); 

                assert(state.responses_left != 0);
                state.responses_left--;
                //WDEBUG<< "GOT RETURNING at " << rn.get_id() << ", responses_left = " << state.responses_left << std::endl;
                if (state.responses_left == 0) {
                    params.responses.clear();
                    if (MAX_CACHE_ENTRIES && params.on_hop == 0 && params.cache_update) {
                        fill_minus_duplicates(state.responses, params.responses);
                    } else {
                        params.responses.swap(state.responses);
                    }
                    if (MAX_CACHE_ENTRIES && params.on_hop == 0) {
                        WDEBUG  << "addin to CACHE" << std::endl;
                        std::shared_ptr<node_prog::two_neighborhood_cache_value> toCache(new two_neighborhood_cache_value(params.prop_key, params.responses));
                        std::shared_ptr<std::vector<db::element::remote_node>> watch_set(new std::vector<db::element::remote_node>());
                        watch_set->emplace_back(rn);
                        for (edge &e: n.get_edges()) {
                            watch_set->emplace_back(e.get_neighbor());
                        }
                        //WDEBUG << "storing cache" << std::endl;
                        add_cache_func(toCache, watch_set, std::hash<std::string>()(params.prop_key));
                    } else  {
                        params.on_hop--;
                    }
                    next.emplace_back(std::make_pair(state.prev_node, params));
                    //WDEBUG<< "sending to " << state.prev_node.get_id() << std::endl;
                } else {
                    assert(next.size() == 0);
                }
            }
            return std::make_pair(search_type::BREADTH_FIRST, next); 
        }
}

#endif
