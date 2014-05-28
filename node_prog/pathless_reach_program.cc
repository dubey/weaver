/*
 * ===============================================================
 *    Description:  Reachability program.
 *
 *        Created:  Sunday 23 April 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

//#define weaver_debug_
#include "pathless_reach_program.h"
            bool returning; // false = request, true = reply
            db::element::remote_node prev_node;
            uint64_t dest;
            std::vector<std::pair<std::string, std::string>> edge_props;
            bool reachable;


namespace node_prog
{
    std::pair<search_type, std::vector<std::pair<db::element::remote_node, pathless_reach_params>>>
    pathless_reach_node_program(
            node &n,
            db::element::remote_node &rn,
            pathless_reach_params &params,
            std::function<pathless_reach_node_state&()> state_getter,
            std::function<void(std::shared_ptr<Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            cache_response<Cache_Value_Base>*)
    {
        pathless_reach_node_state &state = state_getter();
        std::vector<std::pair<db::element::remote_node, pathless_reach_params>> next;
        if (state.reachable == true) {
            return std::make_pair(search_type::BREADTH_FIRST, next);
        }
        bool false_reply = false;
        db::element::remote_node prev_node = params.prev_node;
        params.prev_node = rn;
        if (!params.returning) { // request mode
            if (params.dest == rn.get_id()) {
                // we found the node we are looking for, prepare a reply
                params.reachable = true;
                //    WDEBUG  << "found dest!" << std::endl;
                next.emplace_back(std::make_pair(db::element::coordinator, params));
                return std::make_pair(search_type::DEPTH_FIRST, next);
            } else {
                // have not found it yet so follow all out edges
                if (!state.visited) {
                    state.prev_node = prev_node;
                    state.visited = true;

                    for (edge &e: n.get_edges()) {
                        // checking edge properties
                        if (e.has_all_properties(params.edge_props)) {
                            // e->traverse(); no more traversal recording

                            // propagate reachability request
                            next.emplace_back(std::make_pair(e.get_neighbor(), params));
                            //WDEBUG  << "emplacing " << e.get_neighbor().id << " to next" << std::endl;
                            state.out_count++;
                        }
                    }
                    if (state.out_count == 0) {
                        false_reply = true;
                    }
                } else {
                    false_reply = true;
                }
            }
            if (false_reply) {
                params.returning = true;
                params.reachable = false;
                next.emplace_back(std::make_pair(prev_node, params));
            }
            return std::make_pair(search_type::BREADTH_FIRST, next);
        } else { // reply mode
            if (((--state.out_count == 0) || params.reachable) && !state.reachable) {
                state.reachable |= params.reachable;
                next.emplace_back(std::make_pair(state.prev_node, params));
            }
            if ((int)state.out_count < 0) {
                WDEBUG << "ALERT! Bad state value in reach program" << std::endl;
                next.clear();
            }
            if (params.reachable) {
                return std::make_pair(search_type::DEPTH_FIRST, next);
            } else {
                return std::make_pair(search_type::BREADTH_FIRST, next);
            }
        }
    }
}
