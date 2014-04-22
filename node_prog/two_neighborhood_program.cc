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
                db::element::remote_node &,
                two_neighborhood_params &params,
                std::function<two_neighborhood_state&()>,
                std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                    std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
                cache_response<Cache_Value_Base>*)
        {
            /*
               for (property &prop : n.get_properties()) {
               if (params.keys.empty() || (std::find(params.keys.begin(), params.keys.end(), prop.get_key()) != params.keys.end())) {
               params.node_props.emplace_back(prop.get_key(), prop.get_value());
               }
               }

               toRet.emplace_back(std::make_pair(db::element::coordinator, std::move(params)));
               }
             */
    std::vector<std::pair<db::element::remote_node, two_neighborhood_params>> toRet;
    return std::make_pair(search_type::DEPTH_FIRST, toRet); 
}
}

#endif
