/*
 * ===============================================================
 *    Description:  Base classes, typedefs, and necessary enum
 *                  declarations for all node programs. 
 *
 *        Created:  Sunday 17 March 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill, dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef weaver_node_prog_node_prog_type_h_
#define weaver_node_prog_node_prog_type_h_

#include <vector>
#include <map>
#include <unordered_map>

namespace node_prog
{
    enum search_type
    {
        BREADTH_FIRST,
        DEPTH_FIRST
    };

    enum prog_type
    {
        REACHABILITY,
        PATHLESS_REACHABILITY,
        N_HOP_REACHABILITY,
        TRIANGLE_COUNT,
        DIJKSTRA,
        CLUSTERING,
        TWO_NEIGHBORHOOD,
        READ_NODE_PROPS,
        READ_EDGES_PROPS,
        READ_N_EDGES,
        EDGE_COUNT,
        EDGE_GET,
        NODE_GET,
        TRAVERSE_PROPS,
        DISCOVER_PATHS,
        GET_BTC_BLOCK,
        GET_BTC_TX,
        END
    };

}

namespace std
{
    // used if we want a hash table with a prog type as the key
    template <>
    struct hash<node_prog::prog_type>
    {
        public:
            size_t operator()(node_prog::prog_type x) const throw() 
            {
                return hash<int>()(x);
            }
    };
}

#endif
