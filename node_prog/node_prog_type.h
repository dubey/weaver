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

#ifndef __NODE_PROG_TYPE__
#define __NODE_PROG_TYPE__

#include <vector>
#include <map>
#include <unordered_map>

namespace node_prog
{
    enum prog_type
    {
        DEFAULT,
        REACHABILITY,
        N_HOP_REACHABILITY,
        TRIANGLE_COUNT,
        DIJKSTRA,
        CLUSTERING,
        READ_NODE_PROPS,
        READ_EDGES_PROPS
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
