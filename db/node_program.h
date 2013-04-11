/*
 * ===============================================================
 *    Description:  something
 *
 *        Created:  Sunday 17 March 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill, dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __NODE_PROG__
#define __NODE_PROG__

#include <vector>
#include <unordered_map>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"
#include "element/remote_node.h"

namespace db
{
    enum prog_type
    {
        REACHABILITY = 0,
        DIJKSTRA,
        CLUSTERING
    };

    template <typename params_type, typename node_state_type, typename cache_value_type>
    class node_program
    {
        std::vector<std::pair<element::remote_node, params_type>> (f)(element::node&, params_type&, node_state_type&, cache_value_type&);
    };
} 

namespace std
{
    // used if we want a hash table with a remote node as the key
    template <>
    struct hash<db::prog_type>
    {
        public:
            size_t operator()(db::prog_type x) const throw() 
            {
                return hash<int>()(x);
            }
    };
}

#endif //__NODE_PROG__
