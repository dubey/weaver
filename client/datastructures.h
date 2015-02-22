/*
 * ===============================================================
 *    Description:  Client datastructures.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_client_datastructures_h_
#define weaver_client_datastructures_h_

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "node_prog/property.h"

namespace cl
{
    using property = node_prog::property;

    struct edge
    {
        std::string handle;
        std::string start_node, end_node;
        std::vector<std::shared_ptr<property>> properties;
    };

    struct node
    {
        std::string handle;
        std::vector<std::shared_ptr<property>> properties;
        std::unordered_map<std::string, edge> out_edges;
        std::unordered_set<std::string> aliases;
    };

}

#endif
