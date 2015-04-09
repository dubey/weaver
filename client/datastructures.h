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

    struct hash_edge
    {
        size_t operator()(const edge &e) const
        {
            return std::hash<std::string>()(e.handle);
        }
    };

    struct equals_edge
    {
        bool operator()(const edge &e1, const edge &e2) const
        {
            return e1.handle == e2.handle;
        }
    };

    struct node
    {
        std::string handle;
        std::vector<std::shared_ptr<property>> properties;
        std::unordered_map<std::string, edge> out_edges;
        std::unordered_set<std::string> aliases;
    };

    struct hash_node
    {
        size_t operator()(const node &n) const
        {
            return std::hash<std::string>()(n.handle);
        }
    };

    struct equals_node
    {
        bool operator()(const node &n1, const node &n2) const
        {
            return n1.handle == n2.handle;
        }
    };
}

#endif
