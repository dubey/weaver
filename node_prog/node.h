/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_node_h_
#define weaver_node_prog_node_h_

#include <stdint.h>
#include <iostream>
#include <iterator>
#include <vector>
#include <unordered_map>

#include "edge_list.h"

namespace node_prog
{
    class node
    {
        public:
            virtual ~node() { }
            virtual edge_list get_edges() = 0;
            virtual prop_list get_properties() = 0;
    };
}

#endif
