/*
 * ===============================================================
 *    Description:  An entry in the node map (~page table entry)
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_node_entry_h_
#define weaver_db_node_entry_h_

#include <vector>

#include "db/node.h"

namespace db
{
    struct node_entry
    {
        bool present;
        std::vector<node*> nodes;

        node_entry() : present(false) { }

        node_entry(node *n)
            : present(true)
            , nodes(1, n)
        { }
    };
}

#endif
