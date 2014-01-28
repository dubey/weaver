/*
 * ===============================================================
 *    Description:  Remote node pointer for edges.
 *
 *        Created:  03/01/2013 11:29:16 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __NODE_PTR__
#define __NODE_PTR__

#include "db/element/remote_node.h"

namespace common
{
    class node_ptr : private db::element::remote_node
    {
        using db::element::remote_node::get_handle();
    };
}

#endif
