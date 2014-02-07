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

#ifndef __NODE_HANDLE__
#define __NODE_HANDLE__

#include "db/element/remote_node.h"

namespace node_prog
{
    class node_handle
    {
    private:
      const db::element::remote_node& wrapped;
      node_handle(const db::element::remote_node& to_wrap) : wrapped(to_wrap) {}
    public:
      friend class node_handle_api;
    };

    static db::element::remote_node coord_remote_node(0,0);
    static node_handle& coordinator = (node_handle &) coord_remote_node;


  };
}

namespace std
{
    // used if we want a hash table with a node_handle as the key
    template <>
    struct hash<node_prog::node_handle> 
    {
        public:
            size_t operator()(const node_prog::node_handle &x) const throw() 
            {
                db::element::remote_node& toHash = dynamic_cast<db::element::remote_node&>(x);
                return std::hash<db::element::remote_node>()(toHash);
            }
    };
}

#endif
