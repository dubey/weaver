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

#ifndef __REMOTE_NODE__
#define __REMOTE_NODE__

namespace db
{
namespace element
{
    class remote_node
    {
        public:
            remote_node(int, uint64_t);

        public:
            int loc;
            uint64_t handle;
    };
    
    inline
    remote_node :: remote_node(int l, uint64_t h)
        : loc(l)
        , handle(h)
    {
    }
}
}

#endif
