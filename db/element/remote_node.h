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
            remote_node();
            remote_node(int, size_t);

        public:
            int loc;
            size_t handle;
            bool operator==(const db::element::remote_node &t) const;
    };
    
    inline
    remote_node :: remote_node(int l, size_t h)
        : loc(l)
        , handle(h)
    {
    }

    inline
    remote_node :: remote_node()
    {
    }

    inline bool
    remote_node :: operator==(const db::element::remote_node &t) const{
        return (handle == t.handle) && (loc == t.loc);
    }
}
}

#endif
