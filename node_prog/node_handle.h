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

namespace node_prog
{
    class node_handle
    {
        public:
            bool operator==(const node_handle &) const
            {
                return true;//(id == t.id) && (loc == t.loc);
            }

            bool operator!=(const node_handle &) const
            {
                return false;//(id != t.id) || (loc != t.loc);
            }
    };

    extern node_handle& coordinator;
}

#endif
