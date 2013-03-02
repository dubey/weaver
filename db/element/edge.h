/*
 * ===============================================================
 *    Description:  Graph edge class 
 *
 *        Created:  Tuesday 16 October 2012 02:28:29  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 * 
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __EDGE__
#define __EDGE__

#include <stdint.h>
#include <vector>
#include <po6/net/location.h>

#include "remote_node.h"
#include "element.h"

namespace db
{
namespace element
{
    class edge : public element
    {
        public:
            edge(uint64_t time, int remote_loc, size_t remote_handle);
        
        public:
            remote_node nbr; // out-neighbor for this edge
    };

    inline
    edge :: edge(uint64_t time, int remote_loc, size_t remote_handle)
        : element(time)
        , nbr(remote_loc, remote_handle)
    {
    }
}
}

#endif //__NODE__
