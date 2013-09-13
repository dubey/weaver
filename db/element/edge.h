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
            edge(uint64_t handle, vc::vclock &vclk, uint64_t remote_loc, uint64_t remote_handle);
            edge(uint64_t handle, vc::vclock &vclk, remote_node &rn);
        
        public:
            remote_node nbr; // out-neighbor for this edge
            uint32_t msg_count; // number of messages sent on this link
            void traverse(); // indicate that this edge was traversed; useful for migration statistics
    };

    inline
    edge :: edge(uint64_t handle, vc::vclock &vclk, uint64_t remote_loc, uint64_t remote_handle)
        : element(handle, vclk)
        , nbr(remote_loc, remote_handle)
        , msg_count(0)
    { }

    inline
    edge :: edge(uint64_t handle, vc::vclock &vclk, remote_node &rn)
        : element(handle, vclk)
        , nbr(rn)
        , msg_count(0)
    { }

    // caution: should be called with node mutex held
    // should always be called when an edge is traversed in a node program
    inline void
    edge :: traverse()
    {
        msg_count++;
    }
}
}

#endif
