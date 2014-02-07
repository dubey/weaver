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

#include "common/event_order.h"
#include "node_prog/edge.h"
#include "node_prog/prop_list.h"
#include "node_prog/property.h"
#include "property.h"
#include "remote_node.h"
#include "element.h"

namespace db
{
namespace element
{
    class edge : public element, public node_prog::edge
    {
        public:
            edge();
            edge(uint64_t id, vc::vclock &vclk, uint64_t remote_loc, uint64_t remote_id);
            edge(uint64_t id, vc::vclock &vclk, remote_node &rn);
        
        public:
            remote_node nbr; // out-neighbor for this edge
            uint32_t msg_count; // number of messages sent on this link
            bool migr_edge; // true if this edge was migrated along with parent node
            void traverse(); // indicate that this edge was traversed; useful for migration statistics

            node_prog::node_handle& get_neighbor();
            node_prog::prop_list get_properties();
            bool has_property(node_prog::property& p);
            bool has_all_properties(std::vector<node_prog::property>& props);
    };

    // empty constructor for unpacking
    inline
    edge :: edge()
        : element()
        , msg_count(0)
        , migr_edge(false)
    { }

    inline
    edge :: edge(uint64_t id, vc::vclock &vclk, uint64_t remote_loc, uint64_t remote_id)
        : element(id, vclk)
        , nbr(remote_loc, remote_id)
        , msg_count(0)
        , migr_edge(false)
    { }

    inline
    edge :: edge(uint64_t id, vc::vclock &vclk, remote_node &rn)
        : element(id, vclk)
        , nbr(rn)
        , msg_count(0)
        , migr_edge(false)
    { }

    // caution: should be called with node mutex held
    // should always be called when an edge is traversed in a node program
    inline void
    edge :: traverse()
    {
        msg_count++;
    }

    node_prog::node_handle&
    edge :: get_neighbor()
    {
        return (node_prog::node_handle &) nbr; // cast
    }

    node_prog::prop_list 
    edge :: get_properties()
    {
        assert(view_time != NULL);
        return node_prog::prop_list(properties, *view_time);
    };

    bool
    edge :: has_property(node_prog::property& p)
    {
        assert(view_time != NULL);
        return has_property((db::element::property &) p, *view_time);// cast
    }

    bool
    edge :: has_all_properties(std::vector<node_prog::property>& props)
    {
        assert(view_time != NULL);
        for (auto &p : props) {
            if (!has_property((db::element::property &) p, *view_time)) { // cast
                return false;
            }
        }
        return true;
    }
}
}

#endif
