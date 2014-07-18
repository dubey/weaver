/*
 * ===============================================================
 *    Description:  db::edge implementation.
 *
 *        Created:  2014-05-30 17:28:31
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <memory>
#include "db/edge.h"

using db::element::edge;
using db::element::remote_node;

// empty constructor for unpacking
edge :: edge()
    : base()
#ifdef WEAVER_CLDG
    , msg_count(0)
#endif
#ifdef WEAVER_NEW_CLDG
    , msg_count(0)
#endif
    , migr_edge(false)
{ }

edge :: edge(const edge_handle_t &handle, vc::vclock &vclk, uint64_t remote_loc, node_id_t &remote_id)
    : base(handle, vclk)
    , nbr(remote_loc, remote_id)
#ifdef WEAVER_CLDG
    , msg_count(0)
#endif
#ifdef WEAVER_NEW_CLDG
    , msg_count(0)
#endif
    , migr_edge(false)
{ }

edge :: edge(const edge_handle_t &handle, vc::vclock &vclk, remote_node &rn)
    : base(handle, vclk)
    , nbr(rn)
#ifdef WEAVER_CLDG
    , msg_count(0)
#endif
#ifdef WEAVER_NEW_CLDG
    , msg_count(0)
#endif
    , migr_edge(false)
{ }

// caution: should be called with node mutex held
// should always be called when an edge is traversed in a node program
void
edge :: traverse()
{
#ifdef WEAVER_CLDG
    msg_count++;
#endif
#ifdef WEAVER_NEW_CLDG
    msg_count++;
#endif
}

remote_node& // TODO, make const, nbr private var
edge :: get_neighbor()
{
    return nbr; 
}

node_prog::prop_list 
edge :: get_properties()
{
    assert(base.view_time != NULL);
    return node_prog::prop_list(base.properties, *base.view_time);
}

bool
edge :: has_property(std::pair<std::string, std::string> &p)
{
    assert(base.view_time != NULL);
    return base.has_property(p, *base.view_time);
}

bool
edge :: has_all_properties(std::vector<std::pair<std::string, std::string>> &props)
{
    assert(base.view_time != NULL);
    return base.has_all_properties(props, *base.view_time);
}
