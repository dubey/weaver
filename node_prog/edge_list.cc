/*
 * ===============================================================
 *    Description:  Node prog edge list implementation.
 *
 *        Created:  2014-05-29 18:48:08
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "node_prog/edge_list.h"

using node_prog::edge_map_iter;

edge_map_iter&
edge_map_iter :: operator++()
{
    while (internal_cur != internal_end) {
        internal_cur++;
        if (internal_cur != internal_end
         && order::clock_creat_before_del_after(*req_time, internal_cur->second->base.get_creat_time(), internal_cur->second->base.get_del_time())) {
            break;
        }
    }
    return *this;
}

edge_map_iter :: edge_map_iter(edge_map_t::iterator begin, edge_map_t::iterator end,
    std::shared_ptr<vc::vclock> &req_time)
    : internal_cur(begin), internal_end(end), req_time(req_time)
{
    if (internal_cur != internal_end
     && !order::clock_creat_before_del_after(*req_time, internal_cur->second->base.get_creat_time(), internal_cur->second->base.get_del_time())) {
        ++(*this);
    }
}

bool
edge_map_iter :: operator==(const edge_map_iter& rhs)
{
    return internal_cur == rhs.internal_cur && *req_time == *rhs.req_time;
}

bool
edge_map_iter :: operator!=(const edge_map_iter& rhs)
{
    return internal_cur != rhs.internal_cur || !(*req_time == *rhs.req_time);
}

node_prog::edge&
edge_map_iter :: operator*()
{
    db::element::edge &toRet = *internal_cur->second;
    toRet.base.view_time = req_time;
    return (edge&)toRet;
}
