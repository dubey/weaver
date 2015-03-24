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
using node_prog::edge_list;

edge_map_iter&
edge_map_iter :: operator++()
{
    while (++internal_cur != internal_end) {
        db::edge *e = *internal_cur;
        if (time_oracle->clock_creat_before_del_after(*req_time, e->base.get_creat_time(), e->base.get_del_time())) {
            cur_edge = e;
            break;
        }
    }
    return *this;
}

edge_map_iter :: edge_map_iter(edge_map_t::iterator begin,
    edge_map_t::iterator end,
    std::shared_ptr<vc::vclock> &req_time,
    order::oracle *to)
    : cur_edge(nullptr)
    , internal_cur(begin)
    , internal_end(end)
    , req_time(req_time)
    , time_oracle(to)
{
    if (internal_cur != internal_end) {
        db::edge *e = *internal_cur;
        if (time_oracle->clock_creat_before_del_after(*req_time, e->base.get_creat_time(), e->base.get_del_time())) {
            cur_edge = e;
        } else {
            ++(*this);
        }
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
    db::edge &toRet = *cur_edge;
    toRet.base.view_time = req_time;
    toRet.base.time_oracle = time_oracle;
    return toRet;
}

edge_list :: edge_list(edge_map_t &edge_list,
    std::shared_ptr<vc::vclock> &req_time,
    order::oracle *to)
    : wrapped(edge_list)
    , req_time(req_time)
    , time_oracle(to)
{ }

edge_map_iter
edge_list :: begin()
{
    return edge_map_iter(wrapped.begin(), wrapped.end(), req_time, time_oracle);
}

edge_map_iter
edge_list :: end()
{
    return edge_map_iter(wrapped.end(), wrapped.end(), req_time, time_oracle);
}

uint64_t
edge_list :: count()
{
    return wrapped.size();
}
