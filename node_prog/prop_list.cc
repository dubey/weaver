/*
 * ===============================================================
 *    Description:  Node prog prop list implementation.
 *
 *        Created:  2014-05-29 19:08:19
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/event_order.h"
#include "node_prog/property.h"
#include "node_prog/prop_list.h"

using node_prog::prop_iter;

prop_iter&
prop_iter :: operator++()
{
    while (internal_cur != internal_end) {
        internal_cur++;
        if (internal_cur != internal_end
         && order::clock_creat_before_del_after(req_time, internal_cur->second.get_creat_time(), internal_cur->second.get_del_time())) {
            break;
        }
    }
    return *this;
}

prop_iter :: prop_iter(prop_map_t::iterator begin, prop_map_t::iterator end,
    vc::vclock &req_time)
    : internal_cur(begin)
    , internal_end(end)
    , req_time(req_time)
{
    if (internal_cur != internal_end
     && !order::clock_creat_before_del_after(req_time, internal_cur->second.get_creat_time(), internal_cur->second.get_del_time())) {
        ++(*this);
    }
}

bool
prop_iter :: operator!=(const prop_iter& rhs) const
{
    return internal_cur != rhs.internal_cur || !(req_time == rhs.req_time);
}

node_prog::property&
prop_iter :: operator*()
{
    return (property &)internal_cur->second;
}
