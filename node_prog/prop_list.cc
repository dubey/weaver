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

#include "node_prog/property.h"
#include "node_prog/prop_list.h"

using node_prog::prop_iter;

prop_iter&
prop_iter :: operator++()
{
    while (++internal_cur != internal_end) {
        bool to_break = false;
        for (const std::shared_ptr<db::property> p: internal_cur->second) {
            if (time_oracle->clock_creat_before_del_after(req_time, p->get_creat_time(), p->get_del_time())) {
                to_break = true;
                break;
            }
        }

        if (to_break) {
            break;
        }
    }

    return *this;
}

prop_iter :: prop_iter(prop_map_t::iterator begin,
    prop_map_t::iterator end,
    vc::vclock &req_time,
    order::oracle *to)
    : internal_cur(begin)
    , internal_end(end)
    , req_time(req_time)
    , time_oracle(to)
{
    if (internal_cur != internal_end) {
        bool to_break = false;

        for (const std::shared_ptr<db::property> p: internal_cur->second) {
            if (time_oracle->clock_creat_before_del_after(req_time, p->get_creat_time(), p->get_del_time())) {
                to_break = true;
                break;
            }
        }

        if (!to_break) {
            ++(*this);
        }
    }
}

bool
prop_iter :: operator!=(const prop_iter& rhs) const
{
    return internal_cur != rhs.internal_cur || !(req_time == rhs.req_time);
}

std::vector<std::shared_ptr<node_prog::property>>
prop_iter :: operator*()
{
    std::vector<std::shared_ptr<property>> ret;
    ret.reserve(internal_cur->second.size());

    for (const std::shared_ptr<db::property> p: internal_cur->second) {
        if (time_oracle->clock_creat_before_del_after(req_time, p->get_creat_time(), p->get_del_time())) {
            ret.emplace_back(p);
        }
    }

    return ret;
}
