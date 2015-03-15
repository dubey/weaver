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

#include "common/weaver_constants.h"
#include "node_prog/property.h"
#include "node_prog/prop_list.h"

using node_prog::prop_iter;

bool
prop_iter :: check_props_iter(props_ds_t::iterator &iter)
{
#ifdef weaver_large_property_maps_
    for (const std::shared_ptr<db::property> p: iter->second) {
        if (time_oracle->clock_creat_before_del_after(req_time, p->get_creat_time(), p->get_del_time())) {
            return true;
        }
    }
#else
    const std::shared_ptr<db::property> p = *iter;
    if (time_oracle->clock_creat_before_del_after(req_time, p->get_creat_time(), p->get_del_time())) {
        return true;
    }
#endif

    return false;
}

prop_iter&
prop_iter :: operator++()
{
    while (++internal_cur != internal_end) {
        if (check_props_iter(internal_cur)) {
            break;
        }
    }

    return *this;
}

prop_iter :: prop_iter(props_ds_t::iterator begin,
    props_ds_t::iterator end,
    vc::vclock &req_time,
    order::oracle *to)
    : internal_cur(begin)
    , internal_end(end)
    , req_time(req_time)
    , time_oracle(to)
{
    if (internal_cur != internal_end) {
        if (!check_props_iter(internal_cur)) {
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

#ifdef weaver_large_property_maps_
    for (const std::shared_ptr<db::property> p: internal_cur->second) {
        if (time_oracle->clock_creat_before_del_after(req_time, p->get_creat_time(), p->get_del_time())) {
            ret.emplace_back(p);
        }
    }
#else
    ret.emplace_back(*internal_cur);
#endif

    return ret;
}
