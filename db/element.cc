/*
 * ===============================================================
 *    Description:  db::element implementation.
 *
 *        Created:  2014-05-30 16:58:32
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/event_order.h"
#include "db/element.h"

using db::element::element;
using db::element::property;

element :: element(const std::string &_handle, vc::vclock &vclk)
    : handle(_handle)
    , creat_time(vclk)
    , del_time(UINT64_MAX, UINT64_MAX)
    , time_oracle(nullptr)
{ }

void
element :: add_property(const property &prop)
{
    properties.emplace(prop.key, prop);
}

void
element :: add_property(const std::string &key, const std::string &value, const vc::vclock &vclk)
{
    properties.emplace(key, property(key, value, vclk));
}

void
element :: delete_property(std::string &key, vc::vclock &tdel)
{
    auto p = properties.find(key);
    assert(p != properties.end());
    p->second.update_del_time(tdel);
}

// caution: assuming mutex access to this element
void
element :: remove_property(std::string &key)
{
    properties.erase(key);
}

bool
element :: has_property(const std::string &key, const std::string &value)
{
    auto p = properties.find(key);
    if (p != properties.end()
     && p->second.value == value) {
        const property &prop = p->second;
        const vc::vclock& vclk_creat = prop.get_creat_time();
        const vc::vclock& vclk_del = prop.get_del_time();
        int64_t cmp1 = time_oracle->compare_two_vts(*view_time, vclk_creat);
        int64_t cmp2 = time_oracle->compare_two_vts(*view_time, vclk_del);
        if (cmp1 >= 1 && cmp2 == 0) {
            return true;
        }
    }
    return false;
}

bool
element :: has_property(const std::pair<std::string, std::string> &p)
{
    return has_property(p.first, p.second);
}

bool
element :: has_all_properties(const std::vector<std::pair<std::string, std::string>> &props)
{
    for (const auto &p : props) {
        if (!has_property(p)) { 
            return false;
        }
    }
    return true;
}

void
element :: set_properties(std::unordered_map<std::string, property> &props)
{
    properties = props;
}

const std::unordered_map<std::string, property>*
element :: get_props() const
{
    return &properties;
}

void
element :: update_del_time(vc::vclock &tdel)
{
    assert(del_time.vt_id == UINT64_MAX);
    del_time = tdel;
}

const vc::vclock&
element :: get_del_time() const
{
    return del_time;
}

void
element :: update_creat_time(vc::vclock &tcreat)
{
    creat_time = tcreat;
}

const vc::vclock&
element :: get_creat_time() const
{
    return creat_time;
}

void
element :: set_handle(const std::string &_handle)
{
    handle = _handle;
}

std::string
element :: get_handle() const
{
    return handle;
}
