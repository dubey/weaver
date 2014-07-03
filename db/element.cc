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

element :: element(uint64_t _id, const std::string &_handle, vc::vclock &vclk)
    : id(_id)
    , handle(_handle)
    , creat_time(vclk)
    , del_time(UINT64_MAX, UINT64_MAX)
    , view_time(NULL)
{ }

void
element :: add_property(property prop)
{
    properties.push_back(prop);
}

void
element :: delete_property(std::string &key, vc::vclock &tdel)
{
    for (auto &iter: properties) {
        if (iter.key == key) {
            iter.update_del_time(tdel);
        }
    }
}

bool
check_remove_prop(std::string &key, vc::vclock &vclk, property &prop)
{
    if (prop.key == key) {
        if (order::compare_two_vts(vclk, prop.get_del_time()) >= 1) {
            return true;
        }
    }
    return false;
}

// caution: assuming mutex access to this element
void
element :: remove_property(std::string &key, vc::vclock &vclk)
{
    auto iter = std::remove_if(properties.begin(), properties.end(),
            // lambda function to check if property can be removed
            [&key, &vclk](property &prop) {
                return check_remove_prop(key, vclk, prop);
            });
    properties.erase(iter, properties.end());
}

bool
element :: has_property(std::string &key, std::string &value, vc::vclock &vclk)
{
    for (auto &p: properties) {
        if (p.equals(key, value)) {
            const vc::vclock& vclk_creat = p.get_creat_time();
            const vc::vclock& vclk_del = p.get_del_time();
            int64_t cmp1 = order::compare_two_vts(vclk, vclk_creat);
            int64_t cmp2 = order::compare_two_vts(vclk, vclk_del);
            if (cmp1 >= 1 && cmp2 == 0) {
                return true;
            }
        }
    }
    return false;
}

bool
element :: has_property(std::pair<std::string, std::string> &p, vc::vclock &vclk)
{
    return has_property(p.first, p.second, vclk);
}

bool
element :: has_all_properties(std::vector<std::pair<std::string, std::string>> &props, vc::vclock &vclk)
{
    for (auto &p : props) {
        if (!has_property(p.first, p.second, vclk)) { 
            return false;
        }
    }
    return true;
}

// if property with same key-value does not exist, add it
bool
element :: check_and_add_property(property prop)
{
    for (auto &iter: properties) {
        if (prop == iter) {
            return true;
        }
    }
    properties.push_back(prop);
    return false;
}

void
element :: set_properties(std::vector<property> &props)
{
    properties = props;
}

const std::vector<property>*
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

// return a pair, first is whether prop exists, second is value
std::pair<bool, std::string>
element :: get_property_value(std::string prop_key, vc::vclock &at_time)
{
    for (property& prop : properties)
    {
        const vc::vclock& vclk_creat = prop.get_creat_time();
        const vc::vclock& vclk_del = prop.get_del_time();
        int64_t cmp1 = order::compare_two_vts(at_time, vclk_creat);
        int64_t cmp2 = order::compare_two_vts(at_time, vclk_del);
        if (prop_key == prop.key && cmp1 >= 1 && cmp2 == 0) {
            return std::make_pair(true, prop.value);
        } 
    }
    return std::make_pair(false, std::string(""));
}

void
element :: set_id(uint64_t _id)
{
    id = _id;
}

uint64_t
element :: get_id() const
{
    return id;
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
