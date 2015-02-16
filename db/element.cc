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

element :: element(const std::string &_handle, const vc::vclock &vclk)
    : handle(_handle)
    , creat_time(vclk)
    , del_time(UINT64_MAX, UINT64_MAX)
    , time_oracle(nullptr)
{ }

bool
element :: add_property(const property &prop)
{
    auto find_iter = properties.find(prop.key);
    if (find_iter == properties.end()) {
        std::vector<std::shared_ptr<property>> new_vec;
        new_vec.emplace_back(std::make_shared<property>(prop));
        properties.emplace(prop.key, new_vec);
        return true;
    } else {
        bool exists = false;
        for (const std::shared_ptr<property> p: find_iter->second) {
            if (*p == prop && p->del_time.vt_id != UINT64_MAX) {
                exists = true;
                break;
            }
        }

        if (!exists) {
            find_iter->second.emplace_back(std::make_shared<property>(prop));
            return true;
        } else {
            return false;
        }
    }
}

bool
element :: add_property(const std::string &key, const std::string &value, const vc::vclock &vclk)
{
    property prop(key, value, vclk);
    return add_property(prop);
}

bool
element :: delete_property(const std::string &key, const vc::vclock &tdel)
{
    auto iter = properties.find(key);
    if (iter == properties.end()) {
        return false;
    } else {
        for (std::shared_ptr<property> p: iter->second) {
            p->update_del_time(tdel);
        }
        return true;
    }
}

bool
element :: delete_property(const std::string &key, const std::string &value, const vc::vclock &tdel)
{
    auto iter = properties.find(key);
    if (iter == properties.end()) {
        return false;
    } else {
        for (std::shared_ptr<property> p: iter->second) {
            if (p->key == key && p->value == value && p->del_time.vt_id != UINT64_MAX) {
                p->update_del_time(tdel);
                return true;
            }
        }
        return false;
    }
}

// caution: assuming mutex access to this element
void
element :: remove_property(const std::string &key)
{
    properties.erase(key);
}

bool
element :: has_property(const std::string &key, const std::string &value)
{
    auto iter = properties.find(key);
    if (iter != properties.end()) {
        for (const std::shared_ptr<property> p: iter->second) {
            if (p->value == value) {
                const vc::vclock &vclk_creat = p->get_creat_time();
                const vc::vclock &vclk_del = p->get_del_time();
                int64_t cmp1 = time_oracle->compare_two_vts(*view_time, vclk_creat);
                int64_t cmp2 = time_oracle->compare_two_vts(*view_time, vclk_del);
                if (cmp1 >= 1 && cmp2 == 0) {
                    return true;
                }
            }
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
element :: set_properties(std::unordered_map<std::string, std::vector<std::shared_ptr<property>>> &props)
{
    properties = props;
}

const std::unordered_map<std::string, std::vector<std::shared_ptr<property>>>*
element :: get_props() const
{
    return &properties;
}

void
element :: update_del_time(const vc::vclock &tdel)
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
element :: update_creat_time(const vc::vclock &tcreat)
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
