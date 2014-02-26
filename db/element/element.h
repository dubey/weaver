/*
 * ===============================================================
 *    Description:  Graph element base for edges and vertices
 *
 *        Created:  Thursday 11 October 2012 11:15:20  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __ELEMENT__
#define __ELEMENT__

#include <limits.h>
#include <stdint.h>
#include <vector>
#include <algorithm>
#include <string.h>

#include "common/weaver_constants.h"
#include "property.h"
#include "common/event_order.h"

inline bool
check_remove_prop(std::string &key, vc::vclock &vclk, db::element::property &prop)
{
    if (prop.key == key) {
        if (order::compare_two_vts(vclk, prop.get_del_time()) >= 1) {
            return true;
        }
    }
    return false;
}

namespace db
{
namespace element
{
    class element
    {
        public:
            element();
            element(uint64_t id, vc::vclock &vclk);
            
        protected:
            uint64_t id;
            vc::vclock creat_time;
            vc::vclock del_time;

        public:
            std::vector<property> properties;
            std::shared_ptr<vc::vclock> view_time;

        public:
            void add_property(property prop);
            void delete_property(std::string &key, vc::vclock &tdel);
            void remove_property(std::string &key, vc::vclock &vclk);
            bool has_property(std::string &key, std::string &value, vc::vclock &vclk);
            bool has_property(property &prop, vc::vclock &vclk);
            bool has_property(std::pair<std::string, std::string> &p, vc::vclock &vclk);
            bool has_all_properties(std::vector<std::pair<std::string, std::string>> &props, vc::vclock &vclk);
            bool check_and_add_property(property prop);
            void set_properties(std::vector<property> &props);
            void update_del_time(vc::vclock &del_time);
            void update_creat_time(vc::vclock &creat_time);
            const vc::vclock& get_creat_time() const;
            const vc::vclock& get_del_time() const;
            std::pair<bool, std::string> get_property_value(std::string prop_key, vc::vclock &at_time);
            const std::vector<property>* get_props() const;
            void set_id(uint64_t id);
            uint64_t get_id() const;
    };

    inline element :: element() { }

    inline element :: element(uint64_t hndl, vc::vclock &vclk)
        : id(hndl)
        , creat_time(vclk)
        , del_time(MAX_UINT64, MAX_TIME)
        , view_time(NULL)
    { }

    inline void
    element :: add_property(property prop)
    {
        properties.push_back(prop);
    }

    inline void
    element :: delete_property(std::string &key, vc::vclock &tdel)
    {
        for (auto &iter: properties) {
            if (iter.key == key) {
                iter.update_del_time(tdel);
            }
        }
    }

    // caution: assuming mutex access to this element
    inline void
    element :: remove_property(std::string &key, vc::vclock &vclk)
    {
        auto iter = std::remove_if(properties.begin(), properties.end(),
                // lambda function to check if property can be removed
                [&key, &vclk](property &prop) {
                    return check_remove_prop(key, vclk, prop);
                });
        properties.erase(iter, properties.end());
    }

    inline bool
    element ::has_property(std::string &key, std::string &value, vc::vclock &vclk)
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

    inline bool
    element :: has_property(std::pair<std::string, std::string> &p, vc::vclock &vclk)
    {
        return has_property(p.first, p.second, vclk);
    }

    inline bool
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
    inline bool
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

    inline void
    element :: set_properties(std::vector<property> &props)
    {
        properties = props;
    }

    inline void
    element :: update_del_time(vc::vclock &tdel)
    {
        assert(del_time.vt_id == MAX_UINT64);
        del_time = tdel;
    }

    inline void
    element :: update_creat_time(vc::vclock &tcreat)
    {
        creat_time = tcreat;
    }

    inline const vc::vclock&
    element :: get_creat_time() const
    {
        return creat_time;
    }

    inline const vc::vclock&
    element :: get_del_time() const
    {
        return del_time;
    }

    inline const std::vector<property>*
    element :: get_props() const
    {
        return &properties;
    }

    // return a pair, first is whether prop exists, second is value
    inline std::pair<bool, std::string>
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

    inline void
    element :: set_id(uint64_t hndl)
    {
        id = hndl;
    }

    inline uint64_t
    element :: get_id() const
    {
        return id;
    }
}
}

#endif
