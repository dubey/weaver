/*
 * ===============================================================
 *    Description:  Graph element (edges and vertices)
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
#include "common/property.h"
#include "common/event_order.h"

namespace db
{
namespace element
{
    class element
    {
        public:
            element();
            element(uint64_t handle, vc::vclock &vclk);
            
        protected:
            uint64_t handle;
            std::vector<common::property> properties;
            vc::vclock creat_time;
            vc::vclock del_time;

        public:
            void add_property(common::property prop);
            void delete_property(uint32_t key, vc::vclock &tdel);
            void remove_property(uint32_t key, vc::vclock &vclk);
            bool has_property(common::property prop, vc::vclock &vclk);
            bool check_and_add_property(common::property prop);
            void set_properties(std::vector<common::property> &props);
            void update_del_time(vc::vclock &del_time);
            void update_creat_time(vc::vclock &creat_time);
            vc::vclock get_creat_time() const;
            vc::vclock get_del_time() const;
            std::pair<bool, uint64_t> get_property_value(uint32_t prop_key, vc::vclock &at_time);
            const std::vector<common::property>* get_props() const;
            uint64_t get_handle() const;
    };

    inline element :: element()
        : creat_time(MAX_UINT64)
        , del_time(MAX_UINT64)
    { }

    inline element :: element(uint64_t hndl, vc::vclock &vclk)
        : handle(hndl)
        , creat_time(vclk)
        , del_time(MAX_UINT64)
    { }

    inline void
    element :: add_property(common::property prop)
    {
        properties.push_back(prop);
    }

    inline void
    element :: delete_property(uint32_t key, vc::vclock &tdel)
    {
        for (auto &iter: properties) {
            if (iter.key == key) {
                iter.update_del_time(tdel);
            }
        }
    }

    class match_key
    {
        public:
            uint32_t key;
            vc::vclock vclk;

            inline
            match_key(uint32_t k, vc::vclock &vclock)
                : key(k)
                , vclk(vclock)
            { }

            bool operator()(common::property const &prop) const
            {
                if (prop.key == key) {
                    int64_t cmp = order::compare_two_vts(vclk, prop.get_del_time());
                    if (cmp >= 1) {
                        return true;
                    }
                }
                return false;
            }
    };

    // remove properties which match key
    inline void
    element :: remove_property(uint32_t key, vc::vclock &vclk)
    {
        auto iter = std::remove_if(properties.begin(), properties.end(), match_key(key, vclk));
        properties.erase(iter, properties.end());
    }

    inline bool
    element :: has_property(common::property prop, vc::vclock &vclk)
    {
        for (auto &p: properties) {
            if (prop == p) {
                vc::vclock vclk_creat = p.get_creat_time();
                vc::vclock vclk_del = p.get_del_time();
                int64_t cmp1 = order::compare_two_vts(vclk, vclk_creat);
                int64_t cmp2 = order::compare_two_vts(vclk, vclk_del);
                if (cmp1 >= 1 && cmp2 == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    // if property with same key-value does not exist, add it
    bool
    element :: check_and_add_property(common::property prop)
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
    element :: set_properties(std::vector<common::property> &props)
    {
        properties = props;
    }

    inline void
    element :: update_del_time(vc::vclock &tdel)
    {
        del_time = tdel;
    }

    inline void
    element :: update_creat_time(vc::vclock &tcreat)
    {
        creat_time = tcreat;
    }

    inline vc::vclock
    element :: get_creat_time() const
    {
        return creat_time;
    }

    inline vc::vclock
    element :: get_del_time() const
    {
        return del_time;
    }

    inline const std::vector<common::property>*
    element :: get_props() const
    {
        return &properties;
    }

    // return a pair, first is whether prop exists, second is value
    std::pair<bool, uint64_t>
    element :: get_property_value(uint32_t prop_key, vc::vclock &at_time)
    {
        for (common::property& prop : properties)
        {
            vc::vclock vclk_creat = prop.get_creat_time();
            vc::vclock vclk_del = prop.get_del_time();
            int64_t cmp1 = order::compare_two_vts(at_time, vclk_creat);
            int64_t cmp2 = order::compare_two_vts(at_time, vclk_del);
            if (prop_key == prop.key && cmp1 >= 1 && cmp2 == 0) {
                return std::make_pair(true, prop.value);
            } 
        }
        return std::make_pair(false, 0xDEADBEEF);
    }

    inline uint64_t
    element :: get_handle() const
    {
        return handle;
    }
}
}

#endif
