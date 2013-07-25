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

namespace db
{
namespace element
{
    class element
    {
        public:
            element();
            element(vclock::timestamp &ts);
            
        protected:
            std::vector<common::property> properties;
            vclock::timestamp creat_time;
            vclock::timestamp del_time;

        public:
            void add_property(common::property prop);
            void delete_property(uint32_t key, vclock::timestamp &tdel);
            void remove_property(uint32_t key, vclock::nvc &clocks);
            bool has_property(common::property prop, vclock::nvc &clocks);
            bool check_and_add_property(common::property prop);
            void set_properties(std::vector<common::property> &props);
            void update_del_time(uint64_t del_time);
            void update_creat_time(uint64_t creat_time);
            uint64_t get_creat_time() const;
            uint64_t get_del_time() const;
            std::pair<bool, uint64_t> get_property_value(uint32_t prop_key, uint64_t at_time);
            const std::vector<common::property>* get_props() const;
    };

    inline element :: element() { }

    inline element :: element(vclock::timestamp &ts) : creat_time(ts) { }

    inline void
    element :: add_property(common::property prop)
    {
        properties.push_back(prop);
    }

    inline void
    element :: delete_property(uint32_t key, vclock::timestamp &tdel)
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
            vclock::nvc clocks;

            inline
            match_key(uint32_t k, vclock::nvc &clks)
                : key(k)
                , clocks(clks)
            { }

            bool operator()(common::property const &prop) const
            {
                if (prop.key == key) {
                    if (del_time.clock > clocks.at(del_time.rh_id).at(del_time.shard_id)) {
                        return true;
                    }
                }
                return false;
            }
    };

    // remove properties which match key
    inline void
    element :: remove_property(uint32_t key, vclock::nvs &clocks)
    {
        auto iter = std::remove_if(properties.begin(), properties.end(), match_key(key, clocks));
        properties.erase(iter, properties.end());
    }

    inline bool
    element :: has_property(common::property prop, vclock::nvc &clocks)
    {
        for (auto &p: properties) {
            if (prop == p) {
                vclock::timestamp &tcreat = p.get_creat_time();
                vclock::timestamp &tdel = p.get_del_time();
                if (tcreat.clock <= clocks.at(tcreat.rh_id).at(tcreat.shard_id) &&
                    tdel.clock > clocks.at(tdel.rh_id).at(tdel.shard_id)) {
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
    element :: update_del_time(vclock::timestmap &tdel)
    {
        del_time = tdel;
    }

    inline void
    element :: update_creat_time(vclock::timestamp &tcreat)
    {
        creat_time = tcreat;
    }

    inline vclock::timestamp&
    element :: get_creat_time() const
    {
        return creat_time;
    }

    inline vclock::timestamp&
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
    element :: get_property_value(uint32_t prop_key, uint64_t at_time)
    {
        for (common::property& prop : properties)
        {
            if (prop_key == prop.key && at_time >= prop.get_creat_time() && at_time < prop.get_del_time()) { 
                return std::make_pair(true, prop.value);
            } 
        }
        return std::make_pair(false, 0xDEADBEEF);
    }
}
}

#endif
