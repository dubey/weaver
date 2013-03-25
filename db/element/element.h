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
            element(uint64_t time);
            
        protected:
            std::vector<common::property> properties;
            uint64_t creat_time;
            uint64_t del_time;

        public:
            void add_property(common::property prop);
            void delete_property(uint32_t key, uint64_t del_time);
            void remove_property(common::property prop);
            bool has_property(common::property prop);
            bool check_and_add_property(common::property prop);
            void set_properties(std::vector<common::property> &props);
            void update_del_time(uint64_t del_time);
            void update_creat_time(uint64_t creat_time);
            uint64_t get_creat_time() const;
            uint64_t get_del_time() const;
            const std::vector<common::property>* get_props() const;
    };

    inline
    element :: element()
    {
    }

    inline
    element :: element(uint64_t time)
        : creat_time(time)
        , del_time(MAX_TIME)
    {
    }

    inline void
    element :: add_property(common::property prop)
    {
        properties.push_back(prop);
    }

    inline void
    element :: delete_property(uint32_t key, uint64_t del_time)
    {
        std::vector<common::property>::iterator iter;
        for (iter = properties.begin(); iter<properties.end(); iter++) {
            if (iter->key == key) {
                iter->update_del_time(del_time);
            }
        }
    }

    inline void
    element :: remove_property(common::property prop)
    {
        std::vector<common::property>::iterator iter = std::remove(properties.begin(), properties.end(), prop);
        properties.erase(iter, properties.end());
    }

    bool
    element :: has_property(common::property prop)
    {
        std::vector<common::property>::iterator iter;
        for (iter = properties.begin(); iter<properties.end(); iter++) {
            if (prop == *iter && prop.get_creat_time() >= iter->get_creat_time() && prop.get_creat_time() < iter->get_del_time()) {
                return true;
            } 
        }
        return false;
    }

    bool
    element :: check_and_add_property(common::property prop)
    {
        std::vector<common::property>::iterator iter;
        for (iter = properties.begin(); iter<properties.end(); iter++) {
            if (prop == *iter) {
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
    element :: update_del_time(uint64_t _del_time)
    {
        del_time = _del_time;
    }

    inline void
    element :: update_creat_time(uint64_t _creat_time)
    {
        creat_time = _creat_time;
    }

    inline uint64_t
    element :: get_creat_time() const
    {
        return creat_time;
    }

    inline uint64_t
    element :: get_del_time() const
    {
        return del_time;
    }

    inline const std::vector<common::property>*
    element :: get_props() const
    {
        return &properties;
    }
}
}

#endif //__ELEMENT__
