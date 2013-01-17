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
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "property.h"
#include "meta_element.h"

namespace db
{
namespace element
{
    class element
    {
        public:
            element(std::shared_ptr<po6::net::location> server, uint64_t time, void* mem_addr);
            
        protected:
            std::vector<property> properties;
            std::shared_ptr<po6::net::location> myloc;
            uint64_t creat_time;
            uint64_t del_time;
            void* elem_addr; // memory address of this element on shard server

        public:
            po6::threads::mutex update_mutex;
            void add_property(property prop);
            void remove_property(property prop);
            bool has_property(property prop);
            bool check_and_add_property(property prop);
            void update_del_time(uint64_t del_time);
            meta_element get_meta_element();
            uint64_t get_creat_time();
            uint64_t get_del_time();
            
    };

    inline
    element :: element(std::shared_ptr<po6::net::location> server, uint64_t time, void* mem_addr)
        : properties(0)
        , myloc(server)
        , creat_time(time)
        , del_time(MAX_TIME)
        , elem_addr(mem_addr)
    {
    }

    inline void
    element :: add_property(property prop)
    {
        update_mutex.lock();
        properties.push_back(prop);
        update_mutex.unlock();
    }

    inline void
    element :: remove_property(property prop)
    {
        update_mutex.lock();
        std::vector<property>::iterator iter = std::remove(properties.begin(), properties.end(), prop);
        properties.erase(iter, properties.end());
        update_mutex.unlock();
    }

    bool
    element :: has_property(property prop)
    {
        update_mutex.lock();
        std::vector<property>::iterator iter;
        int i = 0;
        for (iter = properties.begin(); iter<properties.end(); iter++)
        {
            if (prop == *iter) 
            {
                update_mutex.unlock();
                return true;
            }
        }
        update_mutex.unlock();
        return false;
    }

    bool
    element :: check_and_add_property(property prop)
    {
        update_mutex.lock();
        std::vector<property>::iterator iter;
        int i = 0;
        for (iter = properties.begin(); iter<properties.end(); iter++)
        {
            if (prop == *iter) 
            {
                update_mutex.unlock();
                return true;
            }
        }
        properties.push_back(prop);
        update_mutex.unlock();
        return false;
    }

    inline void
    element :: update_del_time(uint64_t _del_time)
    {
        update_mutex.lock();
        del_time = _del_time;
        update_mutex.unlock();
    }

    inline meta_element
    element :: get_meta_element()
    {
        update_mutex.lock();
        meta_element *ret = new meta_element(*myloc, creat_time, del_time, elem_addr);
        update_mutex.unlock();
        return *ret;
    }

    inline uint64_t
    element :: get_creat_time()
    {
        uint64_t ret;
        update_mutex.lock();
        ret = creat_time;
        update_mutex.unlock();
        return ret;
    }

    inline uint64_t
    element :: get_del_time()
    {
        uint64_t ret;
        update_mutex.lock();
        ret = del_time;
        update_mutex.unlock();
        return ret;
    }
}
}

#endif //__ELEMENT__
