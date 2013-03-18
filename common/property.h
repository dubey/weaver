/*
 * ===============================================================
 *    Description:  Each graph element (node or edge) can have
 *                  properties, which are key-value pairs 
 *
 *        Created:  Friday 12 October 2012 01:28:02  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __PROPERTY__
#define __PROPERTY__

#include <stdlib.h>
#include <string.h>

#include "common/weaver_constants.h"

namespace common 
{
    class property
    {
        public:
            property();
            property(uint32_t, size_t, uint64_t);
        
        public:
            char* __key;
            char* __value;
            uint32_t key;
            size_t value;
            uint64_t creat_time;
            uint64_t del_time;

        public:
            bool operator==(property p2) const;

        public:
            uint64_t get_creat_time();
            uint64_t get_del_time();
            void update_del_time(uint64_t);
    };

    inline
    property :: property()
        : key(0)
        , value(0)
        , creat_time(0)
        , del_time(0)
    {
    }

    inline
    property :: property(uint32_t _key, size_t _value, uint64_t t_creat)
        : key(_key)
        , value(_value)
        , creat_time(t_creat)
        , del_time(MAX_TIME)
    {
    }

    inline bool
    property :: operator==(property p2) const
    {
        return ((key == p2.key) && (value == p2.value));
    }

    inline uint64_t
    property :: get_creat_time()
    {
        return creat_time;
    }

    inline uint64_t
    property :: get_del_time()
    {
        return del_time;
    }

    inline void
    property :: update_del_time(uint64_t t_del)
    {
        del_time = t_del;
    }
}

namespace std
{
    template <>
    struct hash<common::property> 
    {
        public:
            size_t operator()(common::property p) const throw() 
            {
                std::hash<uint32_t> int_hasher;
                return ((int_hasher(p.key) + 0x9e3779b9 + (p.value<<6) + (p.value>>2)) ^ p.value);
            }
    };
}

#endif //__PROPERTY__
