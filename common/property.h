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
#include <stdint.h>

#include "common/weaver_constants.h"
#include "common/vclock.h"

namespace common 
{
    class property
    {
        public:
            property();
            property(uint32_t, uint64_t, vc::vclock_t&);
        
        public:
            uint32_t key;
            uint64_t value;
            vc::vclock_t creat_time;
            vc::vclock_t del_time;

        public:
            bool operator==(property const &p2) const;

        public:
            vc::vclock_t get_creat_time() const;
            vc::vclock_t get_del_time() const;
            void update_del_time(vc::vclock_t&);
    };

    inline
    property :: property()
        : key(0)
        , value(0)
    { }

    inline
    property :: property(uint32_t k, uint64_t v, vc::vclock_t &creat)
        : key(k)
        , value(v)
        , creat_time(creat)
    { }

    inline bool
    property :: operator==(property const &p2) const
    {
        return ((key == p2.key) && (value == p2.value));
    }

    inline vc::vclock_t
    property :: get_creat_time() const
    {
        return creat_time;
    }

    inline vc::vclock_t
    property :: get_del_time() const
    {
        return del_time;
    }

    inline void
    property :: update_del_time(vc::vclock_t &tdel)
    {
        del_time = tdel;
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

#endif
