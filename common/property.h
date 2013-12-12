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

#include <string>

#include "common/weaver_constants.h"
#include "common/vclock.h"

namespace common 
{
    class property
    {
        public:
            property();
            property(std::string&, std::string&);
            property(std::string&, std::string&, vc::vclock&);
        
        public:
            std::string key;
            std::string value;
            vc::vclock creat_time;
            vc::vclock del_time;

        public:
            bool operator==(property const &p2) const;

        public:
            vc::vclock get_creat_time() const;
            vc::vclock get_del_time() const;
            void update_del_time(vc::vclock&);
    };

    inline
    property :: property()
        : key("")
        , value("")
        , creat_time(MAX_UINT64, MAX_UINT64)
        , del_time(MAX_UINT64, MAX_UINT64)
    { }

    inline
    property :: property(std::string &k, std::string &v)
        : key(k)
        , value(v)
    {
    }

    inline
    property :: property(std::string &k, std::string &v, vc::vclock &creat)
        : key(k)
        , value(v)
        , creat_time(creat)
        , del_time(MAX_UINT64, MAX_UINT64)
    { }

    inline bool
    property :: operator==(property const &p2) const
    {
        return ((key == p2.key) && (value == p2.value));
    }

    inline vc::vclock
    property :: get_creat_time() const
    {
        return creat_time;
    }

    inline vc::vclock
    property :: get_del_time() const
    {
        return del_time;
    }

    inline void
    property :: update_del_time(vc::vclock &tdel)
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
                std::hash<std::string> string_hasher;
                size_t hkey = string_hasher(p.key);
                size_t hvalue = string_hasher(p.value);
                return ((hkey + 0x9e3779b9 + (hvalue<<6) + (hvalue>>2)) ^ hvalue);
            }
    };
}

#endif
