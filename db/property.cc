/*
 * ===============================================================
 *    Description:  db::property implementation.
 *
 *        Created:  2014-05-30 17:24:08
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "db/property.h"

using db::element::property;

property :: property()
    : creat_time(UINT64_MAX, UINT64_MAX)
    , del_time(UINT64_MAX, UINT64_MAX)
{ }

property :: property(std::string &k, std::string &v)
    : node_prog::property(k, v)
{ }

property :: property(std::string &k, std::string &v, vc::vclock &creat)
    : node_prog::property(k, v)
    , creat_time(creat)
    , del_time(UINT64_MAX, UINT64_MAX)
{ }

bool
property :: equals(std::string const &key2, std::string const &value2) const
{
    if (key.length() != key2.length()
     || value.length() != value2.length()) {
        return false;
    }
    uint64_t smaller, larger;
    if (key.length() < value.length()) {
        smaller = key.length();
        larger = value.length();
    } else {
        smaller = value.length();
        larger = key.length();
    }
    uint64_t i;
    for (i = 0; i < smaller; i++) {
        if (key[i] != key2[i]) {
            return false;
        } else if (value[i] != value2[i]) {
            return false;
        }
    }
    if (larger == key.length()) {
        for (; i < larger; i++) {
            if (key[i] != key2[i]) {
                return false;
            }
        }
    } else {
        for (; i < larger; i++) {
            if (value[i] != value2[i]) {
                return false;
            }
        }
    }
    return true;
}

bool
property :: operator==(property const &p2) const
{
    return equals(p2.key, p2.value);
}

const vc::vclock&
property :: get_creat_time() const
{
    return creat_time;
}

const vc::vclock&
property :: get_del_time() const
{
    return del_time;
}

void
property :: update_del_time(vc::vclock &tdel)
{
    del_time = tdel;
}
