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
using db::element::property_key_hasher;

property :: property()
    : creat_time(UINT64_MAX, UINT64_MAX)
    , del_time(UINT64_MAX, UINT64_MAX)
{ }

property :: property(const std::string &k, const std::string &v)
    : node_prog::property(k, v)
{ }

property :: property(const std::string &k, const std::string &v, const vc::vclock &creat)
    : node_prog::property(k, v)
    , creat_time(creat)
    , del_time(UINT64_MAX, UINT64_MAX)
{ }

bool
property :: operator==(property const &other) const
{
    return (key == other.key) && (value == other.value);
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

size_t
property_key_hasher :: operator()(const property &p) const
{
    return std::hash<std::string>()(p.key);
}
