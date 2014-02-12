/*
 * ===============================================================
 *    Description:  Weaver cluster configuration implementation.
 *
 *        Created:  2014-02-10 14:08:44
 *
 *         Author:  Robert Escriva, escriva@cs.cornell.edu
 *                  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

// Most of the following code has been 'borrowed' from
// Robert Escriva's HyperDex coordinator.
// see https://github.com/rescrv/HyperDex for the original code.

#define __STDC_LIMIT_MACROS

// STL
#include <algorithm>
#include <sstream>

// HyperDex
#include "common/configuration.h"
#include "common/serialization.h"

configuration :: configuration()
    : m_cluster(0)
    , m_version(0)
    , m_flags(0)
    , m_servers()
{
    refill_cache();
}

configuration :: configuration(const configuration& other)
    : m_cluster(other.m_cluster)
    , m_version(other.m_version)
    , m_flags(other.m_flags)
    , m_servers(other.m_servers)
{
    refill_cache();
}

configuration :: ~configuration() throw ()
{
}

uint64_t
configuration :: cluster() const
{
    return m_cluster;
}

uint64_t
configuration :: version() const
{
    return m_version;
}

void
configuration :: get_all_addresses(std::vector<std::pair<server_id, po6::net::location> >* addrs) const
{
    addrs->resize(m_servers.size());

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        (*addrs)[i].first = m_servers[i].id;
        (*addrs)[i].second = m_servers[i].bind_to;
    }
}

bool
configuration :: exists(const server_id& id) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == id)
        {
            return true;
        }
    }

    return false;
}

po6::net::location
configuration :: get_address(const server_id& id) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == id)
        {
            return m_servers[i].bind_to;
        }
    }

    return po6::net::location();
}

server::state_t
configuration :: get_state(const server_id& id) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == id)
        {
            return m_servers[i].state;
        }
    }

    return server::KILLED;
}

std::string
configuration :: dump() const
{
    std::ostringstream out;
    out << "cluster " << m_cluster << "\n";
    out << "version " << m_version << "\n";
    out << "flags " << std::hex << m_flags << std::dec << "\n";

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        out << "server "
            << m_servers[i].id.get() << " "
            << m_servers[i].bind_to << " "
            << server::to_string(m_servers[i].state) << "\n";
    }

    return out.str();
}

configuration&
configuration :: operator = (const configuration& rhs)
{
    if (this == &rhs)
    {
        return *this;
    }

    m_cluster = rhs.m_cluster;
    m_version = rhs.m_version;
    m_flags = rhs.m_flags;
    m_servers = rhs.m_servers;
    refill_cache();
    return *this;
}

void
configuration :: refill_cache()
{
    std::sort(m_servers.begin(), m_servers.end());
}

e::unpacker
operator >> (e::unpacker up, configuration& c)
{
    uint64_t num_servers;
    up = up >> c.m_cluster >> c.m_version >> c.m_flags
            >> num_servers;
    c.m_servers.clear();
    c.m_servers.reserve(num_servers);

    for (size_t i = 0; !up.error() && i < num_servers; ++i)
    {
        server s;
        up = up >> s;
        c.m_servers.push_back(s);
    }

    return up;
}
