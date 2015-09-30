/*
 * ===============================================================
 *    Description:  Implementation of server barrier methods.
 *
 *        Created:  2014-02-08 14:41:28
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

// Weaver
#include "coordinator/server_barrier.h"
#include "common/passert.h"

using coordinator::server_barrier;

server_barrier :: server_barrier()
    : m_versions()
{
    m_versions.push_back(std::make_pair(uint64_t(0), std::vector<server_id>()));
}

server_barrier :: ~server_barrier() throw ()
{
}

uint64_t
server_barrier :: min_version() const
{
    return m_versions.front().first;
}

void
server_barrier :: new_version(uint64_t version,
                              const std::vector<server_id>& servers)
{
    PASSERT(!m_versions.empty());
    PASSERT(version > 0);
    PASSERT(m_versions.back().first < version);
    m_versions.push_back(std::make_pair(version, servers));
    maybe_clear_prefix();
}

void
server_barrier :: pass(uint64_t version, const server_id& sid)
{
    for (version_list_t::iterator it = m_versions.begin();
            it != m_versions.end(); ++it)
    {
        if (it->first < version)
        {
            continue;
        }
        else if (it->first > version)
        {
            break;
        }

        PASSERT(it->first == version);

        for (size_t i = 0; i < it->second.size(); )
        {
            if (it->second[i] == sid)
            {
                it->second[i] = it->second.back();
                it->second.pop_back();
            }
            else
            {
                ++i;
            }
        }

        maybe_clear_prefix();
        return;
    }
}

void
server_barrier :: maybe_clear_prefix()
{
    version_list_t::iterator last_empty = m_versions.end();

    for (version_list_t::iterator it = m_versions.begin();
            it != m_versions.end(); ++it)
    {
        if (it->second.empty())
        {
            last_empty = it;
        }
    }

    if (last_empty != m_versions.end())
    {
        while (m_versions.begin() != last_empty)
        {
            m_versions.pop_front();
        }
    }
}
