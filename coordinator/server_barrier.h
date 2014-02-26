/*
 * ===============================================================
 *    Description:  Barrier for all servers to ensure that each
 *                  server has received and updated new
 *                  cluster configuration.
 *
 *        Created:  2014-02-08 14:41:18
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

#ifndef weaver_server_manager_server_barrier_h_
#define weaver_server_manager_server_barrier_h_

// STL
#include <list>
#include <utility>
#include <vector>

// e
#include <e/buffer.h>

// Weaver
#include "common/ids.h"
#include "common/serialization.h"

namespace coordinator
{

class server_barrier
{
    public:
        server_barrier();
        ~server_barrier() throw ();

    public:
        uint64_t min_version() const;
        void new_version(uint64_t version,
                         const std::vector<server_id>& servers);
        void pass(uint64_t version, const server_id& sid);

    private:
        void maybe_clear_prefix();

    private:
        friend size_t pack_size(const server_barrier& ri);
        friend e::buffer::packer operator << (e::buffer::packer pa, const server_barrier& ri);
        friend e::unpacker operator >> (e::unpacker up, server_barrier& ri);
        typedef std::pair<uint64_t, std::vector<server_id> > version_t;
        typedef std::list<version_t> version_list_t;
        version_list_t m_versions;
};

inline size_t
pack_size(const server_barrier& ri)
{
    typedef server_barrier::version_list_t version_list_t;
    size_t sz = sizeof(uint32_t);

    for (version_list_t::const_iterator it = ri.m_versions.begin();
            it != ri.m_versions.end(); ++it)
    {
        sz += sizeof(uint64_t) + pack_size(it->second);
    }

    return sz;
}

inline e::buffer::packer
operator << (e::buffer::packer pa, const server_barrier& ri)
{
    typedef server_barrier::version_list_t version_list_t;
    uint32_t x = ri.m_versions.size();
    pa = pa << x;

    for (version_list_t::const_iterator it = ri.m_versions.begin();
            it != ri.m_versions.end(); ++it)
    {
        pa = pa << it->first << it->second;
    }

    return pa;
}

inline e::unpacker
operator >> (e::unpacker up, server_barrier& ri)
{
    typedef server_barrier::version_t version_t;
    uint32_t x = 0;
    up = up >> x;

    for (size_t i = 0; i < x && !up.error(); ++i)
    {
        ri.m_versions.push_back(version_t());
        version_t& v(ri.m_versions.back());
        up = up >> v.first >> v.second;
    }

    return up;
}

}

#endif
