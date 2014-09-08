/*
 * ===============================================================
 *    Description:  Weaver cluster configuration.
 *
 *        Created:  2014-02-10 14:04:37
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

#ifndef weaver_common_configuration_h_
#define weaver_common_configuration_h_

// STL
#include <vector>

// po6
#include <po6/net/location.h>

// e
#include <e/array_ptr.h>
#include <e/slice.h>

// Weaver
#include "common/ids.h"
#include "common/server.h"

class configuration
{
    public:
        configuration();
        configuration(const configuration& other);
        ~configuration() throw ();

    // configuration metadata
    public:
        uint64_t cluster() const;
        uint64_t version() const;

    // membership metadata
    public:
        void get_all_addresses(std::vector<std::pair<server_id, po6::net::location> >* addrs) const;
        bool exists(const server_id& id) const;
        po6::net::location get_address(const server_id& id) const;
        server::state_t get_state(const server_id& id) const;
        uint64_t get_weaver_id(const server_id &id) const;
        uint64_t get_virtual_id(const server_id &id) const;
        server::type_t get_type(const server_id &id) const;
        std::vector<server> get_servers() const { return m_servers; }

    public:
        std::string dump() const;

    public:
        configuration& operator = (const configuration& rhs);
        std::vector<server> delta(const configuration &other);

    private:
        void refill_cache();
        friend size_t pack_size(const configuration&);
        friend e::buffer::packer operator << (e::buffer::packer, const configuration& s);
        friend e::unpacker operator >> (e::unpacker, configuration& s);

    private:
        uint64_t m_cluster;
        uint64_t m_version;
        uint64_t m_flags;
        std::vector<server> m_servers;
};

e::buffer::packer
operator << (e::buffer::packer, const configuration& c);
e::unpacker
operator >> (e::unpacker, configuration& c);
size_t
pack_size(const configuration&);

#endif // weaver_common_configuration_h_
