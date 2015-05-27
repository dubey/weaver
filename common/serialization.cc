/*
 * ===============================================================
 *    Description:  Implementation of po6 classes's packers and
 *                  unpackers.
 *
 *        Created:  2014-02-08 17:26:46
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
#include "common/serialization.h"

e::packer
operator << (e::packer lhs, const po6::net::ipaddr& rhs)
{
    assert(rhs.family() == AF_INET || rhs.family() == AF_INET6 || rhs.family() == AF_UNSPEC);
    uint8_t type;
    uint8_t data[16];
    memset(data, 0, 16);

    if (rhs.family() == AF_INET)
    {
        type = 4;
        sockaddr_in sa;
        rhs.pack(&sa, 0);
        memmove(data, &sa.sin_addr.s_addr, 4);
    }
    else if (rhs.family() == AF_INET6)
    {
        type = 6;
        sockaddr_in6 sa;
        rhs.pack(&sa, 0);
#ifdef _MSC_VER
        memmove(data, &sa.sin6_addr.u.Byte, 16);
#elif defined __APPLE__
        memmove(data, &sa.sin6_addr.__u6_addr.__u6_addr8, 16);
#else
        memmove(data, &sa.sin6_addr.__in6_u.__u6_addr8, 16);
#endif

    }
    else
    {
        type = 0;
    }

    lhs = lhs << type;
    return lhs.copy(e::slice(data, 16));
}

e::unpacker
operator >> (e::unpacker lhs, po6::net::ipaddr& rhs)
{
    uint8_t type;
    lhs = lhs >> type;

    if (lhs.remain() < 16)
    {
        return lhs.as_error();
    }

    e::slice rem = lhs.as_slice();

    if (type == 4)
    {
        in_addr ia;
        memmove(&ia.s_addr, rem.data(), 4);
        rhs = po6::net::ipaddr(ia);
        return lhs.advance(16);
    }
    else if (type == 6)
    {
        in6_addr ia;
#ifdef _MSC_VER
        memmove(ia.u.Byte, rem.data(), 16);
#elif defined __APPLE__
        memmove(ia.__u6_addr.__u6_addr8, rem.data(), 16);
#else
        memmove(ia.__in6_u.__u6_addr8, rem.data(), 16);
#endif
        rhs = po6::net::ipaddr(ia);
        return lhs.advance(16);
    }
    else if (type == 0)
    {
        return lhs.advance(16);
    }
    else
    {
        return lhs.as_error();
    }
}

size_t
pack_size(const po6::net::ipaddr&)
{
    return 17; // One byte for family, and 4/16 for address
}

e::packer
operator << (e::packer lhs, const po6::net::location& rhs)
{
    return lhs << rhs.address << rhs.port;
}

e::unpacker
operator >> (e::unpacker lhs, po6::net::location& rhs)
{
    return lhs >> rhs.address >> rhs.port;
}

size_t
pack_size(const po6::net::location& rhs)
{
    return pack_size(rhs.address) + sizeof(uint16_t);
}

e::packer
operator << (e::packer lhs, const po6::net::hostname& rhs)
{
    return lhs << e::slice(rhs.address.data(), rhs.address.size()) << rhs.port;
}

e::unpacker
operator >> (e::unpacker lhs, po6::net::hostname& rhs)
{
    e::slice address;
    lhs = lhs >> address >> rhs.port;
    rhs.address = std::string(reinterpret_cast<const char*>(address.data()), address.size());
    return lhs;
}

size_t
pack_size(const po6::net::hostname& rhs)
{
    return sizeof(uint32_t) + rhs.address.size() + sizeof(uint16_t);
}
