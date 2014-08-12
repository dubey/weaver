/*
 * ===============================================================
 *    Description:  Implementation of common::server methods.
 *
 *        Created:  2014-02-08 17:30:01
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
#include "common/server.h"
#include "common/serialization.h"

const char*
server :: to_string(state_t state)
{
    switch (state)
    {
        case ASSIGNED:
            return "ASSIGNED";
        case NOT_AVAILABLE:
            return "NOT_AVAILABLE";
        case AVAILABLE:
            return "AVAILABLE";
        case SHUTDOWN:
            return "SHUTDOWN";
        case KILLED:
            return "KILLED";
        default:
            return "UNKNOWN";
    }
}

const char*
server :: to_string(type_t type)
{
    switch (type)
    {
        case UNDEF:
            return "UNDEF";
        case SHARD:
            return "SHARD";
        case VT:
            return "VT";
        default:
            return "BADTYPE";
    }
}

server :: server()
    : state(KILLED)
    , id()
    , weaver_id(UINT64_MAX)
    , type(UNDEF)
    , bind_to()
{
}

server :: server(const server_id& sid)
    : state(ASSIGNED)
    , id(sid)
    , weaver_id(UINT64_MAX)
    , type(UNDEF)
    , bind_to()
{
}

bool
operator < (const server& lhs, const server& rhs)
{
    return lhs.id < rhs.id;
}

e::buffer::packer
operator << (e::buffer::packer lhs, const server& rhs)
{
    uint8_t s = static_cast<uint8_t>(rhs.state);
    uint8_t t = static_cast<uint8_t>(rhs.type);
    return lhs << s << rhs.id << rhs.weaver_id << t << rhs.bind_to;
}

e::unpacker
operator >> (e::unpacker lhs, server& rhs)
{
    uint8_t s, t;
    lhs = lhs >> s >> rhs.id >> rhs.weaver_id >> t >> rhs.bind_to;
    rhs.state = static_cast<server::state_t>(s);
    rhs.type = static_cast<server::type_t>(t);
    return lhs;
}

size_t
pack_size(const server& p)
{
    return sizeof(uint8_t)
         + sizeof(uint64_t)
         + sizeof(uint64_t)
         + sizeof(int)
         + pack_size(p.bind_to);
}
