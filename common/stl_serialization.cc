/*
 * ===============================================================
 *    Description:  STL serialization implementation.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/weaver_constants.h"
#include "common/stl_serialization.h"

uint64_t
message :: size(const bool&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const char&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const uint16_t&)
{
    return sizeof(uint16_t);
}

uint64_t
message :: size(const uint32_t&)
{
    return sizeof(uint32_t);
}

uint64_t
message :: size(const uint64_t&)
{
    return sizeof(uint64_t);
}

uint64_t
message :: size(const int64_t&)
{
    return sizeof(int64_t);
}

uint64_t
message :: size(const int&)
{
    return sizeof(int);
}

uint64_t
message :: size(const double&)
{
    return sizeof(uint64_t);
}

uint64_t
message :: size(const std::string &t)
{
    return t.size() + sizeof(uint32_t);
}

uint64_t
message :: size(const std::vector<bool> &t)
{
    return sizeof(uint32_t) + t.size()*sizeof(uint8_t);
}


// pack

void
message :: pack_buffer(e::packer &packer, const bool &t)
{
    uint8_t to_pack = 0;
    if (t) {
        to_pack = 1;
    }
    packer = packer << to_pack;
}

void 
message :: pack_buffer(e::packer &packer, const uint8_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, const uint16_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, const uint32_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, const uint64_t &t)
{
    packer = packer << t;
}

void
message :: pack_buffer(e::packer &packer, const int64_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, const int &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, const double &t)
{
    uint64_t dbl;
    memcpy(&dbl, &t, sizeof(double)); //to avoid casting issues, probably could avoid copy
    packer = packer << dbl;
}

void
message :: pack_string(e::packer &packer, const std::string &t, const uint32_t sz)
{
    uint32_t words = sz / 8;
    uint32_t leftover_chars = sz % 8;

    const char *rawchars = t.data();
    const uint64_t *rawwords = (const uint64_t*) rawchars;

    for (uint32_t i = 0; i < words; i++) {
        pack_buffer(packer, rawwords[i]);
    }

    for (uint32_t i = 0; i < leftover_chars; i++) {
        pack_buffer(packer, (uint8_t) rawchars[words*8+i]);
    }
}

void 
message :: pack_buffer(e::packer &packer, const std::string &t)
{
    assert(t.size() <= UINT32_MAX);
    uint32_t strlen = t.size();
    packer = packer << strlen;

    pack_string(packer, t, strlen);
}

void
message :: pack_buffer(e::packer &packer, const std::vector<bool> &t)
{
    uint32_t sz = t.size();
    pack_buffer(packer, sz);
    for (bool b: t) {
        pack_buffer(packer, b);
    }
}


// unpack

void
message :: unpack_buffer(e::unpacker &unpacker, bool &t)
{
    uint8_t temp;
    unpacker = unpacker >> temp;
    t = (temp != 0);
}

void
message :: unpack_buffer(e::unpacker &unpacker, uint8_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, uint16_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, uint32_t &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, uint64_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, int64_t &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, int &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, double &t)
{
    uint64_t dbl;
    unpacker = unpacker >> dbl;
    memcpy(&t, &dbl, sizeof(double)); //to avoid casting issues, probably could avoid copy
}

void
message :: unpack_string(e::unpacker &unpacker, std::string &t, const uint32_t sz)
{
    t.resize(sz);

    uint32_t words = sz / 8;
    uint32_t leftover_chars = sz % 8;

    const char* rawchars = t.data();
    uint8_t* rawuint8s = (uint8_t*) rawchars;
    uint64_t* rawwords = (uint64_t*) rawchars;

    for (uint32_t i = 0; i < words; i++) {
        unpack_buffer(unpacker, rawwords[i]);
    }

    for (uint32_t i = 0; i < leftover_chars; i++) {
        unpack_buffer(unpacker, rawuint8s[words*8+i]);
    }
}

void 
message :: unpack_buffer(e::unpacker &unpacker, std::string &t)
{
    uint32_t strlen;
    unpack_buffer(unpacker, strlen);

    unpack_string(unpacker, t, strlen);
}

void
message :: unpack_buffer(e::unpacker &unpacker, std::vector<bool> &t)
{
    uint32_t sz;
    unpack_buffer(unpacker, sz);
    t.reserve(sz);
    bool b;
    for (uint32_t i = 0; i < sz; i++) {
        unpack_buffer(unpacker, b);
        t.push_back(b);
    }
}


