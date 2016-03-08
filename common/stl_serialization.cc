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

#define weaver_debug_

#include "common/weaver_constants.h"
#include "common/stl_serialization.h"

uint64_t
message :: size(void*, const bool&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(void*, const uint8_t&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(void*, const uint16_t&)
{
    return sizeof(uint16_t);
}

uint64_t
message :: size(void*, const uint32_t&)
{
    return sizeof(uint32_t);
}

uint64_t
message :: size(void*, const uint64_t&)
{
    return sizeof(uint64_t);
}

uint64_t
message :: size(void*, const int64_t&)
{
    return sizeof(int64_t);
}

uint64_t
message :: size(void*, const int&)
{
    return sizeof(int);
}

uint64_t
message :: size(void*, const double&)
{
    return sizeof(uint64_t);
}

uint64_t
message :: size(void*, const std::string &t)
{
    return t.size() + sizeof(uint32_t);
}

uint64_t
message :: size(void*, const std::vector<bool> &t)
{
    return sizeof(uint32_t) + t.size()*sizeof(uint8_t);
}

uint64_t
message :: size(void *aux_args, const char* &t)
{
    size_t *buf_sz_ptr = (size_t*)aux_args;
    return sizeof(uint32_t) + *buf_sz_ptr * sizeof(uint8_t);
}


// pack

void
message :: pack_buffer(e::packer &packer, void*, const bool &t)
{
    uint8_t to_pack = 0;
    if (t) {
        to_pack = 1;
    }
    packer = packer << to_pack;
}

void 
message :: pack_buffer(e::packer &packer, void*, const uint8_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, void*, const uint16_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, void*, const uint32_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, void*, const uint64_t &t)
{
    packer = packer << t;
}

void
message :: pack_buffer(e::packer &packer, void*, const int64_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, void*, const int &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::packer &packer, void*, const double &t)
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
        pack_buffer(packer, nullptr, rawwords[i]);
    }

    for (uint32_t i = 0; i < leftover_chars; i++) {
        pack_buffer(packer, nullptr, (uint8_t) rawchars[words*8+i]);
    }
}

void 
message :: pack_buffer(e::packer &packer, void*, const std::string &t)
{
    WDEBUG << "packing string = " << t << std::endl;
    assert(t.size() <= UINT32_MAX);
    uint32_t strlen = t.size();
    packer = packer << strlen;

    pack_string(packer, t, strlen);
}

void
message :: pack_buffer(e::packer &packer, void*, const std::vector<bool> &t)
{
    uint32_t sz = t.size();
    pack_buffer(packer, nullptr, sz);
    for (bool b: t) {
        pack_buffer(packer, nullptr, b);
    }
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const char* &t)
{
    std::cout << "packing chararr" << std::endl;
    size_t *buf_sz_ptr = (size_t*)aux_args;
    std::cout << "buf_sz_ptr=" << buf_sz_ptr << std::endl;
    uint32_t len = *buf_sz_ptr;
    std::cout << "packing buffer, sz=" << len << std::endl;
    pack_buffer(packer, nullptr, len);
    for (uint32_t i = 0; i < len; i++) {
        uint8_t next_char = t[i];
        pack_buffer(packer, nullptr, next_char);
    }
}


// unpack

void
message :: unpack_buffer(e::unpacker &unpacker, void*, bool &t)
{
    uint8_t temp;
    unpacker = unpacker >> temp;
    t = (temp != 0);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void*, uint8_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, void*, uint16_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, void*, uint32_t &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, void*, uint64_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, void*, int64_t &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, void*, int &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, void*, double &t)
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
        unpack_buffer(unpacker, nullptr, rawwords[i]);
    }

    for (uint32_t i = 0; i < leftover_chars; i++) {
        unpack_buffer(unpacker, nullptr, rawuint8s[words*8+i]);
    }
}

void 
message :: unpack_buffer(e::unpacker &unpacker, void*, std::string &t)
{
    uint32_t strlen;
    unpack_buffer(unpacker, nullptr, strlen);

    unpack_string(unpacker, t, strlen);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void*, std::vector<bool> &t)
{
    uint32_t sz;
    unpack_buffer(unpacker, nullptr, sz);
    t.reserve(sz);
    bool b;
    for (uint32_t i = 0; i < sz; i++) {
        unpack_buffer(unpacker, nullptr, b);
        t.push_back(b);
    }
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, char* &t)
{
    uint32_t len = 0;
    unpack_buffer(unpacker, aux_args, len);
    std::cout << "unpacking buffer, sz=" << len << std::endl;

    t = new char[len];
    for (uint32_t i = 0; i < len; i++) {
        uint8_t next_char;
        unpack_buffer(unpacker, aux_args, next_char);
        t[i] = next_char;
    }
}
