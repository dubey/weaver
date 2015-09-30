/*
 * ===============================================================
 *    Description:  enum serialization implementation
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/enum_serialization.h"
#include "common/passert.h"

uint64_t
message :: size(const enum node_prog::prog_type&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const enum transaction::update_type&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const enum transaction::tx_type&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const enum predicate::relation&)
{
    return sizeof(uint8_t);
}

void
message :: pack_buffer(e::packer &packer, const enum node_prog::prog_type &t)
{
    PASSERT(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: pack_buffer(e::packer &packer, const enum transaction::update_type &t)
{
    PASSERT(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: pack_buffer(e::packer &packer, const enum transaction::tx_type &t)
{
    PASSERT(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: pack_buffer(e::packer &packer, const enum predicate::relation &t)
{
    PASSERT(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: unpack_buffer(e::unpacker &unpacker, enum node_prog::prog_type &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum node_prog::prog_type)_type;
}

void
message :: unpack_buffer(e::unpacker &unpacker, enum transaction::update_type &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum transaction::update_type)_type;
}

void
message :: unpack_buffer(e::unpacker &unpacker, enum transaction::tx_type &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum transaction::tx_type)_type;
}

void
message :: unpack_buffer(e::unpacker &unpacker, enum predicate::relation &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum predicate::relation)_type;
}
