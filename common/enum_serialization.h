/*
 * ===============================================================
 *    Description:  serialization for Weaver enums
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_enum_serialization_h_
#define weaver_common_enum_serialization_h_

#include <e/serialization.h>

#include "common/transaction.h"
#include "common/property_predicate.h"
#include "node_prog/node_prog_type.h"

namespace message
{
    uint64_t size(const enum transaction::update_type&);
    uint64_t size(const enum transaction::tx_type&);
    uint64_t size(const enum predicate::relation&);
    uint64_t size(const enum node_prog::prog_type&);
    void pack_buffer(e::packer &packer, const enum transaction::update_type &t);
    void pack_buffer(e::packer &packer, const enum transaction::tx_type &t);
    void pack_buffer(e::packer &packer, const enum predicate::relation &t);
    void pack_buffer(e::packer &packer, const enum node_prog::prog_type &t);
    void unpack_buffer(e::unpacker &unpacker, enum transaction::update_type &t);
    void unpack_buffer(e::unpacker &unpacker, enum transaction::tx_type &t);
    void unpack_buffer(e::unpacker &unpacker, enum predicate::relation &t);
    void unpack_buffer(e::unpacker &unpacker, enum node_prog::prog_type &t);
}

#endif
