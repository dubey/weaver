/*
 * ===============================================================
 *    Description:  Macros for ID packers and unpackers.
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
#include "common/ids.h"

#define CREATE_ID(TYPE) \
    std::ostream& \
    operator << (std::ostream& lhs, const TYPE ## _id& rhs) \
    { \
        return lhs << #TYPE "(" << rhs.get() << ")"; \
    } \
    e::packer \
    operator << (e::packer pa, const TYPE ## _id& rhs) \
    { \
        return pa << rhs.get(); \
    } \
    e::unpacker \
    operator >> (e::unpacker up, TYPE ## _id& rhs) \
    { \
        uint64_t id; \
        up = up >> id; \
        rhs = TYPE ## _id(id); \
        return up; \
    }

CREATE_ID(server)

#undef CREATE_ID
