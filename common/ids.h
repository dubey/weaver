/*
 * ===============================================================
 *    Description:  Macros for generating ID classes, which are
 *                  basically wrappers around a uint64_t.
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

#ifndef weaver_common_ids_h_
#define weaver_common_ids_h_

// C
#include <stdint.h>

// C++
#include <iostream>

// e
#include <e/buffer.h>

// An ID is a simple wrapper around uint64_t in order to prevent devs from
// accidently using one type of ID as another.

#define OPERATOR(TYPE, OP) \
    inline bool \
    operator OP (const TYPE ## _id& lhs, const TYPE ## _id& rhs) \
    { \
        return lhs.get() OP rhs.get(); \
    }
#define CREATE_ID(TYPE) \
    class TYPE ## _id \
    { \
        public: \
            TYPE ## _id() : m_id(0) {} \
            explicit TYPE ## _id(uint64_t id) : m_id(id) {} \
        public: \
            uint64_t get() const { return m_id; } \
        private: \
            uint64_t m_id; \
    }; \
    std::ostream& \
    operator << (std::ostream& lhs, const TYPE ## _id& rhs); \
    inline size_t \
    pack_size(const TYPE ## _id&) \
    { \
        return sizeof(uint64_t); \
    } \
    e::buffer::packer \
    operator << (e::buffer::packer pa, const TYPE ## _id& rhs); \
    e::unpacker \
    operator >> (e::unpacker up, TYPE ## _id& rhs); \
    OPERATOR(TYPE, <) \
    OPERATOR(TYPE, <=) \
    OPERATOR(TYPE, ==) \
    OPERATOR(TYPE, !=) \
    OPERATOR(TYPE, >=) \
    OPERATOR(TYPE, >)

CREATE_ID(server)

#undef OPERATOR
#undef CREATE_ID

#endif
