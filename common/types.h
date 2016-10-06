/*
 * ===============================================================
 *    Description:  Data types in Weaver.
 *
 *        Created:  2014-07-07 13:17:21
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_types_h_
#define weaver_common_types_h_

#include "common/utils.h"

//#define weaver_strict_type_check_
#ifdef weaver_strict_type_check_

#include <e/buffer.h>

#define WEAVER_UINT64(TYPE) \
    struct TYPE \
    { \
        uint64_t value; \
     \
        TYPE(uint64_t v) : value(v) { } \
     \
        TYPE& operator=(TYPE rhs) \
        { \
            value = rhs.value; \
            return *this; \
        } \
     \
        TYPE& operator++() \
        { \
            value++; \
            return *this; \
        } \
     \
        TYPE operator++(int) \
        { \
            TYPE tmp \
            tmp.value = value; \
            operator++(); \
            return tmp; \
        } \
     \
        TYPE& operator--() \
        { \
            value--; \
            return *this; \
        } \
     \
        TYPE operator--(int) \
        { \
            TYPE tmp \
            tmp.value = value; \
            operator--(); \
            return tmp; \
        } \
     \
        TYPE& operator+=(const TYPE &rhs) \
        { \
            value += rhs.value; \
            return *this; \
        } \
     \
        TYPE& operator-=(const TYPE &rhs) \
        { \
            value -= rhs.value; \
            return *this; \
        } \
     \
    }; \
     \
    inline std::ostream& \
    operator<<(std::ostream& os, const TYPE &t) \
    { \
        os << t.value; \
        return os; \
    } \
     \
    inline std::istream& \
    operator>>(std::istream& is, TYPE &t) \
    { \
        is >> t.value; \
        return is; \
    } \
     \
    inline bool \
    operator==(const TYPE& lhs, const TYPE& rhs) \
    { \
        return lhs.value == rhs.value; \
    } \
     \
    inline bool \
    operator!=(const TYPE& lhs, const TYPE& rhs) \
    { \
        return !operator==(lhs,rhs); \
    } \
     \
    inline bool \
    operator< (const TYPE& lhs, const TYPE& rhs) \
    { \
        return lhs.value < rhs.value; \
    } \
     \
    inline bool \
    operator> (const TYPE& lhs, const TYPE& rhs) \
    { \
        return  operator< (rhs,lhs); \
    } \
     \
    inline bool \
    operator<=(const TYPE& lhs, const TYPE& rhs) \
    { \
        return !operator> (lhs,rhs); \
    } \
     \
    inline bool \
    operator>=(const TYPE& lhs, const TYPE& rhs) \
    { \
        return !operator< (lhs,rhs); \
    } \
     \
    inline TYPE operator+(TYPE lhs, const TYPE& rhs) \
    { \
        lhs += rhs; \
        return lhs; \
    } \
     \
    inline TYPE operator-(TYPE lhs, const TYPE& rhs) \
    { \
        lhs -= rhs; \
        return lhs; \
    } \
     \
    namespace std \
    { \
        template <> \
        struct hash<TYPE> \
        { \
            size_t operator()(const TYPE& k) const \
            { \
                return hash<uint64_t>()(k.value); \
            } \
        }; \
    } \
     \
    namespace message \
    { \
        uint64_t size(const TYPE &t) \
        { \
            return sizeof(uint64_t); \
        } \
     \
        void pack_buffer(e::packer &packer, const TYPE &t) \
        { \
            packer << t.value; \
        } \
     \
        void unpack_buffer(e::unpacker &unpacker, TYPE &t) \
        { \
            unpacker >> t.value; \
        } \
    }

WEAVER_UINT64(node_id_t);
WEAVER_UINT64(cache_key_t);
typedef std::string node_handle_t;
typedef std::string edge_handle_t;

#else

typedef std::string node_handle_t;
typedef std::string edge_handle_t;
typedef std::string cache_key_t;

#endif

#endif
