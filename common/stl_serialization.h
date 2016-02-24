/*
 * ===============================================================
 *    Description:  Serialization for STL data structures.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_stl_serialization_h_
#define weaver_common_stl_serialization_h_

#include <stdint.h>
#include <memory>
#include <vector>
#include <deque>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <google/sparse_hash_set>
#include <google/sparse_hash_map>
#include <google/dense_hash_map>
#include <queue>
#include <string>
#include <e/serialization.h>

#include "common/weaver_serialization.h"

namespace message
{
    uint64_t size(void*, const bool&);
    uint64_t size(void*, const char&);
    uint64_t size(void*, const uint16_t&);
    uint64_t size(void*, const uint32_t&);
    uint64_t size(void*, const uint64_t&);
    uint64_t size(void*, const int64_t&);
    uint64_t size(void*, const int&);
    uint64_t size(void*, const double&);
    uint64_t size(void*, const std::string &t);
    uint64_t size(void*, const std::vector<bool> &t);
    template <typename T1, typename T2, typename T3> inline uint64_t size(void*, const std::set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3> inline uint64_t size(void*, const std::unordered_set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3> inline uint64_t size(void*, const google::sparse_hash_set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3, typename T4> inline uint64_t size(void*, const std::unordered_map<T1, T2, T3, T4>& t);
    template <typename T1, typename T2, typename T3, typename T4> inline uint64_t size(void*, const google::sparse_hash_map<T1, T2, T3, T4>& t);
    template <typename T1, typename T2, typename T3, typename T4> inline uint64_t size(void*, const google::dense_hash_map<T1, T2, T3, T4>& t);
    template <typename T> inline uint64_t size(void*, const std::vector<T>& t);
    template <typename T> inline uint64_t size(void*, const std::deque<T>& t);
    template <typename T1, typename T2, typename T3> uint64_t size(void*, std::priority_queue<T1, T2, T3>);
    template <typename T1, typename T2> inline uint64_t size(void*, const std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline uint64_t size(void*, const std::tuple<T1, T2, T3>& t);
    template <typename T> inline uint64_t size(void*, const std::shared_ptr<T> &ptr_t);
    template <typename T> inline uint64_t size(void*, const std::unique_ptr<T> &ptr_t);

    void pack_buffer(e::packer &packer, void*, const bool &t);
    void pack_buffer(e::packer &packer, void*, const uint8_t &t);
    void pack_buffer(e::packer &packer, void*, const uint16_t &t);
    void pack_buffer(e::packer &packer, void*, const uint32_t &t);
    void pack_buffer(e::packer &packer, void*, const uint64_t &t);
    void pack_buffer(e::packer &packer, void*, const int64_t &t);
    void pack_buffer(e::packer &packer, void*, const int &t);
    void pack_buffer(e::packer &packer, void*, const double &t);
    void pack_string(e::packer &packer, const std::string &t, const uint32_t sz);
    void pack_buffer(e::packer &packer, void*, const std::string &t);
    void pack_buffer(e::packer &packer, void*, const std::vector<bool> &t);
    template <typename T1, typename T2, typename T3> inline void pack_buffer(e::packer& packer, void*, const std::set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3> inline void pack_buffer(e::packer& packer, void*, const std::unordered_set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3> inline void pack_buffer(e::packer&, void*, const google::sparse_hash_set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3, typename T4> void pack_buffer(e::packer& packer, void*, const std::unordered_map<T1, T2, T3, T4>& t);
    template <typename T1, typename T2, typename T3, typename T4> inline void pack_buffer(e::packer&, void*, const google::sparse_hash_map<T1, T2, T3, T4>& t);
    template <typename T1, typename T2, typename T3, typename T4> inline void pack_buffer(e::packer&, void*, const google::dense_hash_map<T1, T2, T3, T4>& t);
    template <typename T> inline void pack_buffer(e::packer& packer, void*, const std::vector<T>& t);
    template <typename T> inline void pack_buffer(e::packer& packer, void*, const std::deque<T>& t);
    template <typename T1, typename T2, typename T3> void pack_buffer(e::packer&, void*, std::priority_queue<T1, T2, T3>);
    template <typename T1, typename T2> inline void pack_buffer(e::packer &packer, void*, const std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline void pack_buffer(e::packer &packer, void*, const std::tuple<T1, T2, T3>& t);
    template <typename T> inline void pack_buffer(e::packer& packer, void*, const std::shared_ptr<T> &ptr_t);
    template <typename T> inline void pack_buffer(e::packer& packer, void*, const std::unique_ptr<T> &ptr_t);

    void unpack_buffer(e::unpacker &unpacker, void*, bool &t);
    void unpack_buffer(e::unpacker &unpacker, void*, uint8_t &t);
    void unpack_buffer(e::unpacker &unpacker, void*, uint16_t &t);
    void unpack_buffer(e::unpacker &unpacker, void*, uint32_t &t);
    void unpack_buffer(e::unpacker &unpacker, void*, uint64_t &t);
    void unpack_buffer(e::unpacker &unpacker, void*, int64_t &t);
    void unpack_buffer(e::unpacker &unpacker, void*, int &t);
    void unpack_buffer(e::unpacker &unpacker, void*, double &t);
    void unpack_string(e::unpacker &unpacker, std::string &t, const uint32_t sz);
    void unpack_buffer(e::unpacker &unpacker, void*, std::string &t);
    void unpack_buffer(e::unpacker &unpacker, void*, std::vector<bool> &t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker &unpacker, void*, std::set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker &unpacker, void*, std::unordered_set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker &unpacker, void*, google::sparse_hash_set<T1, T2, T3>& t);
    template <typename T1, typename T2, typename T3, typename T4> void unpack_buffer(e::unpacker &unpacker, void*, std::unordered_map<T1, T2, T3, T4>& t);
    template <typename T1, typename T2, typename T3, typename T4> void unpack_buffer(e::unpacker &unpacker, void*, google::sparse_hash_map<T1, T2, T3, T4>& t);
    template <typename T1, typename T2, typename T3, typename T4> void unpack_buffer(e::unpacker &unpacker, void*, google::dense_hash_map<T1, T2, T3, T4>& t);
    template <typename T> void unpack_buffer(e::unpacker &unpacker, void*, std::vector<T>& t);
    template <typename T> void unpack_buffer(e::unpacker &unpacker, void*, std::deque<T>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker &unpacker, void*, std::priority_queue<T1, T2, T3>&);
    template <typename T1, typename T2> void unpack_buffer(e::unpacker &unpacker, void*, std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker &unpacker, void*, std::tuple<T1, T2, T3>& t);
    template <typename T> void unpack_buffer(e::unpacker &unpacker, void*, std::shared_ptr<T> &ptr_t);
    template <typename T> void unpack_buffer(e::unpacker &unpacker, void*, std::unique_ptr<T> &ptr_t);


    // size templates

    template <typename T1, typename T2>
    inline uint64_t size(void *aux_args, const std::pair<T1, T2> &t)
    {
        return size(aux_args, t.first) + size(aux_args, t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(void *aux_args, const std::tuple<T1, T2, T3> &t){
        return size(aux_args, std::get<0>(t))
             + size(aux_args, std::get<1>(t))
             + size(aux_args, std::get<2>(t));
    }

    template <typename T>
    inline uint64_t size(void *aux_args, const std::shared_ptr<T> &ptr_t)
    {
        bool dummy;
        uint64_t sz = size(aux_args, dummy);
        if (ptr_t) {
            sz += size(aux_args, *ptr_t);
        }
        return sz;
    }

    template <typename T>
    inline uint64_t size(void *aux_args, const std::unique_ptr<T> &ptr_t)
    {
        bool dummy;
        uint64_t sz = size(aux_args, dummy);
        if (ptr_t) {
            sz += size(aux_args, *ptr_t);
        }
        return sz;
    }

#define SET_SZ \
    uint64_t total_size = sizeof(uint32_t); \
    for (const T1 &elem : t) { \
        total_size += size(aux_args, elem); \
    } \
    return total_size;

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(void *aux_args, const std::set<T1,T2,T3> &t)
    {
        SET_SZ;
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(void *aux_args, const std::unordered_set<T1,T2,T3> &t)
    {
        SET_SZ;
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(void *aux_args, const google::sparse_hash_set<T1,T2,T3> &t)
    {
        SET_SZ;
    }

#undef SET_SZ

#define MAP_SZ \
    uint64_t total_size = sizeof(uint32_t); \
    for (const std::pair<const T1, T2> &pair : t) { \
        total_size += size(aux_args, pair.first) + size(aux_args, pair.second); \
    } \
    return total_size;

    template <typename T1, typename T2, typename T3, typename T4>
    inline uint64_t size(void *aux_args, const std::unordered_map<T1, T2, T3, T4> &t)
    {
        MAP_SZ;
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline uint64_t size(void *aux_args, const google::sparse_hash_map<T1,T2,T3,T4> &t)
    {
        MAP_SZ;
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline uint64_t size(void *aux_args, const google::dense_hash_map<T1,T2,T3,T4> &t)
    {
        MAP_SZ;
    }

#undef MAP_SZ

    template <typename T>
    inline uint64_t size(void *aux_args, const std::vector<T> &t)
    {
        uint64_t tot_size = sizeof(uint32_t);
        for (const T &elem: t) {
            tot_size += size(aux_args, elem);
        }
        return tot_size;
    }
    
    template <typename T>
    inline uint64_t size(void *aux_args, const std::deque<T> &t)
    {
        uint64_t tot_size = sizeof(uint32_t);
        for (const T &elem: t) {
            tot_size += size(aux_args, elem);
        }
        return tot_size;
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(void *aux_args, std::priority_queue<T1, T2, T3> t)
    {
        // cannot iterate pqueue so create a copy, no reference
        uint64_t sz = sizeof(uint32_t);
        while (!t.empty()) {
            sz += size(aux_args, t.top());
            t.pop();
        }
        return sz;
    }

    // packing templates

    template <typename T1, typename T2>
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const std::pair<T1, T2> &t)
    {
        // assumes constant size
        pack_buffer(packer, aux_args, t.first);
        pack_buffer(packer, aux_args, t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline void pack_buffer(e::packer &packer, void *aux_args, const std::tuple<T1, T2, T3> &t){
        pack_buffer(packer, aux_args, std::get<0>(t));
        pack_buffer(packer, aux_args, std::get<1>(t));
        pack_buffer(packer, aux_args, std::get<2>(t));
    }

    template <typename T> inline void pack_buffer(e::packer& packer, void *aux_args, const std::shared_ptr<T> &ptr_t)
    {
        bool exists;
        if (ptr_t) {
            exists = true;
            pack_buffer(packer, aux_args, exists);
            pack_buffer(packer, aux_args, *ptr_t);
        } else {
            exists = false;
            pack_buffer(packer, aux_args, exists);
        }
    }

    template <typename T> inline void pack_buffer(e::packer& packer, void *aux_args, const std::unique_ptr<T> &ptr_t)
    {
        bool exists;
        if (ptr_t) {
            exists = true;
            pack_buffer(packer, aux_args, exists);
            pack_buffer(packer, aux_args, *ptr_t);
        } else {
            exists = false;
            pack_buffer(packer, aux_args, exists);
        }
    }

    template <typename T> 
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const std::vector<T> &t)
    {
        // !assumes constant element size
        assert(t.size() <= UINT32_MAX);
        uint32_t num_elems = t.size();
        pack_buffer(packer, aux_args, num_elems);
        for (const T &elem: t) {
            pack_buffer(packer, aux_args, elem);
        }
    }

    template <typename T> 
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const std::deque<T> &t)
    {
        // !assumes constant element size
        assert(t.size() <= UINT32_MAX);
        uint32_t num_elems = t.size();
        pack_buffer(packer, aux_args, num_elems);
        for (const T &elem : t) {
            pack_buffer(packer, aux_args, elem);
        }
    }

    template <typename T1, typename T2, typename T3>
    inline void
    pack_buffer(e::packer &packer, void *aux_args, std::priority_queue<T1, T2, T3> t)
    {
        assert(t.size() <= UINT32_MAX);
        uint32_t num_elems = t.size();
        packer = packer << num_elems;
        while (!t.empty()) {
            pack_buffer(packer, aux_args, t.top());
            t.pop();
        }
    }

#define SET_PACK \
    assert(t.size() <= UINT32_MAX); \
    uint32_t num_keys = t.size(); \
    pack_buffer(packer, aux_args, num_keys); \
    for (const T1 &elem : t) { \
        pack_buffer(packer, aux_args, elem); \
    }

    template <typename T1, typename T2, typename T3>
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const std::set<T1,T2,T3> &t)
    {
        SET_PACK;
    }

    template <typename T1, typename T2, typename T3>
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const std::unordered_set<T1,T2,T3> &t)
    {
        SET_PACK;
    }

    template <typename T1, typename T2, typename T3>
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const google::sparse_hash_set<T1, T2, T3> &t)
    {
        SET_PACK;
    }

#undef SET_PACK

#define MAP_PACK \
    assert(t.size() <= UINT32_MAX); \
    uint32_t num_keys = t.size(); \
    pack_buffer(packer, aux_args, num_keys); \
    for (const std::pair<const T1, T2> &pair : t) { \
        pack_buffer(packer, aux_args, pair.first); \
        pack_buffer(packer, aux_args, pair.second); \
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const std::unordered_map<T1, T2, T3, T4> &t)
    {
        MAP_PACK;
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const google::sparse_hash_map<T1, T2, T3, T4> &t)
    {
        MAP_PACK;
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline void 
    pack_buffer(e::packer &packer, void *aux_args, const google::dense_hash_map<T1, T2, T3, T4> &t)
    {
        MAP_PACK;
    }

#undef MAP_PACK

    // unpacking templates

    template <typename T1, typename T2>
    inline void 
    unpack_buffer(e::unpacker &unpacker, void *aux_args, std::pair<T1, T2> &t)
    {
        //assumes constant size
        unpack_buffer(unpacker, aux_args, t.first);
        unpack_buffer(unpacker, aux_args, t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline void unpack_buffer(e::unpacker &unpacker, void *aux_args, std::tuple<T1, T2, T3>& t){
        unpack_buffer(unpacker, aux_args, std::get<0>(t));
        unpack_buffer(unpacker, aux_args, std::get<1>(t));
        unpack_buffer(unpacker, aux_args, std::get<2>(t));
    }

    template <typename T> inline void unpack_buffer(e::unpacker &unpacker, void *aux_args, std::shared_ptr<T> &ptr_t)
    {
        bool exists;
        unpack_buffer(unpacker, aux_args, exists);
        if (exists) {
            ptr_t.reset(new T());
            unpack_buffer(unpacker, aux_args, *ptr_t);
        }
    }

    template <typename T> inline void unpack_buffer(e::unpacker &unpacker, void *aux_args, std::unique_ptr<T> &ptr_t)
    {
        bool exists;
        unpack_buffer(unpacker, aux_args, exists);
        if (exists) {
            ptr_t.reset(new T());
            unpack_buffer(unpacker, aux_args, *ptr_t);
        }
    }

    template <typename T> 
    inline void 
    unpack_buffer(e::unpacker &unpacker, void *aux_args, std::vector<T> &t)
    {
        assert(t.size() == 0);
        uint32_t elements_left;
        unpack_buffer(unpacker, aux_args, elements_left);

        t.resize(elements_left);

        for (uint32_t i = 0; i < elements_left; i++) {
            unpack_buffer(unpacker, aux_args, t[i]);
        }
    }

    template <typename T> 
    inline void 
    unpack_buffer(e::unpacker &unpacker, void *aux_args, std::deque<T> &t)
    {
        assert(t.size() == 0);
        uint32_t elements_left;
        unpack_buffer(unpacker, aux_args, elements_left);

        t.resize(elements_left);

        for (uint32_t i = 0; i < elements_left; i++) {
            unpack_buffer(unpacker, aux_args, t[i]);
        }
    }

    template <typename T1, typename T2, typename T3>
    inline void
    unpack_buffer(e::unpacker &unpacker, void *aux_args, std::priority_queue<T1, T2, T3> &t)
    {
        assert(t.size() == 0);
        uint32_t elements_left = 0;
        unpack_buffer(unpacker, aux_args, elements_left);
        while (elements_left > 0) {
            T1 to_add;
            unpack_buffer(unpacker, aux_args, to_add);
            t.push(std::move(to_add));
            elements_left--;
        }
    }

#define SET_UNPACK \
    assert(t.size() == 0); \
    uint32_t elements_left; \
    unpack_buffer(unpacker, aux_args, elements_left); \
    while (elements_left > 0) { \
        T1 new_elem; \
        unpack_buffer(unpacker, aux_args, new_elem); \
        t.insert(new_elem); \
        elements_left--; \
    }

    template <typename T1, typename T2, typename T3>
    inline void 
    unpack_buffer(e::unpacker &unpacker, void *aux_args, std::set<T1,T2,T3> &t)
    {
        SET_UNPACK;
    }

    template <typename T1, typename T2, typename T3>
    inline void 
    unpack_buffer(e::unpacker &unpacker, void *aux_args, std::unordered_set<T1,T2,T3> &t)
    {
        SET_UNPACK;
    }

    template <typename T1, typename T2, typename T3>
    inline void
    unpack_buffer(e::unpacker &unpacker, void *aux_args, google::sparse_hash_set<T1,T2,T3> &t)
    {
        SET_UNPACK;
    }

#undef SET_UNPACK

#define MAP_UNPACK \
    assert(t.size() == 0); \
    uint32_t elements_left; \
    unpack_buffer(unpacker, aux_args, elements_left); \
    while (elements_left > 0) { \
        T1 key_to_add; \
        T2 val_to_add; \
        unpack_buffer(unpacker, aux_args, key_to_add); \
        unpack_buffer(unpacker, aux_args, val_to_add); \
        t[key_to_add] = val_to_add; \
        elements_left--; \
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline void 
    unpack_buffer(e::unpacker &unpacker, void *aux_args, std::unordered_map<T1, T2, T3, T4> &t)
    {
        MAP_UNPACK;
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline void
    unpack_buffer(e::unpacker &unpacker, void *aux_args, google::sparse_hash_map<T1,T2,T3,T4> &t)
    {
        MAP_UNPACK;
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline void
    unpack_buffer(e::unpacker &unpacker, void *aux_args, google::dense_hash_map<T1,T2,T3,T4> &t)
    {
        MAP_UNPACK;
    }

#undef MAP_UNPACK

}

#endif
