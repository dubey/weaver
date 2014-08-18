/*
 * ===============================================================
 *    Description:  Base class for Hyperdex client, used by both
 *                  db::hyper_stub and coordinator::hyper_stub.
 *
 *        Created:  2014-02-26 15:23:54
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_hyper_stub_base_h_
#define weaver_common_hyper_stub_base_h_

#include <memory>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <e/endian.h>
#include <e/buffer.h>
#include <hyperdex/client.hpp>

#include "common/message.h"

class hyper_stub_base
{
    protected:
        typedef int64_t (hyperdex::Client::*hyper_func)(const char*,
            const char*,
            size_t,
            const struct hyperdex_client_attribute*,
            size_t,
            hyperdex_client_returncode*);
        typedef int64_t (hyperdex::Client::*hyper_map_func)(const char*,
            const char*,
            size_t,
            const struct hyperdex_client_map_attribute*,
            size_t,
            hyperdex_client_returncode*);
        hyperdex::Client cl;        
        bool call(hyper_func h,
            const char *space,
            const char *key, size_t key_sz,
            hyperdex_client_attribute *cl_attr, size_t num_attrs);
        bool map_call(hyper_map_func h,
            const char *space,
            const char *key, size_t key_sz,
            hyperdex_client_map_attribute *map_attr, size_t num_attrs);
        bool multiple_call(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs);
        bool multiple_call(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs,
            std::vector<hyper_map_func> &map_funcs,
            std::vector<const char*> &map_spaces,
            std::vector<const char*> &map_keys, std::vector<size_t> &map_key_szs,
            std::vector<hyperdex_client_map_attribute*> &map_attrs, std::vector<size_t> &map_num_attrs);
        bool get(const char *space,
            const char *key, size_t key_sz,
            const hyperdex_client_attribute **cl_attr, size_t *num_attrs);
        bool multiple_get(std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<const hyperdex_client_attribute**> &cl_attrs, std::vector<size_t*> &num_attrs);
        bool del(const char* space,
            const char *key, size_t key_sz);
        bool multiple_del(std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs);


        template <typename T> void prepare_buffer(const T &t, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, T &t);
        template <typename T> void prepare_buffer(const std::unordered_map<uint64_t, T> &map, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, T> &map);
        template <typename T> void prepare_buffer(const std::unordered_map<std::string, T> &map, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<std::string, T> &map);
        void prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map, std::unique_ptr<e::buffer> &buf);
        void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, uint64_t> &map);
        void prepare_buffer(const std::unordered_set<uint64_t> &set, std::unique_ptr<e::buffer> &buf);
        void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<uint64_t> &set);
        void prepare_buffer(const std::unordered_set<std::string> &set, std::unique_ptr<e::buffer> &buf);
        void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<std::string> &set);

    private:
        void pack_uint64(e::buffer::packer &packer, uint64_t num);

    public:
        hyper_stub_base();
};

// store the given t as a HYPERDATATYPE_STRING
template <typename T>
inline void
hyper_stub_base :: prepare_buffer(const T &t, std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = message::size(t);
    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack_at(0);
    message::pack_buffer(packer, t);
}

// unpack the HYPERDATATYPE_STRING in to the given object
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, T &t)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    message::unpack_buffer(unpacker, t);
}

// store the given unordered_map as a HYPERDATATYPE_MAP_INT64_STRING
template <typename T>
inline void
hyper_stub_base :: prepare_buffer(const std::unordered_map<uint64_t, T> &map, std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = 0;
    std::vector<uint64_t> sorted;
    sorted.reserve(map.size());
    std::vector<uint32_t> val_sz;
    for (auto &p: map) {
        sorted.emplace_back(p.first);
        val_sz.emplace_back(message::size(p.second));
        buf_sz += sizeof(p.first) // map key
                + sizeof(uint32_t) // map val encoding sz
                + val_sz.back(); // map val encoding
    }
    std::sort(sorted.begin(), sorted.end());

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();
    // now iterate in sorted order
    uint64_t i = 0;
    for (uint64_t key: sorted) {
        pack_uint64(packer, key);
        message::pack_buffer(packer, map[key]);
        i++;
    }
}

// unpack the HYPERDATATYPE_MAP_INT64_STRING in to the given map
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, T> &map)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    const uint8_t *cur = ebuf->data();
    e::unpacker unpacker = ebuf->unpack_from(0);
    uint64_t key;

    while (!unpacker.empty()) {
        cur = e::unpack64le(cur, &key);
        unpacker.advance(8);
        message::unpack_buffer(unpacker, map[key]);
        cur += message::size(map[key]);
    }
}

// store the given unordered_map as a HYPERDATATYPE_MAP_STRING_STRING
template <typename T>
inline void
hyper_stub_base :: prepare_buffer(const std::unordered_map<std::string, T> &map, std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = 0;
    std::vector<std::string> sorted;
    sorted.reserve(map.size());
    std::vector<uint32_t> key_sz;
    std::vector<uint32_t> val_sz;
    for (auto &p: map) {
        sorted.emplace_back(p.first);
        key_sz.emplace_back(message::size(p.first));
        val_sz.emplace_back(message::size(p.second));
        buf_sz += sizeof(uint32_t) // map key encoding sz
                + key_sz.back()
                + sizeof(uint32_t) // map val encoding sz
                + val_sz.back(); // map val encoding
    }
    std::sort(sorted.begin(), sorted.end());

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();
    // now iterate in sorted order
    uint64_t i = 0;
    for (const std::string &key: sorted) {
        message::pack_buffer(packer, key);
        message::pack_buffer(packer, map.at(key));
        i++;
    }
}

// unpack the HYPERDATATYPE_MAP_STRING_STRING in to the given map
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<std::string, T> &map)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    std::string key;

    while (!unpacker.empty()) {
        key.erase();
        message::unpack_buffer(unpacker, key);
        message::unpack_buffer(unpacker, map[key]);
    }
}

inline void
hyper_stub_base :: pack_uint64(e::buffer::packer &pkr, uint64_t num)
{
    uint8_t intbuf[8];
    e::pack64le(num, intbuf);
    e::slice intslc(intbuf, 8);
    pkr = pkr.copy(intslc);
}

#endif
