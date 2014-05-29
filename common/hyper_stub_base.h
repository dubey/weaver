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
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <e/endian.h>
#include <e/buffer.h>
#include <hyperdex/client.hpp>

#include "common/weaver_constants.h"
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
        void hyper_call_and_loop(hyper_func h, const char *space, uint64_t key, hyperdex_client_attribute *cl_attr, size_t num_attrs);
        void hypermap_call_and_loop(hyper_map_func h, const char *space, uint64_t key, hyperdex_client_map_attribute *map_attr, size_t num_attrs);
        void hyper_multiple_call_and_loop(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<uint64_t> &keys,
            std::vector<hyperdex_client_attribute*> &attrs,
            std::vector<size_t> &num_attrs);
        void hyper_multiple_call_and_loop(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<uint64_t> &keys,
            std::vector<hyperdex_client_attribute*> &attrs,
            std::vector<size_t> &num_attrs,
            std::vector<hyper_map_func> &map_funcs,
            std::vector<const char*> &map_spaces,
            std::vector<uint64_t> &map_keys,
            std::vector<hyperdex_client_map_attribute*> &map_attrs,
            std::vector<size_t> &map_num_attrs);
        void hyper_get_and_loop(const char *space, uint64_t key, const hyperdex_client_attribute **cl_attr, size_t *num_attrs);
        void hyper_multiple_get_and_loop(std::vector<const char*> &spaces,
            std::vector<uint64_t> &keys,
            std::vector<const hyperdex_client_attribute**> &cl_attrs,
            std::vector<size_t*> &num_attrs);
        void hyper_del_and_loop(const char* space, uint64_t key);


        template <typename T> void prepare_buffer(const T &t, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, T &t);
        template <typename T> void prepare_buffer(const std::unordered_map<uint64_t, T> &map, std::unique_ptr<char> &buf, uint64_t &buf_sz);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, T> &map);
        void prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map, std::unique_ptr<char> &buf, uint64_t &buf_sz);
        void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, uint64_t> &map);
        void prepare_buffer(const std::unordered_set<uint64_t> &set, std::unique_ptr<char> &buf, uint64_t &buf_sz);
        void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<uint64_t> &set);

    public:
        hyper_stub_base() : cl(HYPERDEX_COORD_IPADDR, HYPERDEX_COORD_PORT) { }
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
hyper_stub_base :: prepare_buffer(const std::unordered_map<uint64_t, T> &map, std::unique_ptr<char> &ret_buf, uint64_t &buf_sz)
{
    buf_sz = 0;
    std::set<uint64_t> sorted;
    std::vector<uint32_t> val_sz;
    for (auto &p: map) {
        sorted.emplace(p.first);
        val_sz.emplace_back(message::size(p.second));
        buf_sz += sizeof(p.first) // map key
                + sizeof(uint32_t) // map val encoding sz
                + val_sz.back(); // map val encoding
    }

    char *buf = (char*)malloc(buf_sz);
    ret_buf.reset(buf);
    // now iterate in sorted order
    uint64_t i = 0;
    std::unique_ptr<e::buffer> temp_buf;
    for (uint64_t hndl: sorted) {
        buf = e::pack64le(hndl, buf);
        buf = e::pack32le(val_sz[i], buf);
        temp_buf.reset(e::buffer::create(val_sz[i]));
        e::buffer::packer packer = temp_buf->pack_at(0);
        message::pack_buffer(packer, map.at(hndl));
        memmove(buf, temp_buf->data(), val_sz[i]);
        buf += val_sz[i];
        i++;
    }
}

// unpack the HYPERDATATYPE_MAP_INT64_STRING in to the given map
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, T> &map)
{
    const char *end = buf + buf_sz;
    uint64_t key;
    uint32_t val_sz;
    std::unique_ptr<e::buffer> temp_buf;

    while (buf != end) {
        buf = e::unpack64le(buf, &key);
        buf = e::unpack32le(buf, &val_sz);
        temp_buf.reset(e::buffer::create(buf, val_sz));
        e::unpacker unpacker = temp_buf->unpack_from(0);
        message::unpack_buffer(unpacker, map[key]);
        buf += val_sz;
    }
}

#endif
