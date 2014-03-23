/*
 * ===============================================================
 *    Description:  Implementation of hyper_stub_base.
 *
 *        Created:  2014-02-26 15:28:58
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#include "common/hyper_stub_base.h"

// call hyperdex function h using key hndl, attributes cl_attr, and then loop for response
void
hyper_stub_base :: hyper_call_and_loop(hyper_func h, const char *space,
    uint64_t key, hyperdex_client_attribute *cl_attr, size_t num_attrs)
{
    hyperdex_client_returncode status;
    std::unique_ptr<int64_t> key_buf(new int64_t(key));
    int64_t hdex_id = (cl.*h)(space, (const char*)key_buf.get(), sizeof(int64_t), cl_attr, num_attrs, &status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << hdex_id << ", status = " << status << std::endl;
        return;
    }
    hdex_id = cl.loop(-1, &status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
    }
}

// call hyperdex map function h using key hndl, attributes cl_attr, and then loop for response
void
hyper_stub_base :: hypermap_call_and_loop(hyper_map_func h, const char *space,
    uint64_t key, hyperdex_client_map_attribute *map_attr, size_t num_attrs)
{
    hyperdex_client_returncode status;
    std::unique_ptr<int64_t> key_buf(new int64_t(key));
    int64_t hdex_id = (cl.*h)(space, (const char*)key_buf.get(), sizeof(int64_t), map_attr, num_attrs, &status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex map function failed, op id = " << hdex_id << ", status = " << status << std::endl;
        return;
    }
    hdex_id = cl.loop(-1, &status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
    }
}

// multiple hyperdex calls
void
hyper_stub_base :: hyper_multiple_call_and_loop(std::vector<hyper_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<uint64_t> &keys,
    std::vector<hyperdex_client_attribute*> &attrs,
    std::vector<size_t> &num_attrs)
{
    std::vector<const char*> map_spaces;
    std::vector<hyper_map_func> map_funcs;
    std::vector<uint64_t> map_keys;
    std::vector<hyperdex_client_map_attribute*> map_attrs;
    std::vector<size_t> map_num_attrs;
    hyper_multiple_call_and_loop(funcs, spaces, keys, attrs, num_attrs,
        map_funcs, map_spaces, map_keys, map_attrs, map_num_attrs);
}

void
hyper_stub_base :: hyper_multiple_call_and_loop(std::vector<hyper_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<uint64_t> &keys,
    std::vector<hyperdex_client_attribute*> &attrs,
    std::vector<size_t> &num_attrs,
    std::vector<hyper_map_func> &map_funcs,
    std::vector<const char*> &map_spaces,
    std::vector<uint64_t> &map_keys,
    std::vector<hyperdex_client_map_attribute*> &map_attrs,
    std::vector<size_t> &map_num_attrs)
{
    uint64_t num_calls = funcs.size();
    assert(num_calls == spaces.size());
    assert(num_calls == keys.size());
    assert(num_calls == num_attrs.size());
    assert(num_calls == attrs.size());

    uint64_t map_num_calls = map_funcs.size();
    assert(map_num_calls == map_spaces.size());
    assert(map_num_calls == map_keys.size());
    assert(map_num_calls == map_num_attrs.size());
    assert(map_num_calls == map_attrs.size());

    hyperdex_client_returncode status;
    int hdex_id;

    uint64_t i = 0;
    for (; i < num_calls; i++) {
        hdex_id = (cl.*funcs[i])(spaces[i], (const char*)&keys[i], sizeof(int64_t), attrs[i], num_attrs[i], &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex function failed, op id = " << hdex_id << ", status = " << status << std::endl;
            break;
        }
    }
    for (uint64_t j = 0; j < map_num_calls; j++, i++) {
        hdex_id = (cl.*map_funcs[j])(map_spaces[j], (const char*)&map_keys[j], sizeof(int64_t), map_attrs[j], map_num_attrs[j], &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex map function failed, op id = " << hdex_id << ", status = " << status << std::endl;
            return;
        }
    }
    for (; i > 0; i--) {
        hdex_id = cl.loop(-1, &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
        }
    }
}

void
hyper_stub_base :: hyper_get_and_loop(const char *space, uint64_t key,
    const hyperdex_client_attribute **cl_attr, size_t *num_attrs)
{
    hyperdex_client_returncode status;
    std::unique_ptr<int64_t> key_buf(new int64_t(key));
    int64_t hdex_id = cl.get(space, (const char*)key_buf.get(), sizeof(int64_t), &status, cl_attr, num_attrs);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex get failed, op id = " << hdex_id << ", status = " << status << std::endl;
        return;
    }
    hdex_id = cl.loop(-1, &status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
    }
}

void
hyper_stub_base :: hyper_multiple_get_and_loop(std::vector<const char*> &spaces,
    std::vector<uint64_t> &keys,
    std::vector<const hyperdex_client_attribute**> &cl_attrs,
    std::vector<size_t*> &num_attrs)
{
    uint64_t num_calls = spaces.size();
    assert(num_calls == keys.size());
    assert(num_calls == num_attrs.size());
    assert(num_calls == cl_attrs.size());

    hyperdex_client_returncode status;
    int64_t hdex_id;
    
    uint64_t i = 0;
    for (; i < num_calls; i++) {
        hdex_id = cl.get(spaces[i], (const char*)&keys[i], sizeof(int64_t), &status, cl_attrs[i], num_attrs[i]);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex get failed, op id = " << hdex_id << ", status = " << status << std::endl;
            break;
        }
    }
    for (; i > 0; i--) {
        hdex_id = cl.loop(-1, &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
            continue;
        }
    }
}

// delete the given key
void
hyper_stub_base :: hyper_del_and_loop(const char *space, uint64_t key)
{
    hyperdex_client_returncode status;
    int64_t hdex_id = cl.del(space, (const char*)&key, sizeof(int64_t), &status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex get failed, op id = " << hdex_id << ", status = " << status << std::endl;
        return;
    }
    hdex_id = cl.loop(-1, &status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
    }
}

// store the given unordered_map<int, int> as a HYPERDATATYPE_MAP_INT64_INT64
void
hyper_stub_base :: prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map, std::unique_ptr<char> &ret_buf, uint64_t &buf_sz)
{
    buf_sz = map.size() * (sizeof(int64_t) + sizeof(int64_t));
    char *buf = (char*)malloc(buf_sz);
    ret_buf.reset(buf);
    
    for (auto &p: map) {
        buf = e::pack64le(p.first, buf);
        buf = e::pack64le(p.second, buf);
    }
}

// unpack the HYPERDATATYPE_MAP_INT64_INT64 in to an unordered_map<int, int>
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, uint64_t> &map)
{
    uint64_t num_entries = buf_sz / (sizeof(int64_t) + sizeof(int64_t));
    map.reserve(num_entries);
    uint64_t key, val;

    for (uint64_t i = 0; i < num_entries; i++) {
        buf = e::unpack64le(buf, &key);
        buf = e::unpack64le(buf, &val);
        map.emplace(key, val);
    }
}

// store the given unordered_set as a HYPERDATATYPE_SET_INT64
void
hyper_stub_base :: prepare_buffer(const std::unordered_set<uint64_t> &set, std::unique_ptr<char> &ret_buf, uint64_t &buf_sz)
{
    buf_sz = sizeof(uint64_t) * set.size();
    std::set<uint64_t> sorted;
    for (uint64_t x: set) {
        sorted.emplace(x);
    }

    char *buf = (char*)malloc(buf_sz);
    ret_buf.reset(buf);
    // now iterate in sorted order
    for (uint64_t x: sorted) {
        buf = e::pack64le(x, buf);
    }
}

// unpack the HYPERDATATYPE_SET_INT64 in to an unordered_set
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<uint64_t> &set)
{
    uint64_t set_sz = buf_sz / sizeof(uint64_t);
    uint64_t next;
    set.reserve(set_sz);

    for (uint64_t i = 0; i < set_sz; i++) {
        buf = e::unpack64le(buf, &next);
        set.emplace(next);
    }
}
