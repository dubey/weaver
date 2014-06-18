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

#define weaver_debug_
#include "common/hyper_stub_base.h"

// call hyperdex function h using key hndl, attributes cl_attr, and then loop for response
void
hyper_stub_base :: hyper_call_and_loop(hyper_func h, const char *space,
    uint64_t key, hyperdex_client_attribute *cl_attr, size_t num_attrs)
{
    hyperdex_client_returncode call_status, loop_status;
    std::unique_ptr<int64_t> key_buf(new int64_t(key));

    int64_t hdex_id = (cl.*h)(space, (const char*)key_buf.get(), sizeof(int64_t), cl_attr, num_attrs, &call_status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << hdex_id << ", status = " << call_status << std::endl;
        assert(false);
        return;
    }

    hdex_id = cl.loop(-1, &loop_status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << loop_status << std::endl;
    }

    if (loop_status != HYPERDEX_CLIENT_SUCCESS || call_status != HYPERDEX_CLIENT_SUCCESS) {
        WDEBUG << "hyperdex error"
               << ", call status: " << call_status
               << ", loop status: " << loop_status << std::endl;
        WDEBUG << "error message: " << cl.error_message() << std::endl;
        WDEBUG << "error loc: " << cl.error_location() << std::endl;
    }
}

// call hyperdex map function h using key hndl, attributes cl_attr, and then loop for response
void
hyper_stub_base :: hypermap_call_and_loop(hyper_map_func h, const char *space,
    uint64_t key, hyperdex_client_map_attribute *map_attr, size_t num_attrs)
{
    hyperdex_client_returncode call_status, loop_status;
    std::unique_ptr<int64_t> key_buf(new int64_t(key));

    int64_t hdex_id = (cl.*h)(space, (const char*)key_buf.get(), sizeof(int64_t), map_attr, num_attrs, &call_status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex map function failed, op id = " << hdex_id << ", status = " << call_status << std::endl;
        return;
    }

    hdex_id = cl.loop(-1, &loop_status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << loop_status << std::endl;
    }

    if (loop_status != HYPERDEX_CLIENT_SUCCESS || call_status != HYPERDEX_CLIENT_SUCCESS) {
        WDEBUG << "hyperdex error"
               << ", call status: " << call_status
               << ", loop status: " << loop_status << std::endl;
        WDEBUG << "error message: " << cl.error_message() << std::endl;
        WDEBUG << "error loc: " << cl.error_location() << std::endl;
        assert(false);
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

    hyperdex_client_returncode call_status[num_calls+map_num_calls];
    int hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);

    uint64_t i = 0;
    for (; i < num_calls; i++) {
        hdex_id = (cl.*funcs[i])(spaces[i], (const char*)&keys[i], sizeof(int64_t), attrs[i], num_attrs[i], call_status+i);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex function failed, op id = " << hdex_id << ", status = " << call_status[i] << std::endl;
            assert(false);
            break;
        }
        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    for (uint64_t j = 0; j < map_num_calls; j++, i++) {
        hdex_id = (cl.*map_funcs[j])(map_spaces[j], (const char*)&map_keys[j], sizeof(int64_t), map_attrs[j], map_num_attrs[j], call_status+i);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex map function failed, op id = " << hdex_id << ", status = " << call_status[i] << std::endl;
            return;
        }
        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    for (; i > 0; i--) {
        hdex_id = cl.loop(-1, &loop_status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << loop_status << std::endl;
        }
        assert(opid_to_idx.find(hdex_id) != opid_to_idx.end());
        int64_t &idx = opid_to_idx[hdex_id];
        assert(idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS || call_status[idx] != HYPERDEX_CLIENT_SUCCESS) {
            WDEBUG << "hyperdex error at call " << idx
                   << ", call status: " << call_status[idx]
                   << ", loop status: " << loop_status << std::endl;
            WDEBUG << "error message: " << cl.error_message() << std::endl;
            WDEBUG << "error loc: " << cl.error_location() << std::endl;
        }
        idx = -1;
    }
}

void
hyper_stub_base :: hyper_get_and_loop(const char *space, uint64_t key,
    const hyperdex_client_attribute **cl_attr, size_t *num_attrs)
{
    hyperdex_client_returncode get_status, loop_status;
    std::unique_ptr<int64_t> key_buf(new int64_t(key));

    int64_t hdex_id = cl.get(space, (const char*)key_buf.get(), sizeof(int64_t), &get_status, cl_attr, num_attrs);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex get failed, op id = " << hdex_id << ", status = " << get_status << std::endl;
        return;
    }

    hdex_id = cl.loop(-1, &loop_status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << loop_status << std::endl;
    }

    if (loop_status != HYPERDEX_CLIENT_SUCCESS || get_status != HYPERDEX_CLIENT_SUCCESS) {
        WDEBUG << "hyperdex error"
               << ", call status: " << get_status
               << ", loop status: " << loop_status << std::endl;
        WDEBUG << "error message: " << cl.error_message() << std::endl;
        WDEBUG << "error loc: " << cl.error_location() << std::endl;
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

    hyperdex_client_returncode get_status[num_calls];
    int64_t hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    
    uint64_t i = 0;
    for (; i < num_calls; i++) {
        hdex_id = cl.get(spaces[i], (const char*)&keys[i], sizeof(int64_t), get_status+i, cl_attrs[i], num_attrs[i]);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex get failed, op id = " << hdex_id << ", status = " << get_status[i] << std::endl;
            break;
        }
        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    for (; i > 0; i--) {
        hdex_id = cl.loop(-1, &loop_status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << loop_status << std::endl;
            continue;
        }
        assert(opid_to_idx.find(hdex_id) != opid_to_idx.end());
        int64_t &idx = opid_to_idx[hdex_id];
        assert(idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS || get_status[idx] != HYPERDEX_CLIENT_SUCCESS) {
            WDEBUG << "hyperdex error"
                   << ", call status: " << get_status[idx]
                   << ", loop status: " << loop_status << std::endl;
            WDEBUG << "error message: " << cl.error_message() << std::endl;
            WDEBUG << "error loc: " << cl.error_location() << std::endl;
        }

        idx = -1;
    }
}

// delete the given key
void
hyper_stub_base :: hyper_del_and_loop(const char *space, uint64_t key)
{
    hyperdex_client_returncode del_status, loop_status;
    int64_t hdex_id = cl.del(space, (const char*)&key, sizeof(int64_t), &del_status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex get failed, op id = " << hdex_id << ", status = " << del_status << std::endl;
        return;
    }

    hdex_id = cl.loop(-1, &loop_status);
    if (hdex_id < 0) {
        WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << loop_status << std::endl;
    }

    if (loop_status != HYPERDEX_CLIENT_SUCCESS || del_status != HYPERDEX_CLIENT_SUCCESS) {
        WDEBUG << "hyperdex error"
               << ", call status: " << del_status
               << ", loop status: " << loop_status << std::endl;
        WDEBUG << "error message: " << cl.error_message() << std::endl;
        WDEBUG << "error loc: " << cl.error_location() << std::endl;
    }
}

// store the given unordered_map<int, int> as a HYPERDATATYPE_MAP_INT64_INT64
void
hyper_stub_base :: prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map, std::unique_ptr<char> &ret_buf, uint64_t &buf_sz)
{
    std::vector<uint64_t> sorted;
    sorted.reserve(map.size());
    for (auto &p: map) {
        sorted.emplace_back(p.first);
    }
    std::sort(sorted.begin(), sorted.end());

    buf_sz = map.size() * (sizeof(int64_t) + sizeof(int64_t));
    char *buf = (char*)malloc(buf_sz);
    ret_buf.reset(buf);
    
    for (uint64_t key: sorted) {
        buf = e::pack64le(key, buf);
        buf = e::pack64le(map.at(key), buf);
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
    std::vector<uint64_t> sorted;
    sorted.reserve(set.size());
    for (uint64_t x: set) {
        sorted.emplace_back(x);
    }
    std::sort(sorted.begin(), sorted.end());

    buf_sz = sizeof(uint64_t) * set.size();
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
