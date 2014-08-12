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
#include "common/config_constants.h"

hyper_stub_base :: hyper_stub_base()
    : cl(HyperdexCoordIpaddr, HyperdexCoordPort)
{ }


#define HYPERDEX_CHECK_ID(status) \
    if (hdex_id < 0) { \
        WDEBUG << "Hyperdex function failed, op id = " << hdex_id << ", status = " << status << std::endl; \
        success = false; \
    } else { \
        success_calls++; \
    }

#define HYPERDEX_CALL(h, space, key, key_sz, attr, attr_sz, call_status) \
    hdex_id = (cl.*h)(space, key, key_sz, attr, attr_sz, &call_status); \
    HYPERDEX_CHECK_ID(call_status);

#define HYPERDEX_GET(space, key, key_sz, get_status, attr, attr_sz) \
    hdex_id = cl.get(space, key, key_sz, &get_status, attr, attr_sz); \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_DEL(space, key, key_sz, del_status) \
    hdex_id = cl.del(space, key, key_sz, &del_status); \
    HYPERDEX_CHECK_ID(del_status);

#define HYPERDEX_LOOP \
    hdex_id = cl.loop(-1, &loop_status); \
    HYPERDEX_CHECK_ID(loop_status);

#define HYPERDEX_CHECK_STATUSES(status, fail_check) \
    if (loop_status != HYPERDEX_CLIENT_SUCCESS || (fail_check)) { \
        WDEBUG << "hyperdex error" \
               << ", call status: " << status \
               << ", loop status: " << loop_status << std::endl; \
        WDEBUG << "error message: " << cl.error_message() << std::endl; \
        WDEBUG << "error loc: " << cl.error_location() << std::endl; \
        success = false; \
    }


// call hyperdex function h using params and then loop for response
bool
hyper_stub_base :: call(hyper_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_attribute *cl_attr, size_t num_attrs)
{
    hyperdex_client_returncode call_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    HYPERDEX_CALL(h, space, key, key_sz, cl_attr, num_attrs, call_status);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(call_status, call_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

// call hyperdex map function h using key hndl, attributes cl_attr, and then loop for response
bool
hyper_stub_base :: map_call(hyper_map_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_map_attribute *map_attr, size_t num_attrs)
{
    hyperdex_client_returncode call_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;
    
    HYPERDEX_CALL(h, space, key, key_sz, map_attr, num_attrs, call_status);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(call_status, call_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

// multiple hyperdex calls
bool
hyper_stub_base :: multiple_call(std::vector<hyper_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs)
{
    std::vector<const char*> map_spaces;
    std::vector<hyper_map_func> map_funcs;
    std::vector<const char*> map_keys;
    std::vector<size_t> map_key_szs;
    std::vector<hyperdex_client_map_attribute*> map_attrs;
    std::vector<size_t> map_num_attrs;
    return multiple_call(funcs,
        spaces,
        keys, key_szs,
        attrs, num_attrs,
        map_funcs,
        map_spaces,
        map_keys, map_key_szs,
        map_attrs, map_num_attrs);
}

bool
hyper_stub_base :: multiple_call(std::vector<hyper_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs,
    std::vector<hyper_map_func> &map_funcs,
    std::vector<const char*> &map_spaces,
    std::vector<const char*> &map_keys, std::vector<size_t> &map_key_szs,
    std::vector<hyperdex_client_map_attribute*> &map_attrs, std::vector<size_t> &map_num_attrs)
{
    uint64_t num_calls = funcs.size();
    assert(num_calls == spaces.size());
    assert(num_calls == keys.size());
    assert(num_calls == key_szs.size());
    assert(num_calls == num_attrs.size());
    assert(num_calls == attrs.size());

    uint64_t map_num_calls = map_funcs.size();
    assert(map_num_calls == map_spaces.size());
    assert(map_num_calls == map_keys.size());
    assert(map_num_calls == map_key_szs.size());
    assert(map_num_calls == map_num_attrs.size());
    assert(map_num_calls == map_attrs.size());

    hyperdex_client_returncode call_status[num_calls+map_num_calls];
    int hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;

    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_CALL(funcs[i], spaces[i], keys[i], key_szs[i], attrs[i], num_attrs[i], call_status[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    for (uint64_t j = 0; j < map_num_calls; j++, i++) {
        HYPERDEX_CALL(map_funcs[j], map_spaces[j], map_keys[j], map_key_szs[j], map_attrs[j], map_num_attrs[j], call_status[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        assert(opid_to_idx.find(hdex_id) != opid_to_idx.end());
        int64_t &idx = opid_to_idx[hdex_id];
        assert(idx >= 0);

        HYPERDEX_CHECK_STATUSES(call_status[idx], call_status[idx] != HYPERDEX_CLIENT_SUCCESS);

        idx = -1;
    }

    return success;
}

bool
hyper_stub_base :: get(const char *space,
    const char *key, size_t key_sz,
    const hyperdex_client_attribute **cl_attr, size_t *num_attrs)
{
    hyperdex_client_returncode get_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    HYPERDEX_GET(space, key, key_sz, get_status, cl_attr, num_attrs);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(get_status, get_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

bool
hyper_stub_base :: multiple_get(std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<const hyperdex_client_attribute**> &cl_attrs, std::vector<size_t*> &num_attrs)
{
    uint64_t num_calls = spaces.size();
    assert(num_calls == keys.size());
    assert(num_calls == key_szs.size());
    assert(num_calls == num_attrs.size());
    assert(num_calls == cl_attrs.size());

    hyperdex_client_returncode get_status[num_calls];
    int64_t hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;
    
    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_GET(spaces[i], keys[i], key_szs[i], get_status[i], cl_attrs[i], num_attrs[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        assert(opid_to_idx.find(hdex_id) != opid_to_idx.end());
        int64_t &idx = opid_to_idx[hdex_id];
        assert(idx >= 0);

        HYPERDEX_CHECK_STATUSES(get_status[idx], get_status[idx] != HYPERDEX_CLIENT_SUCCESS);

        idx = -1;
    }

    return success;
}

// delete the given key
bool
hyper_stub_base :: del(const char* space, const char *key, size_t key_sz)
{
    hyperdex_client_returncode del_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    HYPERDEX_DEL(space, key, key_sz, del_status);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(del_status, del_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

// delete multiple keys
bool
hyper_stub_base :: multiple_del(std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs)
{
    uint64_t num_calls = spaces.size();
    assert(keys.size() == num_calls);
    assert(key_szs.size() == num_calls);

    hyperdex_client_returncode del_status[num_calls];
    int64_t hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;
    
    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_DEL(spaces[i], keys[i], key_szs[i], del_status[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        assert(opid_to_idx.find(hdex_id) != opid_to_idx.end());
        int64_t &idx = opid_to_idx[hdex_id];
        assert(idx >= 0);

        HYPERDEX_CHECK_STATUSES(del_status[idx], del_status[idx] != HYPERDEX_CLIENT_SUCCESS && del_status[idx] != HYPERDEX_CLIENT_NOTFOUND);

        idx = -1;
    }

    return success;
}

#undef HYPERDEX_CHECK_ID
#undef HYPERDEX_CALL
#undef HYPERDEX_GET
#undef HYPERDEX_DEL
#undef HYPERDEX_LOOP
#undef HYPERDEX_CHECK_STATUSES


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
