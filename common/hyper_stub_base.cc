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
    : graph_attrs{"shard",
        "creat_time",
        "properties",
        "out_edges",
        "migr_status", // 0 for stable, 1 for moving
        "last_upd_clk",
        "restore_clk"}
    , graph_dtypes{HYPERDATATYPE_INT64,
        HYPERDATATYPE_STRING,
        HYPERDATATYPE_MAP_STRING_STRING,
        HYPERDATATYPE_MAP_STRING_STRING,
        HYPERDATATYPE_INT64,
        HYPERDATATYPE_STRING,
        HYPERDATATYPE_STRING}
    , tx_attrs{"vt_id",
        "tx_data"}
    , tx_dtypes{HYPERDATATYPE_INT64,
        HYPERDATATYPE_STRING}
    , index_attrs{"node",
        "shard"}
    , index_dtypes{HYPERDATATYPE_STRING,
        HYPERDATATYPE_INT64}
    , cl(hyperdex_client_create(HyperdexCoordIpaddr, HyperdexCoordPort))
{ }


#ifdef weaver_benchmark_ // assert false on error

#define HYPERDEX_CHECK_ID(status) \
    if (hdex_id < 0) { \
        WDEBUG << "Hyperdex function failed, op id = " << hdex_id \
               << ", status = " << hyperdex_client_returncode_to_string(status) << std::endl; \
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl; \
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl; \
        assert(false); \
        success = false; \
    } else { \
        success_calls++; \
    }

#define HYPERDEX_CALL(h, space, key, key_sz, attr, attr_sz, call_status) \
    { \
        int num_hdex_calls = 0; \
        do { \
            hdex_id = h(hyper_tx, space, key, key_sz, attr, attr_sz, &call_status); \
        } while (hdex_id < 0 && call_status == HYPERDEX_CLIENT_INTERRUPTED && num_hdex_calls++ < 5); \
    } \
    HYPERDEX_CHECK_ID(call_status);

#define HYPERDEX_CALL_NOTX(h, space, key, key_sz, attr, attr_sz, call_status) \
    { \
        int num_hdex_calls = 0; \
        do { \
            hdex_id = h(cl, space, key, key_sz, attr, attr_sz, &call_status); \
        } while (hdex_id < 0 && call_status == HYPERDEX_CLIENT_INTERRUPTED && num_hdex_calls++ < 5); \
    } \
    HYPERDEX_CHECK_ID(call_status);

#define HYPERDEX_GET(space, key, key_sz, get_status, attr, attr_sz) \
    { \
        int num_hdex_calls = 0; \
        do { \
            hdex_id = hyperdex_client_xact_get(hyper_tx, space, key, key_sz, &get_status, attr, attr_sz); \
        } while (hdex_id < 0 && (get_status == HYPERDEX_CLIENT_INTERRUPTED) && num_hdex_calls++ < 5); \
    } \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_GET_NOTX(space, key, key_sz, get_status, attr, attr_sz) \
    { \
        int num_hdex_calls = 0; \
        do { \
            hdex_id = hyperdex_client_get(cl, space, key, key_sz, &get_status, attr, attr_sz); \
        } while (hdex_id < 0 && (get_status == HYPERDEX_CLIENT_INTERRUPTED) && num_hdex_calls++ < 5); \
    } \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_GET_PARTIAL(space, key, key_sz, attrnames, attrnames_sz, get_status, attr, attr_sz) \
    { \
        int num_hdex_calls = 0; \
        do { \
            hdex_id = hyperdex_client_xact_get_partial(hyper_tx, space, key, key_sz, attrnames, attrnames_sz, &get_status, attr, attr_sz); \
        } while (hdex_id < 0 && (get_status == HYPERDEX_CLIENT_INTERRUPTED) && num_hdex_calls++ < 5); \
    } \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_GET_PARTIAL_NOTX(space, key, key_sz, attrnames, attrnames_sz, get_status, attr, attr_sz) \
    { \
        int num_hdex_calls = 0; \
        do { \
            hdex_id = hyperdex_client_get_partial(cl, space, key, key_sz, attrnames, attrnames_sz, &get_status, attr, attr_sz); \
        } while (hdex_id < 0 && (get_status == HYPERDEX_CLIENT_INTERRUPTED) && num_hdex_calls++ < 5); \
    } \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_DEL(space, key, key_sz, del_status) \
    { \
        int num_hdex_calls = 0; \
        do { \
            hdex_id = hyperdex_client_xact_del(hyper_tx, space, key, key_sz, &del_status); \
        } while (hdex_id < 0 && del_status == HYPERDEX_CLIENT_INTERRUPTED && num_hdex_calls++ < 5); \
    } \
    HYPERDEX_CHECK_ID(del_status);

#define HYPERDEX_LOOP \
    { \
        int num_hdex_calls = 0; \
        do { \
            hdex_id = hyperdex_client_loop(cl, -1, &loop_status); \
        } while (hdex_id < 0 && loop_status == HYPERDEX_CLIENT_INTERRUPTED && num_hdex_calls++ < 5); \
    } \
    HYPERDEX_CHECK_ID(loop_status);

#define HYPERDEX_CHECK_STATUSES(status, fail_check) \
    if ((loop_status != HYPERDEX_CLIENT_SUCCESS) || (fail_check)) { \
        WDEBUG << "hyperdex error" \
               << ", call status: " << hyperdex_client_returncode_to_string(status) \
               << ", loop status: " << hyperdex_client_returncode_to_string(loop_status) << std::endl; \
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl; \
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl; \
        assert(false); \
        success = false; \
    }

#else

#define HYPERDEX_CHECK_ID(status) \
    if (hdex_id < 0) { \
        WDEBUG << "Hyperdex function failed, op id = " << hdex_id \
               << ", status = " << hyperdex_client_returncode_to_string(status) << std::endl; \
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl; \
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl; \
        success = false; \
    } else { \
        success_calls++; \
    }

#define HYPERDEX_CALL(h, space, key, key_sz, attr, attr_sz, call_status) \
    do { \
        hdex_id = h(hyper_tx, space, key, key_sz, attr, attr_sz, &call_status); \
    } while (hdex_id < 0 && call_status == HYPERDEX_CLIENT_INTERRUPTED); \
    HYPERDEX_CHECK_ID(call_status);

#define HYPERDEX_CALL_NOTX(h, space, key, key_sz, attr, attr_sz, call_status) \
    do { \
        hdex_id = h(cl, space, key, key_sz, attr, attr_sz, &call_status); \
    } while (hdex_id < 0 && call_status == HYPERDEX_CLIENT_INTERRUPTED); \
    HYPERDEX_CHECK_ID(call_status);

#define HYPERDEX_GET(space, key, key_sz, get_status, attr, attr_sz) \
    do { \
        hdex_id = hyperdex_client_xact_get(hyper_tx, space, key, key_sz, &get_status, attr, attr_sz); \
    } while (hdex_id < 0 && (get_status == HYPERDEX_CLIENT_INTERRUPTED || get_status == HYPERDEX_CLIENT_COORDFAIL)); \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_GET_NOTX(space, key, key_sz, get_status, attr, attr_sz) \
    do { \
        hdex_id = hyperdex_client_get(cl, space, key, key_sz, &get_status, attr, attr_sz); \
    } while (hdex_id < 0 && (get_status == HYPERDEX_CLIENT_INTERRUPTED || get_status == HYPERDEX_CLIENT_COORDFAIL)); \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_GET_PARTIAL(space, key, key_sz, attrnames, attrnames_sz, get_status, attr, attr_sz) \
    do { \
        hdex_id = hyperdex_client_xact_get_partial(hyper_tx, space, key, key_sz, attrnames, attrnames_sz, &get_status, attr, attr_sz); \
    } while (hdex_id < 0 && (get_status == HYPERDEX_CLIENT_INTERRUPTED || get_status == HYPERDEX_CLIENT_COORDFAIL)); \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_GET_PARTIAL_NOTX(space, key, key_sz, attrnames, attrnames_sz, get_status, attr, attr_sz) \
    do { \
        hdex_id = hyperdex_client_get_partial(cl, space, key, key_sz, attrnames, attrnames_sz, &get_status, attr, attr_sz); \
    } while (hdex_id < 0 && (get_status == HYPERDEX_CLIENT_INTERRUPTED || get_status == HYPERDEX_CLIENT_COORDFAIL)); \
    HYPERDEX_CHECK_ID(get_status);

#define HYPERDEX_DEL(space, key, key_sz, del_status) \
    do { \
        hdex_id = hyperdex_client_xact_del(hyper_tx, space, key, key_sz, &del_status); \
    } while (hdex_id < 0 && del_status == HYPERDEX_CLIENT_INTERRUPTED); \
    HYPERDEX_CHECK_ID(del_status);

#define HYPERDEX_LOOP \
    do { \
        hdex_id = hyperdex_client_loop(cl, -1, &loop_status); \
    } while (hdex_id < 0 && loop_status == HYPERDEX_CLIENT_INTERRUPTED); \
    HYPERDEX_CHECK_ID(loop_status);

#define HYPERDEX_CHECK_STATUSES(status, fail_check) \
    if ((loop_status != HYPERDEX_CLIENT_SUCCESS) || (fail_check)) { \
        WDEBUG << "hyperdex error" \
               << ", call status: " << hyperdex_client_returncode_to_string(status) \
               << ", loop status: " << hyperdex_client_returncode_to_string(loop_status) << std::endl; \
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl; \
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl; \
        success = false; \
    }

#endif


void
hyper_stub_base :: begin_tx()
{
    hyper_tx = hyperdex_client_begin_transaction(cl);
}

void
hyper_stub_base :: commit_tx(hyperdex_client_returncode &commit_status)
{
    bool success = true;
    int success_calls = 0;
    hyperdex_client_returncode loop_status;

    int64_t hdex_id = hyperdex_client_commit_transaction(hyper_tx, &commit_status);
    HYPERDEX_CHECK_ID(commit_status);

    if (success) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(commit_status,
            commit_status != HYPERDEX_CLIENT_ABORTED && commit_status != HYPERDEX_CLIENT_SUCCESS);
    }
}

void
hyper_stub_base :: abort_tx()
{
    hyperdex_client_abort_transaction(hyper_tx);
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

    HYPERDEX_CALL_NOTX(h, space, key, key_sz, cl_attr, num_attrs, call_status);

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(call_status, call_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

// call hyperdex function h using params and then loop for response
bool
hyper_stub_base :: call(hyper_tx_func h,
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
hyper_stub_base :: map_call(hyper_map_tx_func h,
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
hyper_stub_base :: multiple_call(std::vector<hyper_tx_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs)
{
    std::vector<const char*> map_spaces;
    std::vector<hyper_map_tx_func> map_funcs;
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
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs)
{
    uint64_t num_calls = funcs.size();
    assert(num_calls == spaces.size());
    assert(num_calls == keys.size());
    assert(num_calls == key_szs.size());
    assert(num_calls == num_attrs.size());
    assert(num_calls == attrs.size());

    hyperdex_client_returncode call_status[num_calls];
    int hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;

    for (uint64_t i = 0; i < num_calls; i++) {
        HYPERDEX_CALL_NOTX(funcs[i], spaces[i], keys[i], key_szs[i], attrs[i], num_attrs[i], call_status[i]);

        assert(opid_to_idx.find(hdex_id) == opid_to_idx.end());
        opid_to_idx[hdex_id] = i;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (uint64_t i = 0; i < num_loops; i++) {
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
hyper_stub_base :: multiple_call(std::vector<hyper_tx_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs,
    std::vector<hyper_map_tx_func> &map_funcs,
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
    const hyperdex_client_attribute **cl_attr, size_t *num_attrs,
    bool tx)
{
    hyperdex_client_returncode get_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    if (tx) {
        HYPERDEX_GET(space, key, key_sz, get_status, cl_attr, num_attrs);
    } else {
        HYPERDEX_GET_NOTX(space, key, key_sz, get_status, cl_attr, num_attrs);
    }

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(get_status, get_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

bool
hyper_stub_base :: get_partial(const char *space,
    const char *key, size_t key_sz,
    const char** attrnames, size_t attrnames_sz,
    const hyperdex_client_attribute **cl_attr, size_t *num_attrs,
    bool tx)
{
    hyperdex_client_returncode get_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    if (tx) {
        HYPERDEX_GET_PARTIAL(space, key, key_sz, attrnames, attrnames_sz, get_status, cl_attr, num_attrs);
    } else {
        HYPERDEX_GET_PARTIAL_NOTX(space, key, key_sz, attrnames, attrnames_sz, get_status, cl_attr, num_attrs);
    }

    if (success_calls == 1) {
        HYPERDEX_LOOP;
        HYPERDEX_CHECK_STATUSES(get_status, get_status != HYPERDEX_CLIENT_SUCCESS);
    }

    return success;
}

bool
hyper_stub_base :: multiple_get(std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<const hyperdex_client_attribute**> &cl_attrs, std::vector<size_t*> &num_attrs,
    bool tx)
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
        if (tx) {
            HYPERDEX_GET(spaces[i], keys[i], key_szs[i], get_status[i], cl_attrs[i], num_attrs[i]);
        } else {
            HYPERDEX_GET_NOTX(spaces[i], keys[i], key_szs[i], get_status[i], cl_attrs[i], num_attrs[i]);
        }

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

bool
hyper_stub_base :: multiple_get_partial(std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    const char** attrnames, size_t attrnames_sz, // all have same attrname
    std::vector<const hyperdex_client_attribute**> &cl_attrs, std::vector<size_t*> &num_attrs,
    bool tx)
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
        if (tx) {
            HYPERDEX_GET_PARTIAL(spaces[i], keys[i], key_szs[i], attrnames, attrnames_sz, get_status[i], cl_attrs[i], num_attrs[i]);
        } else {
            HYPERDEX_GET_PARTIAL_NOTX(spaces[i], keys[i], key_szs[i], attrnames, attrnames_sz, get_status[i], cl_attrs[i], num_attrs[i]);
        }

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
#undef HYPERDEX_CALL_NOTX
#undef HYPERDEX_GET
#undef HYPERDEX_GET_NOTX
#undef HYPERDEX_DEL
#undef HYPERDEX_LOOP
#undef HYPERDEX_CHECK_STATUSES

bool
hyper_stub_base :: recreate_node(const hyperdex_client_attribute *cl_attr, db::element::node &n)
{
    std::vector<int> idx(NUM_GRAPH_ATTRS, -1);    
    for (int i = 0; i < NUM_GRAPH_ATTRS; i++) {
        for (int j = 0; j < NUM_GRAPH_ATTRS; j++) {
            if (strcmp(cl_attr[i].attr, graph_attrs[j]) == 0) {
                idx[j] = i;
                break;
            }
        }
    }
    std::vector<bool> check_idx(NUM_GRAPH_ATTRS, false);
    for (int i = 0; i < NUM_GRAPH_ATTRS; i++) {
        assert(idx[i] != -1);
        assert(!check_idx[idx[i]]);
        check_idx[idx[i]] = true;
    }

    // shard
    n.shard = *((uint64_t*)cl_attr[idx[0]].value);
    // create clock
    vc::vclock create_clk;
    unpack_buffer(cl_attr[idx[1]].value, cl_attr[idx[1]].value_sz, create_clk);
    // properties
    unpack_buffer<db::element::property>(cl_attr[idx[2]].value, cl_attr[idx[2]].value_sz, n.base.properties);

    n.state = db::element::node::mode::STABLE;
    n.in_use = false;
    n.base.update_creat_time(create_clk);

    // out edges
    unpack_buffer<db::element::edge*>(cl_attr[idx[3]].value, cl_attr[idx[3]].value_sz, n.out_edges);
    // last update clock
    unpack_buffer(cl_attr[idx[5]].value, cl_attr[idx[5]].value_sz, n.last_upd_clk);
    // restore clock
    unpack_buffer(cl_attr[idx[6]].value, cl_attr[idx[6]].value_sz, n.restore_clk);
    assert(n.restore_clk.size() == ClkSz);

    return true;
}

bool
hyper_stub_base :: get_node(db::element::node &n)
{
    const hyperdex_client_attribute *attr;
    size_t num_attrs;
    node_handle_t handle = n.get_handle();

    bool success = get(graph_space, handle.c_str(), handle.size(), &attr, &num_attrs, true);
    if (success) {
        success = recreate_node(attr, n);
        hyperdex_client_destroy_attrs(attr, num_attrs);
    }

    return success;
}

bool
hyper_stub_base :: get_nodes(std::unordered_map<node_handle_t, db::element::node*> &nodes, bool tx)
{
    int64_t num_nodes = nodes.size();
    std::vector<const char*> spaces(num_nodes, graph_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<const hyperdex_client_attribute**> attrs(num_nodes, NULL);
    std::vector<size_t*> num_attrs(num_nodes, NULL);

    const hyperdex_client_attribute *attr_array[num_nodes];
    size_t num_attrs_array[num_nodes];

    int64_t i = 0;
    for (auto &x: nodes) {
        keys[i] = x.first.c_str();
        key_szs[i] = x.first.size();
        attrs[i] = attr_array + i;
        num_attrs[i] = num_attrs_array + i;

        i++;
    }

    bool success = multiple_get(spaces, keys, key_szs, attrs, num_attrs, tx);

    if (success) {
        for (i = 0; i < num_nodes; i++) {
            if (*num_attrs[i] == NUM_GRAPH_ATTRS) {
                bool recr_success = recreate_node(attr_array[i], *nodes[node_handle_t(keys[i])]);
                hyperdex_client_destroy_attrs(attr_array[i], NUM_GRAPH_ATTRS);
                success = success && recr_success;
            } else {
                WDEBUG << "bad num attributes " << *num_attrs[i] << std::endl;
                if (*num_attrs[i] > 0) {
                    hyperdex_client_destroy_attrs(attr_array[i], *num_attrs[i]);
                }
            }
        }
    }

    return success;
}

void
hyper_stub_base :: prepare_node(hyperdex_client_attribute *cl_attr,
    db::element::node &n,
    std::unique_ptr<e::buffer> &creat_clk_buf,
    std::unique_ptr<e::buffer> &props_buf,
    std::unique_ptr<e::buffer> &out_edges_buf,
    std::unique_ptr<e::buffer> &last_clk_buf,
    std::unique_ptr<e::buffer> &restore_clk_buf)
{
    // shard
    cl_attr[0].attr = graph_attrs[0];
    cl_attr[0].value = (const char*)&n.shard;
    cl_attr[0].value_sz = sizeof(int64_t);
    cl_attr[0].datatype = graph_dtypes[0];

    // create clock
    prepare_buffer(n.base.get_creat_time(), creat_clk_buf);
    cl_attr[1].attr = graph_attrs[1];
    cl_attr[1].value = (const char*)creat_clk_buf->data();
    cl_attr[1].value_sz = creat_clk_buf->size();
    cl_attr[1].datatype = graph_dtypes[1];

    // properties
    prepare_buffer<db::element::property>(n.base.properties, props_buf);
    cl_attr[2].attr = graph_attrs[2];
    cl_attr[2].value = (const char*)props_buf->data();
    cl_attr[2].value_sz = props_buf->size();
    cl_attr[2].datatype = graph_dtypes[2];

    // out edges
    prepare_buffer<db::element::edge*>(n.out_edges, out_edges_buf);
    cl_attr[3].attr = graph_attrs[3];
    cl_attr[3].value = (const char*)out_edges_buf->data();
    cl_attr[3].value_sz = out_edges_buf->size();
    cl_attr[3].datatype = graph_dtypes[3];

    // migr status
    int64_t status = STABLE;
    cl_attr[4].attr = graph_attrs[4];
    cl_attr[4].value = (const char*)&status;
    cl_attr[4].value_sz = sizeof(int64_t);
    cl_attr[4].datatype = graph_dtypes[4];

    // last update clock
    prepare_buffer(n.last_upd_clk, last_clk_buf);
    cl_attr[5].attr = graph_attrs[5];
    cl_attr[5].value = (const char*)last_clk_buf->data();
    cl_attr[5].value_sz = last_clk_buf->size();
    cl_attr[5].datatype = graph_dtypes[5];

    // restore clock
    assert(n.restore_clk.size() == ClkSz);
    prepare_buffer(n.restore_clk, restore_clk_buf);
    cl_attr[6].attr = graph_attrs[6];
    cl_attr[6].value = (const char*)restore_clk_buf->data();
    cl_attr[6].value_sz = restore_clk_buf->size();
    cl_attr[6].datatype = graph_dtypes[6];
}

//bool
//hyper_stub_base :: put_node(db::element::node &n)
//{
//    hyperdex_client_attribute cl_attr[NUM_GRAPH_ATTRS];
//    std::unique_ptr<e::buffer> creat_clk_buf;
//    std::unique_ptr<e::buffer> props_buf;
//    std::unique_ptr<e::buffer> out_edges_buf;
//    std::unique_ptr<e::buffer> last_clk_buf;
//    std::unique_ptr<e::buffer> restore_clk_buf;
//
//    prepare_node(cl_attr, n, creat_clk_buf, props_buf, out_edges_buf, last_clk_buf, restore_clk_buf);
//
//    node_handle_t handle = n.get_handle();
//    return call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), cl_attr, NUM_GRAPH_ATTRS);
//}

bool
hyper_stub_base :: put_nodes(std::unordered_map<node_handle_t, db::element::node*> &nodes, bool if_not_exist)
{
    int num_nodes = nodes.size();
    std::vector<hyper_tx_func> funcs;
    if (if_not_exist) {
         funcs = std::vector<hyper_tx_func>(num_nodes, &hyperdex_client_xact_put_if_not_exist);
    } else {
         funcs = std::vector<hyper_tx_func>(num_nodes, &hyperdex_client_xact_put);
    }
    std::vector<const char*> spaces(num_nodes, graph_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<hyperdex_client_attribute*> attrs(num_nodes);
    std::vector<size_t> num_attrs(num_nodes, NUM_GRAPH_ATTRS);
    std::vector<std::unique_ptr<e::buffer>> creat_clk_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> props_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> out_edges_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> last_clk_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> restore_clk_buf(num_nodes);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_nodes * NUM_GRAPH_ATTRS * sizeof(hyperdex_client_attribute));

    int i = 0;
    for (auto &p: nodes) {
        attrs[i] = attrs_to_add + NUM_GRAPH_ATTRS*i;
        keys[i] = p.first.c_str();
        key_szs[i] = p.first.size();

        prepare_node(attrs[i], *p.second, creat_clk_buf[i], props_buf[i], out_edges_buf[i], last_clk_buf[i], restore_clk_buf[i]);

        i++;
    }

    bool success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);

    free(attrs_to_add);

    return success;
}

bool
hyper_stub_base :: put_nodes_bulk(std::unordered_map<node_handle_t, db::element::node*> &nodes)
{
    int num_nodes = nodes.size();
    std::vector<hyper_func> funcs(num_nodes, &hyperdex_client_put);
    std::vector<const char*> spaces(num_nodes, graph_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<hyperdex_client_attribute*> attrs(num_nodes);
    std::vector<size_t> num_attrs(num_nodes, NUM_GRAPH_ATTRS);
    std::vector<std::unique_ptr<e::buffer>> creat_clk_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> props_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> out_edges_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> last_clk_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> restore_clk_buf(num_nodes);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_nodes * NUM_GRAPH_ATTRS * sizeof(hyperdex_client_attribute));

    int i = 0;
    for (auto &p: nodes) {
        attrs[i] = attrs_to_add + NUM_GRAPH_ATTRS*i;
        keys[i] = p.first.c_str();
        key_szs[i] = p.first.size();

        prepare_node(attrs[i], *p.second, creat_clk_buf[i], props_buf[i], out_edges_buf[i], last_clk_buf[i], restore_clk_buf[i]);

        i++;
    }

    bool success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);

    free(attrs_to_add);

    return success;
}

bool
hyper_stub_base :: del_node(const node_handle_t &handle)
{
    return del(graph_space, handle.c_str(), handle.size());
}

bool
hyper_stub_base :: del_nodes(std::unordered_set<node_handle_t> &to_del)
{
    int64_t num_nodes = to_del.size();
    std::vector<const char*> spaces(num_nodes, graph_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);

    int i = 0;
    for (const node_handle_t &n: to_del) {
        keys[i] = n.c_str();
        key_szs[i] = n.size();

        i++;
    }

    return multiple_del(spaces, keys, key_szs);
}

/*
void
hyper_stub_base :: update_creat_time(db::element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> creat_clk_buf;
    prepare_buffer(n.base.get_creat_time(), creat_clk_buf);
    cl_attr.attr = graph_attrs[0];
    cl_attr.value = (const char*)creat_clk_buf->data();
    cl_attr.value_sz = creat_clk_buf->size();
    cl_attr.datatype = graph_dtypes[0];

    node_handle_t handle = n.get_handle();
    call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub_base :: update_properties(db::element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> props_buf;
    prepare_buffer(*n.base.get_props(), props_buf);
    cl_attr.attr = graph_attrs[2];
    cl_attr.value = (const char*)props_buf->data();
    cl_attr.value_sz = props_buf->size();
    cl_attr.datatype = graph_dtypes[2];

    node_handle_t handle = n.get_handle();
    call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub_base :: add_out_edge(db::element::node &n, db::element::edge *e)
{
    hyperdex_client_map_attribute map_attr;
    std::unique_ptr<e::buffer> key_buf, val_buf;
    prepare_buffer(e->get_handle(), key_buf);
    prepare_buffer(e, val_buf);
    map_attr.attr = graph_attrs[3];
    map_attr.map_key = (const char*)key_buf->data();
    map_attr.map_key_sz = key_buf->size();
    map_attr.map_key_datatype = HYPERDATATYPE_STRING;
    map_attr.value = (const char*)val_buf->data();
    map_attr.value_sz = val_buf->size();
    map_attr.value_datatype = HYPERDATATYPE_STRING;

    node_handle_t handle = n.get_handle();
    map_call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), &map_attr, 1);
}

void
hyper_stub_base :: remove_out_edge(db::element::node &n, db::element::edge *e)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> key_buf;
    prepare_buffer(e->get_handle(), key_buf);
    cl_attr.attr = graph_attrs[3];
    cl_attr.value = (const char*)key_buf->data();
    cl_attr.value_sz = key_buf->size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    node_handle_t handle = n.get_handle();
    call(&hyperdex_client_xact_map_remove, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub_base :: add_in_nbr(const node_handle_t &n_hndl, const node_handle_t &nbr)
{
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[4];
    cl_attr.value = nbr.c_str();
    cl_attr.value_sz = nbr.size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    call(&hyperdex_client_xact_set_add, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}

void
hyper_stub_base :: remove_in_nbr(const node_handle_t &n_hndl, const node_handle_t &nbr)
{
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[4];
    cl_attr.value = nbr.c_str();
    cl_attr.value_sz = nbr.size();
    cl_attr.datatype = HYPERDATATYPE_STRING;

    call(&hyperdex_client_xact_set_remove, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}

void
hyper_stub_base :: update_tx_queue(db::element::node &n)
{
    hyperdex_client_attribute cl_attr;
    std::unique_ptr<e::buffer> txq_buf;
    prepare_buffer(n.tx_queue, txq_buf);
    cl_attr.attr = graph_attrs[5];
    cl_attr.value = (const char*)txq_buf->data();
    cl_attr.value_sz = txq_buf->size();
    cl_attr.datatype = graph_dtypes[5];

    node_handle_t handle = n.get_handle();
    call(&hyperdex_client_xact_put, graph_space, handle.c_str(), handle.size(), &cl_attr, 1);
}

void
hyper_stub_base :: update_migr_status(const node_handle_t &n_hndl, enum persist_node_state status)
{
    int64_t int_status = status;
    hyperdex_client_attribute cl_attr;
    cl_attr.attr = graph_attrs[6];
    cl_attr.value = (const char*)&int_status;
    cl_attr.value_sz = sizeof(int64_t);
    cl_attr.datatype = graph_dtypes[6];

    call(&hyperdex_client_xact_put, graph_space, n_hndl.c_str(), n_hndl.size(), &cl_attr, 1);
}


// store the given unordered_map<int, int> as a HYPERDATATYPE_MAP_INT64_INT64
void
hyper_stub_base :: prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map, std::unique_ptr<e::buffer> &buf)
{
    std::vector<uint64_t> sorted;
    sorted.reserve(map.size());
    for (auto &p: map) {
        sorted.emplace_back(p.first);
    }
    std::sort(sorted.begin(), sorted.end());

    uint64_t buf_sz = map.size() * (sizeof(int64_t) + sizeof(int64_t));
    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    for (uint64_t key: sorted) {
        pack_uint64(packer, key);
        pack_uint64(packer, map.at(key));
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
hyper_stub_base :: prepare_buffer(const std::unordered_set<uint64_t> &set, std::unique_ptr<e::buffer> &buf)
{
    std::vector<uint64_t> sorted;
    sorted.reserve(set.size());
    for (uint64_t x: set) {
        sorted.emplace_back(x);
    }
    std::sort(sorted.begin(), sorted.end());

    uint64_t buf_sz = sizeof(uint64_t) * set.size();
    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    // now iterate in sorted order
    for (uint64_t x: sorted) {
        pack_uint64(packer, x);
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

// store the unordered_set<string> as a HYPERDATATYPE_SET_STRING
void
hyper_stub_base :: prepare_buffer(const std::unordered_set<std::string> &set, std::unique_ptr<e::buffer> &buf)
{
    std::vector<std::string> sorted;
    sorted.reserve(set.size());
    uint64_t buf_sz = 0;

    for (const std::string &s: set) {
        sorted.emplace_back(s);
        buf_sz += sizeof(uint32_t) + s.size();
    }
    std::sort(sorted.begin(), sorted.end());

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    for (const std::string &s: sorted) {
        message::pack_buffer(packer, s);
    }
}

// unpack the HYPERDATATYPE_SET_STRING in to an unordered_set<string>
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<std::string> &set)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    std::string next;

    while (!unpacker.empty()) {
        next.erase();
        message::unpack_buffer(unpacker, next);
        set.emplace(next);
    }
}
*/

bool
hyper_stub_base :: update_nmap(const node_handle_t &handle, uint64_t loc)
{
    hyperdex_client_attribute attr;
    attr.attr = graph_attrs[0];
    attr.value = (const char*)&loc;
    attr.value_sz = sizeof(int64_t);
    attr.datatype = graph_dtypes[0];

    return call(hyperdex_client_put, graph_space, handle.c_str(), handle.size(), &attr, 1);
}

uint64_t
hyper_stub_base :: get_nmap(node_handle_t &handle)
{
    const hyperdex_client_attribute *attr;
    size_t num_attrs;
    uint64_t shard = UINT64_MAX;

    bool success = get_partial(graph_space, handle.c_str(), handle.size(), graph_attrs, 1, &attr, &num_attrs, false);
    if (success) {
        assert(num_attrs == 1);
        assert(attr->value_sz == sizeof(int64_t));
        shard = *((uint64_t*)attr->value);
        hyperdex_client_destroy_attrs(attr, num_attrs);
    }

    return shard;
}

std::unordered_map<node_handle_t, uint64_t>
hyper_stub_base :: get_nmap(std::unordered_set<node_handle_t> &toGet, bool tx)
{
    int64_t num_nodes = toGet.size();
    std::vector<const char*> spaces(num_nodes, graph_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<const hyperdex_client_attribute**> attrs(num_nodes, NULL);
    std::vector<size_t*> num_attrs(num_nodes, NULL);

    const hyperdex_client_attribute *attr_array[num_nodes];
    size_t num_attrs_array[num_nodes];

    int i = 0;
    for (auto &n: toGet) {
        keys[i] = n.c_str();
        key_szs[i] = n.size();
        attrs[i] = attr_array + i;
        num_attrs[i] = num_attrs_array + i;

        i++;
    }

    bool success = multiple_get_partial(spaces, keys, key_szs, graph_attrs, 1, attrs, num_attrs, tx);
    UNUSED(success);

    uint64_t *val;
    std::unordered_map<node_handle_t, uint64_t> mappings;
    mappings.reserve(num_nodes);
    for (int64_t i = 0; i < num_nodes; i++) {
        if (*num_attrs[i] == 1) {
            val = (uint64_t*)attr_array[i]->value;
            mappings[node_handle_t(keys[i])] = *val;
            hyperdex_client_destroy_attrs(attr_array[i], 1);
        } else if (*num_attrs[i] > 0) {
            WDEBUG << "bad num attributes " << *num_attrs[i] << std::endl;
            hyperdex_client_destroy_attrs(attr_array[i], *num_attrs[i]);
        }
    }

    return mappings;
}

bool
hyper_stub_base :: add_indices(std::vector<std::string> &indices,
    std::vector<db::element::node*> &nodes,
    bool tx)
{
    assert(AuxIndex == 1);
    uint64_t num_indices = indices.size();
    assert(num_indices == nodes.size());

    std::vector<const char*> spaces(num_indices, index_space);
    std::vector<const char*> keys(num_indices);
    std::vector<size_t> key_szs(num_indices);
    std::vector<hyperdex_client_attribute*> attrs(num_indices);
    std::vector<size_t> num_attrs(num_indices, NUM_INDEX_ATTRS);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_indices * NUM_INDEX_ATTRS * sizeof(hyperdex_client_attribute));

    for (uint64_t i = 0; i < num_indices; i++) {
        attrs[i] = attrs_to_add + NUM_INDEX_ATTRS*i;
        keys[i] = indices[i].c_str();
        key_szs[i] = indices[i].size();

        // node handle
        node_handle_t handle = nodes[i]->get_handle();
        attrs[i][0].attr = index_attrs[0];
        attrs[i][0].value = handle.c_str();
        attrs[i][0].value_sz = handle.size();
        attrs[i][0].datatype = index_dtypes[0];

        // shard
        attrs[i][1].attr = index_attrs[1];
        attrs[i][1].value = (const char*)&nodes[i]->shard;
        attrs[i][1].value_sz = sizeof(int64_t);
        attrs[i][1].datatype = index_dtypes[1];
    }

    bool success;
    if (tx) {
        std::vector<hyper_tx_func> funcs = std::vector<hyper_tx_func>(num_indices, &hyperdex_client_xact_put_if_not_exist);
        success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);
    } else {
        std::vector<hyper_func> funcs = std::vector<hyper_func>(num_indices, &hyperdex_client_put_if_not_exist);
        success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);
    }

    free(attrs_to_add);

    return success;
}

bool
hyper_stub_base :: del_indices(std::vector<std::string> &indices)
{
    assert(AuxIndex == 1);

    int64_t num_indices = indices.size();
    std::vector<const char*> spaces(num_indices, index_space);
    std::vector<const char*> keys(num_indices);
    std::vector<size_t> key_szs(num_indices);

    int i = 0;
    for (const std::string &idx: indices) {
        keys[i] = idx.c_str();
        key_szs[i] = idx.size();

        i++;
    }

    return multiple_del(spaces, keys, key_szs);
}


void
hyper_stub_base :: pack_uint64(e::buffer::packer &pkr, uint64_t num)
{
    uint8_t intbuf[8];
    e::pack64le(num, intbuf);
    e::slice intslc(intbuf, 8);
    pkr = pkr.copy(intslc);
}

void
hyper_stub_base :: unpack_uint64(e::unpacker &unpacker, uint64_t &num)
{
    uint8_t intbuf[8];
    for (size_t i = 0; i < 8; i++) {
        unpacker = unpacker >> intbuf[i];
    }
    e::unpack64le(intbuf, &num);
}

void
hyper_stub_base :: pack_uint32(e::buffer::packer &pkr, uint32_t num)
{
    uint8_t intbuf[4];
    e::pack32le(num, intbuf);
    e::slice intslc(intbuf, 4);
    pkr = pkr.copy(intslc);
}

void
hyper_stub_base :: unpack_uint32(e::unpacker &unpacker, uint32_t &num)
{
    uint8_t intbuf[4];
    for (size_t i = 0; i < 4; i++) {
        unpacker = unpacker >> intbuf[i];
    }
    e::unpack32le(intbuf, &num);
}

void
hyper_stub_base :: pack_string(e::buffer::packer &packer, const std::string &s)
{
    uint8_t *rawchars = (uint8_t*)s.data();

    for (size_t i = 0; i < s.size(); i++) {
        packer = packer << rawchars[i];
    }
}

void
hyper_stub_base :: unpack_string(e::unpacker &unpacker, std::string &s, uint32_t sz)
{
    s.resize(sz);

    uint8_t *rawchars = (uint8_t*)s.data();

    for (uint32_t i = 0; i < sz; i++) {
        unpacker = unpacker >> rawchars[i];
    }
}
