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
#include "common/weaver_constants.h"
#include "common/hyper_stub_base.h"
#include "common/config_constants.h"

hyper_stub_base :: hyper_stub_base()
    : graph_attrs{"shard",
        "creat_time",
        "properties",
        "out_edges",
        "migr_status", // 0 for stable, 1 for moving
        "last_upd_clk",
        "restore_clk",
        "aliases"}
    , graph_dtypes{HYPERDATATYPE_INT64,
        HYPERDATATYPE_STRING,
#ifdef weaver_large_property_maps_
        HYPERDATATYPE_MAP_STRING_STRING,
#else
        HYPERDATATYPE_LIST_STRING,
#endif
        HYPERDATATYPE_MAP_STRING_STRING,
        HYPERDATATYPE_INT64,
        HYPERDATATYPE_STRING,
        HYPERDATATYPE_STRING,
        HYPERDATATYPE_SET_STRING}
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
    if ((loop_status != HYPERDEX_CLIENT_SUCCESS && loop_status != HYPERDEX_CLIENT_TIMEOUT) || (fail_check)) { \
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

#define DELAYED_INSERT_HDEX_ID \
    if (delayed_opid_to_idx.find(hdex_id) != delayed_opid_to_idx.end()) { \
        WDEBUG << "logical error: repeated hdex_id " << hdex_id << std::endl; \
        return false; \
    } \
    delayed_opid_to_idx.emplace(hdex_id, delayed_idx);

#define DELAYED_CHECK_HDEX_ID \
    if (delayed_opid_to_idx.find(hdex_id) == delayed_opid_to_idx.end()) { \
        WDEBUG << "logical error: hdex_id not found " << hdex_id << std::endl; \
        return false; \
    } \
    int64_t &idx = delayed_opid_to_idx[hdex_id]; \
    if (idx < 0) { \
        WDEBUG << "logical error: negative idx " << idx << std::endl; \
        return false; \
    }

bool
hyper_stub_base :: call_no_loop(hyper_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_attribute *cl_attr, size_t num_attrs,
    int64_t &op_id)
{
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    HYPERDEX_CALL_NOTX(h, space, key, key_sz, cl_attr, num_attrs, delayed_call_status[delayed_idx]);
    DELAYED_INSERT_HDEX_ID;
    delayed_idx++;

    op_id = hdex_id;
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

bool
hyper_stub_base :: map_call_no_loop(hyper_map_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_map_attribute *map_attr, size_t num_attrs,
    int64_t &op_id)
{
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;
    
    HYPERDEX_CALL_NOTX(h, space, key, key_sz, map_attr, num_attrs, delayed_call_status[delayed_idx]);
    DELAYED_INSERT_HDEX_ID;
    delayed_idx++;

    op_id = hdex_id;
    return success;
}

bool
hyper_stub_base :: loop(int64_t &op_id, hyperdex_client_returncode &code)
{
    bool success = true;
    int64_t hdex_id;
    hyperdex_client_returncode loop_status;
    uint64_t success_calls = 0;

    HYPERDEX_LOOP;
    DELAYED_CHECK_HDEX_ID;
    HYPERDEX_CHECK_STATUSES(delayed_call_status[idx], delayed_call_status[idx] != HYPERDEX_CLIENT_SUCCESS);

    code = delayed_call_status[idx];
    delayed_opid_to_idx.erase(hdex_id);
    delayed_call_status.erase(idx);

    op_id = hdex_id;
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

#define INSERT_HDEX_ID \
    if (opid_to_idx.find(hdex_id) != opid_to_idx.end()) { \
        WDEBUG << "logical error: repeated hdex_id " << hdex_id << std::endl; \
        return false; \
    } \
    opid_to_idx[hdex_id] = i;

#define CHECK_HDEX_ID \
    if (opid_to_idx.find(hdex_id) == opid_to_idx.end()) { \
        WDEBUG << "logical error: hdex_id not found " << hdex_id << std::endl; \
        return false; \
    } \
    int64_t &idx = opid_to_idx[hdex_id]; \
    if (idx < 0) { \
        WDEBUG << "logical error: negative idx " << idx << std::endl; \
        return false; \
    }

bool
hyper_stub_base :: multiple_call(std::vector<hyper_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs)
{
    uint64_t num_calls = funcs.size();
    if (num_calls != spaces.size()
     || num_calls != keys.size()
     || num_calls != key_szs.size()
     || num_calls != num_attrs.size()
     || num_calls != attrs.size()) {
        WDEBUG << "logical error: size of multiple_call vectors not equal" << std::endl;
        return false;
    }

    hyperdex_client_returncode call_status[num_calls];
    int hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;

    for (uint64_t i = 0; i < num_calls; i++) {
        HYPERDEX_CALL_NOTX(funcs[i], spaces[i], keys[i], key_szs[i], attrs[i], num_attrs[i], call_status[i]);

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (uint64_t i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        CHECK_HDEX_ID;

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
    if (num_calls != spaces.size()
     || num_calls != keys.size()
     || num_calls != key_szs.size()
     || num_calls != num_attrs.size()
     || num_calls != attrs.size()) {
        WDEBUG << "logical error: size of multiple_call vectors not equal" << std::endl;
        return false;
    }

    uint64_t map_num_calls = map_funcs.size();
    if (map_num_calls != map_spaces.size()
     || map_num_calls != map_keys.size()
     || map_num_calls != map_key_szs.size()
     || map_num_calls != map_num_attrs.size()
     || map_num_calls != map_attrs.size()) {
        WDEBUG << "logical error: size of multiple_call vectors not equal" << std::endl;
        return false;
    }

    hyperdex_client_returncode call_status[num_calls+map_num_calls];
    int hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;

    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_CALL(funcs[i], spaces[i], keys[i], key_szs[i], attrs[i], num_attrs[i], call_status[i]);

        INSERT_HDEX_ID;
    }

    for (uint64_t j = 0; j < map_num_calls; j++, i++) {
        HYPERDEX_CALL(map_funcs[j], map_spaces[j], map_keys[j], map_key_szs[j], map_attrs[j], map_num_attrs[j], call_status[i]);

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        CHECK_HDEX_ID;

        HYPERDEX_CHECK_STATUSES(call_status[idx], call_status[idx] != HYPERDEX_CLIENT_SUCCESS);

        idx = -1;
    }

    return success;
}

bool
hyper_stub_base :: multiple_call_no_loop(std::vector<hyper_func> &funcs,
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
    return multiple_call_no_loop(funcs,
        spaces,
        keys, key_szs,
        attrs, num_attrs,
        map_funcs,
        map_spaces,
        map_keys, map_key_szs,
        map_attrs, map_num_attrs);
}

bool
hyper_stub_base :: multiple_call_no_loop(std::vector<hyper_func> &funcs,
    std::vector<const char*> &spaces,
    std::vector<const char*> &keys, std::vector<size_t> &key_szs,
    std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs,
    std::vector<hyper_map_func> &map_funcs,
    std::vector<const char*> &map_spaces,
    std::vector<const char*> &map_keys, std::vector<size_t> &map_key_szs,
    std::vector<hyperdex_client_map_attribute*> &map_attrs, std::vector<size_t> &map_num_attrs)
{
    uint64_t num_calls = funcs.size();
    if (num_calls != spaces.size()
     || num_calls != keys.size()
     || num_calls != key_szs.size()
     || num_calls != num_attrs.size()
     || num_calls != attrs.size()) {
        WDEBUG << "logical error: size of multiple_call vectors not equal" << std::endl;
        return false;
    }

    uint64_t map_num_calls = map_funcs.size();
    if (map_num_calls != map_spaces.size()
     || map_num_calls != map_keys.size()
     || map_num_calls != map_key_szs.size()
     || map_num_calls != map_num_attrs.size()
     || map_num_calls != map_attrs.size()) {
        WDEBUG << "logical error: size of multiple_call vectors not equal" << std::endl;
        return false;
    }

    int hdex_id;
    uint64_t success_calls = 0;
    bool success = true;

    for (uint64_t i = 0; i < num_calls; i++) {
        HYPERDEX_CALL_NOTX(funcs[i], spaces[i], keys[i], key_szs[i], attrs[i], num_attrs[i], delayed_call_status[delayed_idx]);

        DELAYED_INSERT_HDEX_ID;
        delayed_idx++;
    }

    for (uint64_t j = 0; j < map_num_calls; j++) {
        HYPERDEX_CALL_NOTX(map_funcs[j], map_spaces[j], map_keys[j], map_key_szs[j], map_attrs[j], map_num_attrs[j], delayed_call_status[delayed_idx]);

        DELAYED_INSERT_HDEX_ID;
        delayed_idx++;
    }

    return success;
}

bool
hyper_stub_base :: multiple_loop(uint64_t num_loops)
{
    bool success = true;
    int hdex_id;
    hyperdex_client_returncode loop_status;
    uint64_t success_calls = 0;
    for (uint64_t i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        DELAYED_CHECK_HDEX_ID;

        HYPERDEX_CHECK_STATUSES(delayed_call_status[idx], delayed_call_status[idx] != HYPERDEX_CLIENT_SUCCESS);

        delayed_opid_to_idx.erase(hdex_id);
        delayed_call_status.erase(idx);
    }

    return success;
}

#undef DELAYED_INSERT_HDEX_ID
#undef DELAYED_CHECK_HDEX_ID

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
    if (num_calls != keys.size()
     || num_calls != key_szs.size()
     || num_calls != num_attrs.size()
     || num_calls != cl_attrs.size()) {
        WDEBUG << "logical error: size of multiple_get vectors not equal" << std::endl;
        return false;
    }

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

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        CHECK_HDEX_ID;

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
    if (num_calls != keys.size()
     || num_calls != key_szs.size()
     || num_calls != num_attrs.size()
     || num_calls != cl_attrs.size()) {
        WDEBUG << "logical error: size of multiple_get_partial vectors not equal" << std::endl;
        return false;
    }

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

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        CHECK_HDEX_ID;

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
    if (num_calls != keys.size()
     || num_calls != key_szs.size()) {
        WDEBUG << "logical error: size of multiple_del vectors not equal" << std::endl;
        return false;
    }

    hyperdex_client_returncode del_status[num_calls];
    int64_t hdex_id;
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_calls);
    uint64_t success_calls = 0;
    bool success = true;
    
    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_DEL(spaces[i], keys[i], key_szs[i], del_status[i]);

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP;

        CHECK_HDEX_ID;

        HYPERDEX_CHECK_STATUSES(del_status[idx], del_status[idx] != HYPERDEX_CLIENT_SUCCESS && del_status[idx] != HYPERDEX_CLIENT_NOTFOUND);

        idx = -1;
    }

    return success;
}

#undef INSERT_HDEX_ID
#undef CHECK_HDEX_ID

#undef HYPERDEX_CHECK_ID
#undef HYPERDEX_CALL
#undef HYPERDEX_CALL_NOTX
#undef HYPERDEX_GET
#undef HYPERDEX_GET_NOTX
#undef HYPERDEX_DEL
#undef HYPERDEX_LOOP
#undef HYPERDEX_CHECK_STATUSES

bool
hyper_stub_base :: recreate_node(const hyperdex_client_attribute *cl_attr, db::node &n)
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
        if (idx[i] == -1 || check_idx[idx[i]]) {
            WDEBUG << "logical error: recreate node " << idx[i] << " " << check_idx[idx[i]] << std::endl;
            return false;
        }
        check_idx[idx[i]] = true;
    }

    // shard
    n.shard = *((uint64_t*)cl_attr[idx[0]].value);
    // create clock
    vc::vclock_ptr_t create_clk;
    unpack_buffer(cl_attr[idx[1]].value, cl_attr[idx[1]].value_sz, create_clk);
    // properties
#ifdef weaver_large_property_maps_
    unpack_buffer<std::vector<std::shared_ptr<db::property>>>(cl_attr[idx[2]].value, cl_attr[idx[2]].value_sz, n.base.properties);
#else
    unpack_buffer(cl_attr[idx[2]].value, cl_attr[idx[2]].value_sz, n.base.properties);
#endif

    n.state = db::node::mode::STABLE;
    n.in_use = false;
    n.base.update_creat_time(create_clk);

    // out edges
    unpack_buffer<std::vector<db::edge*>>(cl_attr[idx[3]].value, cl_attr[idx[3]].value_sz, n.out_edges);
    // last update clock
    unpack_buffer(cl_attr[idx[5]].value, cl_attr[idx[5]].value_sz, n.last_upd_clk);
    // restore clock
    unpack_buffer(cl_attr[idx[6]].value, cl_attr[idx[6]].value_sz, n.restore_clk);
    if (n.restore_clk->size() != ClkSz) {
        WDEBUG << "unpack error, restore_clk->size=" << n.restore_clk->size() << std::endl;
        return false;
    }
    // aliases
    unpack_buffer(cl_attr[idx[7]].value, cl_attr[idx[7]].value_sz, n.aliases);

    return true;
}

bool
hyper_stub_base :: get_node(db::node &n)
{
    const hyperdex_client_attribute *attr;
    size_t num_attrs;
    const node_handle_t &handle = n.get_handle();

    bool success = get(graph_space, handle.c_str(), handle.size(), &attr, &num_attrs, true);
    if (success) {
        success = recreate_node(attr, n);
        hyperdex_client_destroy_attrs(attr, num_attrs);
    }

    return success;
}

bool
hyper_stub_base :: get_nodes(std::unordered_map<node_handle_t, db::node*> &nodes, bool tx)
{
    int64_t num_nodes = nodes.size();
    std::vector<const char*> spaces(num_nodes, graph_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<const hyperdex_client_attribute**> attrs(num_nodes, nullptr);
    std::vector<size_t*> num_attrs(num_nodes, nullptr);

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
                success = false;
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
    db::node &n,
    std::unique_ptr<e::buffer> &creat_clk_buf,
    std::unique_ptr<e::buffer> &props_buf,
    std::unique_ptr<e::buffer> &out_edges_buf,
    std::unique_ptr<e::buffer> &last_clk_buf,
    std::unique_ptr<e::buffer> &restore_clk_buf,
    std::unique_ptr<e::buffer> &aliases_buf,
    size_t &num_attrs)
{
    size_t attr_idx = 0;

    // shard
    cl_attr[attr_idx].attr = graph_attrs[0];
    cl_attr[attr_idx].value = (const char*)&n.shard;
    cl_attr[attr_idx].value_sz = sizeof(int64_t);
    cl_attr[attr_idx].datatype = graph_dtypes[0];
    attr_idx++;

    // create clock
    prepare_buffer(n.base.get_creat_time(), creat_clk_buf);
    cl_attr[attr_idx].attr = graph_attrs[1];
    cl_attr[attr_idx].value = (const char*)creat_clk_buf->data();
    cl_attr[attr_idx].value_sz = creat_clk_buf->size();
    cl_attr[attr_idx].datatype = graph_dtypes[1];
    attr_idx++;

    // properties
    if (!n.base.properties.empty()) {
#ifdef weaver_large_property_maps_
        prepare_buffer<std::vector<std::shared_ptr<db::property>>>(n.base.properties, props_buf);
#else
        prepare_buffer(n.base.properties, props_buf);
#endif
        cl_attr[attr_idx].attr = graph_attrs[2];
        cl_attr[attr_idx].value = (const char*)props_buf->data();
        cl_attr[attr_idx].value_sz = props_buf->size();
        cl_attr[attr_idx].datatype = graph_dtypes[2];
        attr_idx++;
    }

    // out edges
    if (!n.out_edges.empty()) {
        prepare_buffer<std::vector<db::edge*>>(n.out_edges, out_edges_buf);
        cl_attr[attr_idx].attr = graph_attrs[3];
        cl_attr[attr_idx].value = (const char*)out_edges_buf->data();
        cl_attr[attr_idx].value_sz = out_edges_buf->size();
        cl_attr[attr_idx].datatype = graph_dtypes[3];
        attr_idx++;
    }

    // migr status
    int64_t status = STABLE;
    cl_attr[attr_idx].attr = graph_attrs[4];
    cl_attr[attr_idx].value = (const char*)&status;
    cl_attr[attr_idx].value_sz = sizeof(int64_t);
    cl_attr[attr_idx].datatype = graph_dtypes[4];
    attr_idx++;

    // last update clock
    if (last_clk_buf == nullptr) {
        prepare_buffer(n.last_upd_clk, last_clk_buf);
    }
    cl_attr[attr_idx].attr = graph_attrs[5];
    cl_attr[attr_idx].value = (const char*)last_clk_buf->data();
    cl_attr[attr_idx].value_sz = last_clk_buf->size();
    cl_attr[attr_idx].datatype = graph_dtypes[5];
    attr_idx++;

    // restore clock
    if (restore_clk_buf == nullptr) {
        if (n.restore_clk->size() != ClkSz) {
            WDEBUG << "pack error, restore_clk->size=" << n.restore_clk->size() << std::endl;
        }
        prepare_buffer(n.restore_clk, restore_clk_buf);
    }
    cl_attr[attr_idx].attr = graph_attrs[6];
    cl_attr[attr_idx].value = (const char*)restore_clk_buf->data();
    cl_attr[attr_idx].value_sz = restore_clk_buf->size();
    cl_attr[attr_idx].datatype = graph_dtypes[6];
    attr_idx++;

    // aliases
    prepare_buffer(n.aliases, aliases_buf);
    cl_attr[attr_idx].attr = graph_attrs[7];
    cl_attr[attr_idx].value = (const char*)aliases_buf->data();
    cl_attr[attr_idx].value_sz = aliases_buf->size();
    cl_attr[attr_idx].datatype = graph_dtypes[7];
    attr_idx++;

    num_attrs = attr_idx;
    assert(num_attrs <= NUM_GRAPH_ATTRS);
}

void
hyper_stub_base :: prepare_edges(hyperdex_client_map_attribute *cl_attrs,
    std::vector<async_put_edge_unit> &edges)
{
    for (uint32_t i = 0; i < edges.size(); i++) {
        db::edge *e = edges[i].e;
        const edge_handle_t &edge_handle = edges[i].edge_handle;
        std::unique_ptr<e::buffer> &edge_buf = edges[i].edge_buf;
        // cl_attrs is an array of size edges.size()
        hyperdex_client_map_attribute *cl_attr = cl_attrs + i;

        std::vector<db::edge*> edge_vec(1, e);
        prepare_buffer<std::vector<db::edge*>>(edge_vec, edge_buf);

        cl_attr->attr = graph_attrs[3];
        cl_attr->map_key = edge_handle.c_str();
        cl_attr->map_key_sz = edge_handle.size();
        cl_attr->map_key_datatype = HYPERDATATYPE_STRING;
        cl_attr->value = (const char*)edge_buf->data();
        cl_attr->value_sz = edge_buf->size();
        cl_attr->value_datatype = HYPERDATATYPE_STRING;
    }
}

bool
hyper_stub_base :: put_nodes(std::unordered_map<node_handle_t, db::node*> &nodes, bool if_not_exist)
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
    std::vector<size_t> num_attrs(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> creat_clk_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> props_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> out_edges_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> last_clk_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> restore_clk_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> aliases_buf(num_nodes);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_nodes * NUM_GRAPH_ATTRS * sizeof(hyperdex_client_attribute));

    int i = 0;
    for (auto &p: nodes) {
        attrs[i] = attrs_to_add + NUM_GRAPH_ATTRS*i;
        keys[i] = p.first.c_str();
        key_szs[i] = p.first.size();

        prepare_node(attrs[i], *p.second, creat_clk_buf[i], props_buf[i], out_edges_buf[i], last_clk_buf[i], restore_clk_buf[i], aliases_buf[i], num_attrs[i]);

        i++;
    }

    bool success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);

    free(attrs_to_add);

    return success;
}

bool
hyper_stub_base :: put_nodes_bulk(std::unordered_map<node_handle_t, db::node*> &nodes,
    std::shared_ptr<vc::vclock> last_upd_clk,
    std::shared_ptr<vc::vclock_t> restore_clk)
{
    int num_nodes = nodes.size();
    std::vector<hyper_func> funcs(num_nodes, &hyperdex_client_put);
    std::vector<const char*> spaces(num_nodes, graph_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<hyperdex_client_attribute*> attrs(num_nodes);
    std::vector<size_t> num_attrs(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> creat_clk_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> props_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> out_edges_buf(num_nodes);
    std::vector<std::unique_ptr<e::buffer>> aliases_buf(num_nodes);

    std::unique_ptr<e::buffer> restore_clk_buf, last_clk_buf;
    prepare_buffer(last_upd_clk, last_clk_buf);
    prepare_buffer(restore_clk, restore_clk_buf);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_nodes * NUM_GRAPH_ATTRS * sizeof(hyperdex_client_attribute));

    int i = 0;
    for (auto &p: nodes) {
        attrs[i] = attrs_to_add + NUM_GRAPH_ATTRS*i;
        keys[i] = p.first.c_str();
        key_szs[i] = p.first.size();

        prepare_node(attrs[i], *p.second, creat_clk_buf[i], props_buf[i], out_edges_buf[i], last_clk_buf, restore_clk_buf, aliases_buf[i], num_attrs[i]);

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
        if (num_attrs != 1 || attr->value_sz != sizeof(int64_t)) {
            WDEBUG << "logical error: num_attrs=" << num_attrs << ", attr->value_sz=" << attr->value_sz << std::endl;
            hyperdex_client_destroy_attrs(attr, num_attrs);
            return UINT64_MAX;
        }
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
    std::vector<const hyperdex_client_attribute**> attrs(num_nodes, nullptr);
    std::vector<size_t*> num_attrs(num_nodes, nullptr);

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
hyper_stub_base :: add_indices(std::unordered_map<std::string, db::node*> &indices, bool tx, bool if_not_exist)
{
    if (!AuxIndex) {
        WDEBUG << "logical error: aux index" << std::endl;
        return false;
    }

    uint64_t num_indices = indices.size();
    std::vector<const char*> spaces(num_indices, index_space);
    std::vector<const char*> keys(num_indices);
    std::vector<size_t> key_szs(num_indices);
    std::vector<hyperdex_client_attribute*> attrs(num_indices);
    std::vector<size_t> num_attrs(num_indices, NUM_INDEX_ATTRS);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_indices * NUM_INDEX_ATTRS * sizeof(hyperdex_client_attribute));

    uint64_t i = 0;
    for (auto &p: indices) {
        attrs[i] = attrs_to_add + NUM_INDEX_ATTRS*i;
        keys[i] = p.first.c_str();
        key_szs[i] = p.first.size();

        // node handle
        const node_handle_t &handle = p.second->get_handle();
        attrs[i][0].attr = index_attrs[0];
        attrs[i][0].value = handle.c_str();
        attrs[i][0].value_sz = handle.size();
        attrs[i][0].datatype = index_dtypes[0];

        // shard
        attrs[i][1].attr = index_attrs[1];
        attrs[i][1].value = (const char*)&p.second->shard;
        attrs[i][1].value_sz = sizeof(int64_t);
        attrs[i][1].datatype = index_dtypes[1];

        i++;
    }

    bool success;
    if (tx) {
        std::vector<hyper_tx_func> funcs;
        if (if_not_exist) {
            funcs = std::vector<hyper_tx_func>(num_indices, &hyperdex_client_xact_put_if_not_exist);
        } else {
            funcs = std::vector<hyper_tx_func>(num_indices, &hyperdex_client_xact_put);
        }
        success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);
    } else {
        std::vector<hyper_func> funcs;
        if (if_not_exist) {
            funcs = std::vector<hyper_func>(num_indices, &hyperdex_client_put_if_not_exist);
        } else {
            funcs = std::vector<hyper_func>(num_indices, &hyperdex_client_put);
        }
        success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);
    }

    free(attrs_to_add);

    return success;
}

bool
hyper_stub_base :: recreate_index(const hyperdex_client_attribute *cl_attr, std::pair<node_handle_t, uint64_t> &val)
{
    std::vector<int> idx(NUM_INDEX_ATTRS, -1);    
    for (int i = 0; i < NUM_INDEX_ATTRS; i++) {
        for (int j = 0; j < NUM_INDEX_ATTRS; j++) {
            if (strcmp(cl_attr[i].attr, index_attrs[j]) == 0) {
                idx[j] = i;
                break;
            }
        }
    }
    std::vector<bool> check_idx(NUM_INDEX_ATTRS, false);
    for (int i = 0; i < NUM_INDEX_ATTRS; i++) {
        if (idx[i] == -1 || check_idx[idx[i]]) {
            WDEBUG << "logical error: recreate index " << idx[i] << " " << check_idx[idx[i]] << std::endl;
            return false;
        }
        check_idx[idx[i]] = true;
    }

    // node handle
    val.first = node_handle_t(cl_attr[idx[0]].value, cl_attr[idx[0]].value_sz);
    val.second = *((uint64_t*)cl_attr[idx[1]].value);

    return true;
}

bool
hyper_stub_base :: get_indices(std::unordered_map<std::string, std::pair<node_handle_t, uint64_t>> &indices, bool tx)
{
    if (!AuxIndex) {
        WDEBUG << "logical error: aux index" << std::endl;
        return false;
    }

    int64_t num_indices = indices.size();
    std::vector<const char*> spaces(num_indices, index_space);
    std::vector<const char*> keys(num_indices);
    std::vector<size_t> key_szs(num_indices);
    std::vector<const hyperdex_client_attribute**> attrs(num_indices, nullptr);
    std::vector<size_t*> num_attrs(num_indices, nullptr);

    const hyperdex_client_attribute *attr_array[num_indices];
    size_t num_attrs_array[num_indices];

    int64_t i = 0;
    for (auto &x: indices) {
        keys[i] = x.first.c_str();
        key_szs[i] = x.first.size();
        attrs[i] = attr_array + i;
        num_attrs[i] = num_attrs_array + i;

        i++;
    }

    bool success = multiple_get(spaces, keys, key_szs, attrs, num_attrs, tx);

    if (success) {
        for (i = 0; i < num_indices; i++) {
            if (*num_attrs[i] == NUM_INDEX_ATTRS) {
                bool recr_success = recreate_index(attr_array[i], indices[std::string(keys[i])]);
                hyperdex_client_destroy_attrs(attr_array[i], NUM_INDEX_ATTRS);
                success = success && recr_success;
            } else {
                WDEBUG << "bad num attributes " << *num_attrs[i] << std::endl;
                success = false;
                if (*num_attrs[i] > 0) {
                    hyperdex_client_destroy_attrs(attr_array[i], *num_attrs[i]);
                }
            }
        }
    }

    return success;
}

bool
hyper_stub_base :: del_indices(std::vector<std::string> &indices)
{
    if (!AuxIndex) {
        WDEBUG << "logical error: aux index" << std::endl;
        return false;
    }

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

inline void
hyper_stub_base :: sort_and_pack_as_set(std::vector<std::string> &vec, std::unique_ptr<e::buffer> &buf)
{
    std::sort(vec.begin(), vec.end());

    uint64_t buf_sz = 0;
    // now iterate in vec order
    for (uint64_t i = 0; i < vec.size(); i++) {
        buf_sz += sizeof(uint32_t) // each entry encoding sz
                + vec[i].size();
    }

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    for (uint64_t i = 0; i < vec.size(); i++) {
        pack_uint32(packer, vec[i].size());
        pack_string(packer, vec[i]);
    }
}

// store the given unordered_set as a HYPERDATATYPE_SET_STRING
inline void
hyper_stub_base :: prepare_buffer(const std::unordered_set<std::string> &set, std::unique_ptr<e::buffer> &buf)
{
    std::vector<std::string> sorted;
    sorted.reserve(set.size());

    for (const std::string &s: set) {
        sorted.emplace_back(s);
    }

    sort_and_pack_as_set(sorted, buf);
}

inline void
hyper_stub_base :: prepare_buffer(const db::string_set &set,
    std::unique_ptr<e::buffer> &buf)
{
    std::vector<std::string> sorted;
    sorted.reserve(set.size());

    for (const std::string &s: set) {
        sorted.emplace_back(s);
    }

    sort_and_pack_as_set(sorted, buf);
}

#define UNPACK_SET \
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz)); \
    e::unpacker unpacker = ebuf->unpack_from(0); \
    std::string key; \
    uint32_t sz; \
    while (!unpacker.empty()) { \
        key.erase(); \
        unpack_uint32(unpacker, sz); \
        unpack_string(unpacker, key, sz); \
        set.insert(key); \
    }

// unpack the HYPERDATATYPE_SET_STRING in to the given set
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<std::string> &set)
{
    UNPACK_SET;
}

inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz,
    db::string_set &set)
{
    UNPACK_SET;
}

#undef UNPACK_SET

// pack as HYPERDATATYPE_LIST_STRING
inline void
hyper_stub_base :: prepare_buffer(const std::vector<std::shared_ptr<db::property>> &props, std::unique_ptr<e::buffer> &buf)
{
    std::vector<uint32_t> sz;
    sz.reserve(props.size());
    uint32_t buf_sz = 0;

    for (const std::shared_ptr<db::property> p: props) {
        sz.emplace_back(message::size(p));
        buf_sz += sizeof(uint32_t)
                + sz.back();
    }

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    for (uint64_t i = 0; i < props.size(); i++) {
        pack_uint32(packer, sz[i]);
        message::pack_buffer(packer, props[i]);
    }
}

// unpack from HYPERDATATYPE_LIST_STRING
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::vector<std::shared_ptr<db::property>> &props)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    uint32_t sz;

    while (!unpacker.empty()) {
        unpack_uint32(unpacker, sz);
        std::shared_ptr<db::property> p;
        message::unpack_buffer(unpacker, p);

        props.emplace_back(std::move(p));
    }
}
