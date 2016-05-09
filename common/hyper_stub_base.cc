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
    : node_attrs{"shard",
                 "creat_time",
                 "properties",
                 "out_edges",
                 "max_edge_id",
                 "last_upd_clk",
                 "restore_clk",
                 "aliases"}
    , node_dtypes{HYPERDATATYPE_INT64,
                  HYPERDATATYPE_STRING,
#ifdef weaver_large_property_maps_
                  HYPERDATATYPE_MAP_STRING_STRING,
#else
                  HYPERDATATYPE_LIST_STRING,
#endif
                  HYPERDATATYPE_SET_INT64,
                  HYPERDATATYPE_INT64,
                  HYPERDATATYPE_STRING,
                  HYPERDATATYPE_STRING,
                  HYPERDATATYPE_SET_STRING}
    , edge_attrs{"node_handle",
                 "shard",
                 "edge_id",
                 "data"}
    , edge_dtypes{HYPERDATATYPE_STRING,
                  HYPERDATATYPE_INT64,
                  HYPERDATATYPE_INT64,
                  HYPERDATATYPE_STRING}
    , edge_id_attrs{"handle"}
    , edge_id_dtypes{HYPERDATATYPE_STRING}
    , tx_attrs{"vt_id", "tx_data"}
    , tx_dtypes{HYPERDATATYPE_INT64, HYPERDATATYPE_STRING}
    , m_cl(hyperdex_client_create(HyperdexCoordIpaddr.c_str(), HyperdexCoordPort))
    , m_gen_seed(weaver_util::urandom_uint64())
    , m_mt64_gen(m_gen_seed)
    , m_uint64max_dist()
{
    assert(m_gen_seed != 0);
    assert(m_uint64max_dist.max() == UINT64_MAX);
}


#define HYPERDEX_CALL_WRAPPER(hyper_call, op_id, status, success, success_calls) \
    do { \
        op_id = hyper_call; \
    } while (op_id < 0 && (status == HYPERDEX_CLIENT_INTERRUPTED)); \
    check_op_id(op_id, status, success, success_calls);

#define HYPERDEX_CALL5(f, arg1, arg2, arg3, arg4, arg5, op_id, status, success, success_calls) \
    HYPERDEX_CALL_WRAPPER(f(arg1, arg2, arg3, arg4, arg5), op_id, status, success, success_calls);

#define HYPERDEX_CALL7(f, arg1, arg2, arg3, arg4, arg5, arg6, arg7, op_id, status, success, success_calls) \
    HYPERDEX_CALL_WRAPPER(f(arg1, arg2, arg3, arg4, arg5, arg6, arg7), op_id, status, success, success_calls);

#define HYPERDEX_CALL9(f, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, op_id, status, success, success_calls) \
    HYPERDEX_CALL_WRAPPER(f(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9), op_id, status, success, success_calls);

#define HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls) \
    HYPERDEX_CALL_WRAPPER(hyperdex_client_loop(m_cl, -1, &loop_status), hdex_id, loop_status, success, success_calls);

#define HYPERDEX_LOOP_TIMEOUT(loop_status, hdex_id, success, success_calls) \
    HYPERDEX_CALL_WRAPPER(hyperdex_client_loop(m_cl, 100, &loop_status), hdex_id, loop_status, success, success_calls);


#ifdef weaver_benchmark_ // assert false on error

#define HYPERDEX_CHECK_STATUSES(status, fail_check, debug_key) \
    if ((loop_status != HYPERDEX_CLIENT_SUCCESS) || (fail_check)) { \
        WDEBUG << "hyperdex error" \
               << ", call status: " << hyperdex_client_returncode_to_string(status) \
               << ", loop status: " << hyperdex_client_returncode_to_string(loop_status) << std::endl; \
        if (debug_key != nullptr) { \
            WDEBUG << "key=" << (const char*)debug_key << std::endl; \
        } \
        WDEBUG << "error message: " << hyperdex_client_error_message(m_cl) << std::endl; \
        WDEBUG << "error loc: " << hyperdex_client_error_location(m_cl) << std::endl; \
        assert(false); \
        success = false; \
    }

#else

#define HYPERDEX_CHECK_STATUSES(status, fail_check, debug_key) \
    if ((loop_status != HYPERDEX_CLIENT_SUCCESS && loop_status != HYPERDEX_CLIENT_TIMEOUT) || (fail_check)) { \
        /*WDEBUG << "hyperdex error" \
               << ", call status: " << hyperdex_client_returncode_to_string(status) \
               << ", loop status: " << hyperdex_client_returncode_to_string(loop_status) << std::endl;*/ \
        if (debug_key != nullptr) { \
            WDEBUG << "key=" << (const char*)debug_key << std::endl; \
        } \
        /*WDEBUG << "error message: " << hyperdex_client_error_message(m_cl) << std::endl; \
        WDEBUG << "error loc: " << hyperdex_client_error_location(m_cl) << std::endl;*/ \
        success = false; \
    }

#endif


void
hyper_stub_base :: check_op_id(int64_t op_id,
                               hyperdex_client_returncode status,
                               bool &success,
                               int &success_calls)
{
    if (op_id < 0 && status != HYPERDEX_CLIENT_TIMEOUT && status != HYPERDEX_CLIENT_NONEPENDING) {
        WDEBUG << "Hyperdex function failed, op id = " << op_id
               << ", status = " << hyperdex_client_returncode_to_string(status) << std::endl;
        WDEBUG << "error message: " << hyperdex_client_error_message(m_cl) << std::endl;
        WDEBUG << "error loc: " << hyperdex_client_error_location(m_cl) << std::endl;
#ifdef weaver_benchmark_
        assert(false);
#endif
        success = false;
    } else {
        success_calls++;
    }
}

void
hyper_stub_base :: begin_tx()
{
    m_hyper_tx = hyperdex_client_begin_transaction(m_cl);
}

void
hyper_stub_base :: commit_tx(hyperdex_client_returncode &commit_status)
{
    bool success = true;
    int success_calls = 0;
    hyperdex_client_returncode loop_status;

    int64_t hdex_id = hyperdex_client_commit_transaction(m_hyper_tx, &commit_status);
    check_op_id(hdex_id, commit_status, success, success_calls);

    if (success) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);
        HYPERDEX_CHECK_STATUSES(commit_status,
                                commit_status != HYPERDEX_CLIENT_ABORTED && commit_status != HYPERDEX_CLIENT_SUCCESS,
                                nullptr);
    }
}

void
hyper_stub_base :: abort_tx()
{
    hyperdex_client_abort_transaction(m_hyper_tx);
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

    HYPERDEX_CALL7(h, m_cl,
                   space, key, key_sz,
                   cl_attr, num_attrs,
                   &call_status, hdex_id,
                   call_status, success, success_calls);

    if (success_calls == 1) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);
        HYPERDEX_CHECK_STATUSES(call_status, call_status != HYPERDEX_CLIENT_SUCCESS, key);
    }

    return success;
}

// call hyperdex function h using params and then loop for response
bool
hyper_stub_base :: call(hyper_tx_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_attribute *cl_attr, size_t num_attrs,
    hyperdex_client_returncode *ret_call_status)
{
    hyperdex_client_returncode call_status, loop_status;
    int64_t hdex_id;
    int success_calls = 0;
    bool success = true;

    HYPERDEX_CALL7(h, m_hyper_tx,
                   space, key, key_sz,
                   cl_attr, num_attrs,
                   &call_status, hdex_id,
                   call_status, success, success_calls);

    if (success_calls == 1) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);
        HYPERDEX_CHECK_STATUSES(call_status, call_status != HYPERDEX_CLIENT_SUCCESS, key);
    }

    if (ret_call_status != nullptr) {
        *ret_call_status = call_status;
    }

    return success;
}

bool
hyper_stub_base :: call_no_loop(hyper_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_attribute *cl_attr, size_t num_attrs,
    int64_t &op_id, hyperdex_client_returncode &status)
{
    int success_calls = 0;
    bool success = true;

    HYPERDEX_CALL7(h, m_cl,
                   space, key, key_sz,
                   cl_attr, num_attrs,
                   &status,
                   op_id, status, success, success_calls);

    return success;
}

bool
hyper_stub_base :: call_no_loop(hyper_tx_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_attribute *cl_attr, size_t num_attrs,
    int64_t &op_id, hyperdex_client_returncode &status)
{
    int success_calls = 0;
    bool success = true;

    HYPERDEX_CALL7(h, m_hyper_tx,
                   space, key, key_sz,
                   cl_attr, num_attrs,
                   &status,
                   op_id, status, success, success_calls);

    return success;
}

bool
hyper_stub_base :: get_no_loop(const char* space,
                               const char *key, size_t key_sz,
                               const hyperdex_client_attribute **cl_attr, size_t *num_attrs,
                               int64_t &op_id, hyperdex_client_returncode &status,
                               bool tx)
{
    int success_calls = 0;
    bool success = true;

    if (tx) {
        HYPERDEX_CALL7(hyperdex_client_xact_get, m_hyper_tx,
                       space, key, key_sz,
                       &status,
                       cl_attr, num_attrs,
                       op_id, status, success, success_calls);
    } else {
        HYPERDEX_CALL7(hyperdex_client_get, m_cl,
                       space, key, key_sz,
                       &status,
                       cl_attr, num_attrs,
                       op_id, status, success, success_calls);
    }

    return success;
}

bool
hyper_stub_base :: del_no_loop(const char* space,
                               const char *key, size_t key_sz,
                               int64_t &op_id,
                               hyperdex_client_returncode &status)
{
    int success_calls = 0;
    bool success = true;

    HYPERDEX_CALL5(hyperdex_client_xact_del, m_hyper_tx,
                   space, key, key_sz,
                   &status,
                   op_id, status, success, success_calls);

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
    
    HYPERDEX_CALL7(h, m_hyper_tx,
                   space, key, key_sz,
                   map_attr, num_attrs,
                   &call_status,
                   hdex_id, call_status, success, success_calls);

    if (success_calls == 1) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);
        HYPERDEX_CHECK_STATUSES(call_status, call_status != HYPERDEX_CLIENT_SUCCESS, key);
    }

    return success;
}

bool
hyper_stub_base :: map_call_no_loop(hyper_map_func h,
    const char *space,
    const char *key, size_t key_sz,
    hyperdex_client_map_attribute *map_attr, size_t num_attrs,
    int64_t &op_id, hyperdex_client_returncode &status)
{
    int success_calls = 0;
    bool success = true;
    
    HYPERDEX_CALL7(h, m_cl,
                   space, key, key_sz,
                   map_attr, num_attrs,
                   &status,
                   op_id, status, success, success_calls);

    return success;
}

bool
hyper_stub_base :: loop(bool timeout, int64_t &op_id, hyperdex_client_returncode &loop_status)
{
    int success_calls = 0;
    bool success = true;

    if (timeout) {
        HYPERDEX_LOOP_TIMEOUT(loop_status, op_id, success, success_calls);
    } else {
        HYPERDEX_LOOP(loop_status, op_id, success, success_calls);
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
    int success_calls = 0;
    bool success = true;

    for (uint64_t i = 0; i < num_calls; i++) {
        HYPERDEX_CALL7(funcs[i], m_cl,
                       spaces[i], keys[i], key_szs[i],
                       attrs[i], num_attrs[i],
                       &call_status[i],
                       hdex_id, call_status[i], success, success_calls);

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (uint64_t i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);

        CHECK_HDEX_ID;

        HYPERDEX_CHECK_STATUSES(call_status[idx], call_status[idx] != HYPERDEX_CLIENT_SUCCESS, keys[idx]);

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
    int success_calls = 0;
    bool success = true;

    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_CALL7(funcs[i], m_hyper_tx,
                       spaces[i], keys[i], key_szs[i],
                       attrs[i], num_attrs[i],
                       &call_status[i],
                       hdex_id, call_status[i], success, success_calls);

        INSERT_HDEX_ID;
    }

    for (uint64_t j = 0; j < map_num_calls; j++, i++) {
        HYPERDEX_CALL7(map_funcs[j], m_hyper_tx,
                       map_spaces[j], map_keys[j], map_key_szs[j],
                       map_attrs[j], map_num_attrs[j],
                       &call_status[i],
                       hdex_id, call_status[i], success, success_calls);

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);

        CHECK_HDEX_ID;

        const char *debug_key = idx < (int64_t)keys.size()? keys[idx] : map_keys[idx-keys.size()];
        HYPERDEX_CHECK_STATUSES(call_status[idx], call_status[idx] != HYPERDEX_CLIENT_SUCCESS, debug_key);

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
        HYPERDEX_CALL7(hyperdex_client_xact_get, m_hyper_tx,
                       space, key, key_sz,
                       &get_status,
                       cl_attr, num_attrs,
                       hdex_id, get_status, success, success_calls);
    } else {
        HYPERDEX_CALL7(hyperdex_client_get, m_cl,
                       space, key, key_sz,
                       &get_status,
                       cl_attr, num_attrs,
                       hdex_id, get_status, success, success_calls);
    }

    if (success_calls == 1) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);
        HYPERDEX_CHECK_STATUSES(get_status, get_status != HYPERDEX_CLIENT_SUCCESS, key);
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
        HYPERDEX_CALL9(hyperdex_client_xact_get_partial, m_hyper_tx,
                       space, key, key_sz,
                       attrnames, attrnames_sz,
                       &get_status,
                       cl_attr, num_attrs,
                       hdex_id, get_status, success, success_calls);
    } else {
        HYPERDEX_CALL9(hyperdex_client_get_partial, m_cl,
                       space, key, key_sz,
                       attrnames, attrnames_sz,
                       &get_status,
                       cl_attr, num_attrs,
                       hdex_id, get_status, success, success_calls);
    }

    if (success_calls == 1) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);
        HYPERDEX_CHECK_STATUSES(get_status, get_status != HYPERDEX_CLIENT_SUCCESS, key);
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
    int success_calls = 0;
    bool success = true;

    uint64_t i = 0;
    for (; i < num_calls; i++) {
        if (tx) {
            HYPERDEX_CALL7(hyperdex_client_xact_get, m_hyper_tx,
                           spaces[i], keys[i], key_szs[i],
                           &get_status[i],
                           cl_attrs[i], num_attrs[i],
                           hdex_id, get_status[i], success, success_calls);
        } else {
            HYPERDEX_CALL7(hyperdex_client_get, m_cl,
                           spaces[i], keys[i], key_szs[i],
                           &get_status[i],
                           cl_attrs[i], num_attrs[i],
                           hdex_id, get_status[i], success, success_calls);
        }

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);

        CHECK_HDEX_ID;

        HYPERDEX_CHECK_STATUSES(get_status[idx], get_status[idx] != HYPERDEX_CLIENT_SUCCESS, keys[idx]);

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
    int success_calls = 0;
    bool success = true;

    uint64_t i = 0;
    for (; i < num_calls; i++) {
        if (tx) {
            HYPERDEX_CALL9(hyperdex_client_xact_get_partial, m_hyper_tx,
                           spaces[i], keys[i], key_szs[i],
                           attrnames, attrnames_sz,
                           &get_status[i],
                           cl_attrs[i], num_attrs[i],
                           hdex_id, get_status[i], success, success_calls);
        } else {
            HYPERDEX_CALL9(hyperdex_client_get_partial, m_cl,
                           spaces[i], keys[i], key_szs[i],
                           attrnames, attrnames_sz,
                           &get_status[i],
                           cl_attrs[i], num_attrs[i],
                           hdex_id, get_status[i], success, success_calls);
        }

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);

        CHECK_HDEX_ID;

        HYPERDEX_CHECK_STATUSES(get_status[idx], get_status[idx] != HYPERDEX_CLIENT_SUCCESS, keys[idx]);

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

    HYPERDEX_CALL5(hyperdex_client_xact_del, m_hyper_tx,
                   space, key, key_sz,
                   &del_status,
                   hdex_id, del_status, success, success_calls);

    if (success_calls == 1) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);
        HYPERDEX_CHECK_STATUSES(del_status, del_status != HYPERDEX_CLIENT_SUCCESS, key);
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
    int success_calls = 0;
    bool success = true;
    
    uint64_t i = 0;
    for (; i < num_calls; i++) {
        HYPERDEX_CALL5(hyperdex_client_xact_del, m_hyper_tx,
                       spaces[i], keys[i], key_szs[i],
                       &del_status[i],
                       hdex_id, del_status[i], success, success_calls);

        INSERT_HDEX_ID;
    }

    hyperdex_client_returncode loop_status;
    uint64_t num_loops = success_calls;
    success_calls = 0;
    for (i = 0; i < num_loops; i++) {
        HYPERDEX_LOOP(loop_status, hdex_id, success, success_calls);

        CHECK_HDEX_ID;

        HYPERDEX_CHECK_STATUSES(del_status[idx],
                                del_status[idx] != HYPERDEX_CLIENT_SUCCESS && del_status[idx] != HYPERDEX_CLIENT_NOTFOUND,
                                keys[idx]);

        idx = -1;
    }

    return success;
}

#undef INSERT_HDEX_ID
#undef CHECK_HDEX_ID

#undef HYPERDEX_CALL_WRAPPER
#undef HYPERDEX_CALL5
#undef HYPERDEX_CALL7
#undef HYPERDEX_CALL9
#undef HYPERDEX_LOOP
#undef HYPERDEX_LOOP_TIMEOUT
#undef HYPERDEX_CHECK_STATUSES

bool
hyper_stub_base :: recreate_node(const hyperdex_client_attribute *cl_attr,
                                 db::node &n)
{
    std::vector<int> idx(NUM_NODE_ATTRS, -1);    
    for (int i = 0; i < NUM_NODE_ATTRS; i++) {
        for (int j = 0; j < NUM_NODE_ATTRS; j++) {
            if (strcmp(cl_attr[i].attr, node_attrs[j]) == 0) {
                idx[j] = i;
                break;
            }
        }
    }
    std::vector<bool> check_idx(NUM_NODE_ATTRS, false);
    for (int i = 0; i < NUM_NODE_ATTRS; i++) {
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

    // edge ids
    unpack_buffer(cl_attr[idx[3]].value, cl_attr[idx[3]].value_sz, n.edge_ids);
    // max edge id
    n.max_edge_id = *((uint64_t*)cl_attr[idx[4]].value);
    // last update clock
    unpack_buffer(cl_attr[idx[5]].value, cl_attr[idx[5]].value_sz, n.last_upd_clk);
    // restore clock
    unpack_buffer(cl_attr[idx[6]].value, cl_attr[idx[6]].value_sz, n.restore_clk);
    //if (n.restore_clk->size() != ClkSz) {
    //    WDEBUG << "unpack error, restore_clk->size=" << n.restore_clk->size() << std::endl;
    //    return false;
    //}
    // aliases
    unpack_buffer(cl_attr[idx[7]].value, cl_attr[idx[7]].value_sz, n.aliases);

    return true;
}

bool
hyper_stub_base :: recreate_edge(const hyperdex_client_attribute *cl_attr,
                                 node_handle_t &handle,
                                 uint64_t &shard,
                                 uint64_t &edge_id,
                                 db::edge **e)
{
    std::vector<int> idx(NUM_EDGE_ATTRS, -1);    
    for (int i = 0; i < NUM_EDGE_ATTRS; i++) {
        for (int j = 0; j < NUM_EDGE_ATTRS; j++) {
            if (strcmp(cl_attr[i].attr, edge_attrs[j]) == 0) {
                idx[j] = i;
                break;
            }
        }
    }
    std::vector<bool> check_idx(NUM_EDGE_ATTRS, false);
    for (int i = 0; i < NUM_EDGE_ATTRS; i++) {
        if (idx[i] == -1 || check_idx[idx[i]]) {
            WDEBUG << "logical error: recreate edge " << idx[i] << " " << check_idx[idx[i]] << std::endl;
            return false;
        }
        check_idx[idx[i]] = true;
    }

    handle = std::string(cl_attr[idx[0]].value, cl_attr[idx[0]].value_sz);
    shard  = *((uint64_t*)cl_attr[idx[1]].value);
    if (cl_attr[idx[2]].value_sz == sizeof(int64_t)) {
        edge_id = *((uint64_t*)cl_attr[idx[2]].value);
    }
    if (cl_attr[idx[3]].value_sz > 0 && e != nullptr) {
        unpack_buffer(cl_attr[idx[3]].value, cl_attr[idx[3]].value_sz, *e);
    }

    return true;
}

bool
hyper_stub_base :: get_edge(const edge_handle_t &edge_handle,
                            db::edge **e,
                            aux_edge_data &aux_data,
                            bool tx)
{
    const hyperdex_client_attribute *attr;
    size_t num_attrs;

    bool success = get(edge_space,
                       edge_handle.c_str(), edge_handle.size(),
                       &attr, &num_attrs,
                       tx);
    if (success) {
        success = recreate_edge(attr,
                                aux_data.node_handle,
                                aux_data.shard, 
                                aux_data.edge_id,
                                e);
        hyperdex_client_destroy_attrs(attr, num_attrs);
    }

    return success;
}

bool
hyper_stub_base :: get_node_edges(db::node &n, bool tx)
{
    int64_t num_edges = n.edge_ids.size();
    std::vector<const char*> spaces(num_edges, edge_id_space);
    std::vector<const char*> keys(num_edges);
    std::vector<int64_t> int_keys(num_edges);
    std::vector<size_t> key_szs(num_edges);
    std::vector<const hyperdex_client_attribute**> attrs(num_edges, nullptr);
    std::vector<size_t*> num_attrs(num_edges, nullptr);

    const hyperdex_client_attribute *attr_array[num_edges];
    size_t num_attrs_array[num_edges];

    int64_t i = 0;
    for (const uint64_t id: n.edge_ids) {
        int_keys[i] = (int64_t)id;
        keys[i] = (const char*)&int_keys[i];
        key_szs[i] = sizeof(int64_t);
        attrs[i] = attr_array + i;
        num_attrs[i] = num_attrs_array + i;

        i++;
    }

    bool success = multiple_get(spaces, keys, key_szs, attrs, num_attrs, tx);

    std::vector<edge_handle_t> edge_handles;
    if (success) {
        for (i = 0; i < num_edges; i++) {
            if (*num_attrs[i] == NUM_EDGE_ID_ATTRS) {
                edge_handle_t handle = std::string(attr_array[i][0].value,
                                                   attr_array[i][0].value_sz);
                edge_handles.emplace_back(handle);
                hyperdex_client_destroy_attrs(attr_array[i], NUM_EDGE_ATTRS);
            } else {
                WDEBUG << "bad num attributes " << *num_attrs[i] << std::endl;
                success = false;
                if (*num_attrs[i] > 0) {
                    hyperdex_client_destroy_attrs(attr_array[i], *num_attrs[i]);
                }
            }
        }
    } else {
        WDEBUG << "get edges failed" << std::endl;
    }

    if (!success) {
        return false;
    }

    spaces = std::vector<const char*>(num_edges, edge_space);
    i = 0;
    for (const edge_handle_t &eh: edge_handles) {
        keys[i] = eh.c_str();
        key_szs[i] = eh.size();
        attrs[i] = attr_array + i;
        num_attrs[i] = num_attrs_array + i;

        i++;
    }

    success = multiple_get(spaces, keys, key_szs, attrs, num_attrs, tx);

    if (success) {
        for (i = 0; i < num_edges; i++) {
            if (*num_attrs[i] == NUM_EDGE_ATTRS) {
                node_handle_t node_handle;
                uint64_t shard, edge_id;
                db::edge *e;
                success = recreate_edge(attr_array[i],
                                        node_handle,
                                        shard,
                                        edge_id,
                                        &e);
                n.out_edges[edge_handles[i]].emplace_back(e);
                hyperdex_client_destroy_attrs(attr_array[i], NUM_EDGE_ATTRS);
            } else {
                WDEBUG << "bad num attributes " << *num_attrs[i] << std::endl;
                success = false;
                if (*num_attrs[i] > 0) {
                    hyperdex_client_destroy_attrs(attr_array[i], *num_attrs[i]);
                }
            }
        }
    } else {
        WDEBUG << "get edges failed" << std::endl;
    }

    return success;
}

bool
hyper_stub_base :: get_node(db::node &n, bool recreate_edges)
{
    const hyperdex_client_attribute *attr;
    size_t num_attrs;
    const node_handle_t &handle = n.get_handle();

    bool success = get(node_space,
                       handle.c_str(), handle.size(),
                       &attr, &num_attrs,
                       true);
    if (success) {
        success = recreate_node(attr, n);
        if (success && recreate_edges) {
            success = get_node_edges(n, true);
        }
        hyperdex_client_destroy_attrs(attr, num_attrs);
    }

    return success;
}

bool
hyper_stub_base :: get_nodes(std::unordered_map<node_handle_t, db::node*> &nodes,
                             bool recreate_edges,
                             bool tx)
{
    int64_t num_nodes = nodes.size();
    std::vector<const char*> spaces(num_nodes, node_space);
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
            if (*num_attrs[i] == NUM_NODE_ATTRS) {
                db::node &n = *nodes[node_handle_t(keys[i])];
                bool recr_success = recreate_node(attr_array[i], n);
                if (recr_success && recreate_edges) {
                    recr_success = get_node_edges(n, tx);
                }
                hyperdex_client_destroy_attrs(attr_array[i], NUM_NODE_ATTRS);
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
    size_t &num_attrs,
    size_t &packed_node_sz)
{
    size_t attr_idx = 0;
    packed_node_sz = 0;

    // shard
    cl_attr[attr_idx].attr = node_attrs[0];
    cl_attr[attr_idx].value = (const char*)&n.shard;
    cl_attr[attr_idx].value_sz = sizeof(int64_t);
    cl_attr[attr_idx].datatype = node_dtypes[0];
    packed_node_sz += cl_attr[attr_idx].value_sz;
    attr_idx++;

    // create clock
    prepare_buffer(n.base.get_creat_time(), creat_clk_buf);
    cl_attr[attr_idx].attr = node_attrs[1];
    cl_attr[attr_idx].value = (const char*)creat_clk_buf->data();
    cl_attr[attr_idx].value_sz = creat_clk_buf->size();
    cl_attr[attr_idx].datatype = node_dtypes[1];
    packed_node_sz += cl_attr[attr_idx].value_sz;
    attr_idx++;

    // properties
#ifdef weaver_large_property_maps_
    prepare_buffer<std::vector<std::shared_ptr<db::property>>>(n.base.properties, props_buf);
#else
    prepare_buffer(n.base.properties, props_buf);
#endif
    cl_attr[attr_idx].attr = node_attrs[2];
    cl_attr[attr_idx].value = (const char*)props_buf->data();
    cl_attr[attr_idx].value_sz = props_buf->size();
    cl_attr[attr_idx].datatype = node_dtypes[2];
    packed_node_sz += cl_attr[attr_idx].value_sz;
    attr_idx++;

    // out edge ids
    prepare_buffer(n.edge_ids, out_edges_buf);
    cl_attr[attr_idx].attr = node_attrs[3];
    cl_attr[attr_idx].value = (const char*)out_edges_buf->data();
    cl_attr[attr_idx].value_sz = out_edges_buf->size();
    cl_attr[attr_idx].datatype = node_dtypes[3];
    packed_node_sz += cl_attr[attr_idx].value_sz;
    attr_idx++;

    // max edge id
    if (n.max_edge_id) {
        cl_attr[attr_idx].attr = node_attrs[4];
        cl_attr[attr_idx].value = (const char*)&n.max_edge_id;
        cl_attr[attr_idx].value_sz = sizeof(int64_t);
        cl_attr[attr_idx].datatype = node_dtypes[4];
        packed_node_sz += cl_attr[attr_idx].value_sz;
        attr_idx++;
    }

    // last update clock
    if (last_clk_buf == nullptr) {
        prepare_buffer(n.last_upd_clk, last_clk_buf);
    }
    cl_attr[attr_idx].attr = node_attrs[5];
    cl_attr[attr_idx].value = (const char*)last_clk_buf->data();
    cl_attr[attr_idx].value_sz = last_clk_buf->size();
    cl_attr[attr_idx].datatype = node_dtypes[5];
    packed_node_sz += cl_attr[attr_idx].value_sz;
    attr_idx++;

    // restore clock
    if (restore_clk_buf == nullptr) {
        if (n.restore_clk->size() != ClkSz) {
            WDEBUG << "pack error, restore_clk->size=" << n.restore_clk->size() << std::endl;
        }
        prepare_buffer(n.restore_clk, restore_clk_buf);
    }
    cl_attr[attr_idx].attr = node_attrs[6];
    cl_attr[attr_idx].value = (const char*)restore_clk_buf->data();
    cl_attr[attr_idx].value_sz = restore_clk_buf->size();
    cl_attr[attr_idx].datatype = node_dtypes[6];
    packed_node_sz += cl_attr[attr_idx].value_sz;
    attr_idx++;

    // aliases
    prepare_buffer(n.aliases, aliases_buf);
    cl_attr[attr_idx].attr = node_attrs[7];
    cl_attr[attr_idx].value = (const char*)aliases_buf->data();
    cl_attr[attr_idx].value_sz = aliases_buf->size();
    cl_attr[attr_idx].datatype = node_dtypes[7];
    packed_node_sz += cl_attr[attr_idx].value_sz;
    attr_idx++;

    num_attrs = attr_idx;
    assert(num_attrs <= NUM_NODE_ATTRS);
}

void
hyper_stub_base :: prepare_edge(hyperdex_client_attribute *attrs,
    const node_handle_t &node_handle,
    const uint64_t &shard,
    db::edge &e,
    const uint64_t &edge_id,
    std::unique_ptr<e::buffer> &buf,
    size_t &packed_sz)
{
    size_t attr_idx = 0;
    packed_sz = 0;
    e.edge_id = edge_id;
    prepare_buffer<db::edge*>(&e, buf);

    // node handle
    attrs[attr_idx].attr = edge_attrs[attr_idx];
    attrs[attr_idx].value = node_handle.c_str();
    attrs[attr_idx].value_sz = node_handle.size();
    attrs[attr_idx].datatype = edge_dtypes[attr_idx];
    packed_sz += node_handle.size();
    attr_idx++;

    // shard
    attrs[attr_idx].attr = edge_attrs[attr_idx];
    attrs[attr_idx].value = (const char*)&shard;
    attrs[attr_idx].value_sz = sizeof(int64_t);
    attrs[attr_idx].datatype = edge_dtypes[attr_idx];
    packed_sz += sizeof(int64_t);
    attr_idx++;

    // edge id
    attrs[attr_idx].attr = edge_attrs[attr_idx];
    attrs[attr_idx].value = (const char*)&edge_id;
    attrs[attr_idx].value_sz = sizeof(int64_t);
    attrs[attr_idx].datatype = edge_dtypes[attr_idx];
    packed_sz += sizeof(int64_t);
    attr_idx++;

    // edge data
    attrs[attr_idx].attr = edge_attrs[attr_idx];
    attrs[attr_idx].value = (const char*)buf->data();
    attrs[attr_idx].value_sz = buf->size();
    attrs[attr_idx].datatype = edge_dtypes[attr_idx];
    packed_sz += buf->size();
    attr_idx++;
}

hyperdex_client_returncode
hyper_stub_base :: put_new_edge(uint64_t edge_id,
                                db::edge *e,
                                const node_handle_t &node_handle,
                                uint64_t shard)
{
    std::unique_ptr<e::buffer> edge_buf;
    hyperdex_client_attribute attrs[NUM_EDGE_ATTRS];
    size_t packed_sz;
    prepare_edge(attrs, node_handle, shard, *e, edge_id, edge_buf, packed_sz);

    hyperdex_client_returncode code;
    call(hyperdex_client_xact_put_if_not_exist,
         edge_space,
         e->get_handle().c_str(), e->get_handle().size(),
         attrs, NUM_EDGE_ATTRS,
         &code);

    return code;
}

/*
// overwrite edges for updates
bool
hyper_stub_base :: put_edges(const db::data_map<std::vector<db::edge*>> &edges)
{
    int num_edges = 0;
    for (const auto &p: edges) {
        for (const db::edge *e: p.second) {
            UNUSED(e);
            num_edges++;
        }
    }

    std::vector<hyper_tx_func> funcs = std::vector<hyper_tx_func>(num_edges, &hyperdex_client_xact_put);
    std::vector<const char*> spaces(num_edges, edge_space);
    std::vector<const char*> keys(num_edges);
    std::vector<size_t> key_szs(num_edges, sizeof(int64_t));
    std::vector<hyperdex_client_attribute*> attrs(num_edges);
    std::vector<size_t> num_attrs(num_edges, NUM_EDGE_ATTRS);
    std::vector<std::unique_ptr<e::buffer>> edge_buf(num_edges);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_edges * NUM_EDGE_ATTRS * sizeof(hyperdex_client_attribute));

    int i = 0;
    for (auto &p: edges) {
        for (db::edge *e: p.second) {
            keys[i] = (const char*)&e->edge_id;
            attrs[i] = attrs_to_add + NUM_EDGE_ATTRS*i;
            size_t packed_sz = 0;
            prepare_edge(attrs[i], *e, e->edge_id, edge_buf[i], packed_sz);

            i++;
        }
    }

    bool success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);

    free(attrs_to_add);

    return success;
}
*/

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
    std::vector<const char*> spaces(num_nodes, node_space);
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

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_nodes * NUM_NODE_ATTRS * sizeof(hyperdex_client_attribute));

    int i = 0;
    for (auto &p: nodes) {
        attrs[i] = attrs_to_add + NUM_NODE_ATTRS*i;
        keys[i] = p.first.c_str();
        key_szs[i] = p.first.size();

        size_t packed_sz;
        //if (!p.second->out_edges.empty()
        // && !put_edges(p.second->out_edges)) {
        //    return false;
        //}
        prepare_node(attrs[i], *p.second, creat_clk_buf[i], props_buf[i], out_edges_buf[i], last_clk_buf[i], restore_clk_buf[i], aliases_buf[i], num_attrs[i], packed_sz);

        i++;
    }

    bool success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);

    free(attrs_to_add);

    return success;
}

bool
hyper_stub_base :: del_edge(const edge_handle_t &edge_handle, uint64_t edge_id)
{
    return del(edge_id_space, (const char*)&edge_id, sizeof(int64_t))
        && del(edge_space, edge_handle.c_str(), edge_handle.size());
}

bool
hyper_stub_base :: del_edges(const db::node &n)
{
    int64_t num_edges = n.out_edges.size();
    std::vector<const char*> edge_spaces(num_edges, edge_space);
    std::vector<const char*> id_spaces(num_edges, edge_id_space);
    std::vector<const char*> edge_keys(num_edges);
    std::vector<const char*> id_keys(num_edges);
    std::vector<size_t> edge_key_szs(num_edges);
    std::vector<size_t> id_key_szs(num_edges, sizeof(int64_t));

    int i = 0;
    for (const auto &p: n.out_edges) {
        db::edge *e = p.second.front();
        id_keys[i]      = (const char*)&e->edge_id;
        edge_keys[i]    = e->get_handle().c_str();
        edge_key_szs[i] = e->get_handle().size();

        i++;
    }

    return multiple_del(id_spaces, id_keys, id_key_szs)
        && multiple_del(edge_spaces, edge_keys, edge_key_szs);
}

bool
hyper_stub_base :: del_node(const db::node &n)
{
    if (!del_edges(n)) {
        return false;
    }
    const node_handle_t &handle = n.get_handle();
    return del(node_space, handle.c_str(), handle.size());
}

bool
hyper_stub_base :: del_nodes(std::vector<db::node*> &nodes)
{
    int64_t num_nodes = nodes.size();
    std::vector<const char*> spaces(num_nodes, node_space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);

    int i = 0;
    for (db::node *n: nodes) {
        keys[i] = n->get_handle().c_str();
        key_szs[i] = n->get_handle().size();
        if (!del_edges(*n)) {
            return false;
        }

        i++;
    }

    return multiple_del(spaces, keys, key_szs);
}

bool
hyper_stub_base :: update_nmap(const node_handle_t &handle, uint64_t loc)
{
    hyperdex_client_attribute attr;
    attr.attr = node_attrs[0];
    attr.value = (const char*)&loc;
    attr.value_sz = sizeof(int64_t);
    attr.datatype = node_dtypes[0];

    return call(hyperdex_client_put,
                node_space,
                handle.c_str(), handle.size(),
                &attr, 1);
}

uint64_t
hyper_stub_base :: get_nmap(node_handle_t &handle)
{
    const hyperdex_client_attribute *attr;
    size_t num_attrs;
    uint64_t shard = UINT64_MAX;

    bool success = get_partial(node_space,
                               handle.c_str(), handle.size(),
                               node_attrs, 1,
                               &attr, &num_attrs, false);
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
    std::vector<const char*> spaces(num_nodes, node_space);
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

    bool success = multiple_get_partial(spaces, keys, key_szs, node_attrs, 1, attrs, num_attrs, tx);
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
    std::vector<const char*> spaces(num_indices, edge_space);
    std::vector<const char*> keys(num_indices);
    std::vector<size_t> key_szs(num_indices);
    std::vector<hyperdex_client_attribute*> attrs(num_indices);
    std::vector<size_t> num_attrs(num_indices, 2);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_indices * 2 * sizeof(hyperdex_client_attribute));

    uint64_t i = 0;
    for (auto &p: indices) {
        attrs[i] = attrs_to_add + 2*i;
        keys[i] = p.first.c_str();
        key_szs[i] = p.first.size();

        // node handle
        const node_handle_t &handle = p.second->get_handle();
        attrs[i][0].attr     = edge_attrs[0];
        attrs[i][0].value    = handle.c_str();
        attrs[i][0].value_sz = handle.size();
        attrs[i][0].datatype = edge_dtypes[0];

        // shard
        attrs[i][1].attr     = edge_attrs[1];
        attrs[i][1].value    = (const char*)&p.second->shard;
        attrs[i][1].value_sz = sizeof(int64_t);
        attrs[i][1].datatype = edge_dtypes[1];

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
hyper_stub_base :: get_indices(std::unordered_map<std::string, std::pair<node_handle_t, uint64_t>> &indices, bool tx)
{
    if (!AuxIndex) {
        WDEBUG << "logical error: aux index" << std::endl;
        return false;
    }

    int64_t num_indices = indices.size();
    std::vector<const char*> spaces(num_indices, edge_space);
    std::vector<const char*> keys(num_indices);
    std::vector<size_t> key_szs(num_indices);
    std::vector<const hyperdex_client_attribute**> attrs(num_indices, nullptr);
    std::vector<size_t*> num_attrs(num_indices, nullptr);

    const hyperdex_client_attribute *attr_array[num_indices];
    size_t num_attrs_array[num_indices];

    int64_t i = 0;
    std::vector<std::string> key_str_ordered;
    key_str_ordered.reserve(num_indices);
    for (auto &x: indices) {
        key_str_ordered.emplace_back(x.first);
        keys[i]      = key_str_ordered.back().c_str();
        key_szs[i]   = key_str_ordered.back().size();
        attrs[i]     = attr_array + i;
        num_attrs[i] = num_attrs_array + i;

        i++;
    }

    bool success = multiple_get(spaces, keys, key_szs, attrs, num_attrs, tx);

    if (success) {
        uint64_t ignore_edge_id;
        for (i = 0; i < num_indices; i++) {
            if (*num_attrs[i] == NUM_EDGE_ATTRS) {
                std::string &key  = key_str_ordered[i];
                bool recr_success = recreate_edge(attr_array[i],
                                                  indices[key].first,
                                                  indices[key].second,
                                                  ignore_edge_id,
                                                  nullptr);
                hyperdex_client_destroy_attrs(attr_array[i], *num_attrs[i]);
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
    std::vector<const char*> spaces(num_indices, edge_space);
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


// async calls

#define POST_ASYNC_CALL(ac_ptr, async_calls, success, debug_info) \
    if (success) { \
        async_calls[ac_ptr->op_id] = ac_ptr; \
    } else { \
        WDEBUG << "hyperdex_client op failed, op_id=" << ac_ptr->op_id \
               << ", call_code=" << hyperdex_client_returncode_to_string(ac_ptr->status) << std::endl; \
        WDEBUG << debug_info << std::endl; \
    }

bool
hyper_stub_base :: put_node_async(apn_ptr_t apn,
                                  db::node *n,
                                  std::unique_ptr<e::buffer> &lastupd_clk_buf,
                                  std::unique_ptr<e::buffer> &restore_clk_buf,
                                  std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                                  bool put_if_not_exist,
                                  bool tx)
{
    apn->putine = put_if_not_exist;
    apn->tx     = tx;
    apn->handle = n->get_handle();

    prepare_node(apn->attrs,
                 *n,
                 apn->creat_clk_buf,
                 apn->props_buf,
                 apn->out_edges_buf,
                 lastupd_clk_buf,
                 restore_clk_buf,
                 apn->aliases_buf,
                 apn->num_attrs,
                 apn->packed_sz);

    bool success;
    if (tx) {
        hyper_tx_func h = put_if_not_exist? &hyperdex_client_xact_put_if_not_exist
                                          : &hyperdex_client_xact_put;
        success = call_no_loop(h,
                               node_space,
                               apn->handle.c_str(),
                               apn->handle.size(),
                               apn->attrs,
                               apn->num_attrs,
                               apn->op_id, apn->status);
    } else {
        hyper_func h = put_if_not_exist? &hyperdex_client_put_if_not_exist
                                       : &hyperdex_client_put;
        success = call_no_loop(h,
                               node_space,
                               apn->handle.c_str(),
                               apn->handle.size(),
                               apn->attrs,
                               apn->num_attrs,
                               apn->op_id, apn->status);
    }

    POST_ASYNC_CALL(apn,
                    async_calls,
                    success,
                    "node=" << apn->handle);

    return success;
}

bool
hyper_stub_base :: put_edge_id_async(apei_ptr_t apei,
                                     uint64_t edge_id,
                                     const edge_handle_t &edge_handle,
                                     std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                                     bool tx)
{
    apei->putine      = true;
    apei->tx          = tx;
    apei->edge_id     = edge_id;
    apei->edge_handle = edge_handle;
    // edge handle
    apei->edgeid_attrs[0].attr     = edge_id_attrs[0];
    apei->edgeid_attrs[0].value    = apei->edge_handle.c_str();
    apei->edgeid_attrs[0].value_sz = apei->edge_handle.size();
    apei->edgeid_attrs[0].datatype = edge_id_dtypes[0];
    apei->packed_sz                = apei->edge_handle.size();

    bool success;
    if (tx) {
        success = call_no_loop(hyperdex_client_xact_put_if_not_exist,
                               edge_id_space,
                               (const char*)&apei->edge_id,
                               sizeof(int64_t),
                               apei->edgeid_attrs, NUM_EDGE_ID_ATTRS,
                               apei->op_id, apei->status);
    } else {
        success = call_no_loop(hyperdex_client_put_if_not_exist,
                               edge_id_space,
                               (const char*)&apei->edge_id,
                               sizeof(int64_t),
                               apei->edgeid_attrs, NUM_EDGE_ID_ATTRS,
                               apei->op_id, apei->status);
    }

    POST_ASYNC_CALL(apei,
                    async_calls,
                    success,
                    "edge_id=" << edge_id << ", edge_handle=" << edge_handle);

    return success;
}

bool
hyper_stub_base :: put_edge_async(ape_ptr_t ape,
                                  const node_handle_t &node_handle,
                                  db::edge *e,
                                  uint64_t edge_id,
                                  uint64_t shard,
                                  bool del_after_call,
                                  std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                                  bool put_if_not_exist,
                                  bool tx)
{
    ape->putine         = put_if_not_exist;
    ape->tx             = tx;
    ape->edge_handle    = e->get_handle();
    ape->node_handle    = node_handle;
    ape->e              = e;
    ape->del_after_call = del_after_call;
    ape->edge_id        = edge_id;
    ape->shard          = shard;

    prepare_edge(ape->attrs,
                 ape->node_handle,
                 shard,
                 *e,
                 ape->edge_id,
                 ape->buf,
                 ape->packed_sz);

    bool success;
    if (tx) {
        hyper_tx_func h = put_if_not_exist? &hyperdex_client_xact_put_if_not_exist
                                          : &hyperdex_client_xact_put;
        success = call_no_loop(h,
                               edge_space,
                               ape->edge_handle.c_str(),
                               ape->edge_handle.size(),
                               ape->attrs, NUM_EDGE_ATTRS,
                               ape->op_id, ape->status);
    } else {
        hyper_func h = put_if_not_exist? &hyperdex_client_put_if_not_exist
                                       : &hyperdex_client_put;
        success = call_no_loop(h,
                               edge_space,
                               ape->edge_handle.c_str(),
                               ape->edge_handle.size(),
                               ape->attrs, NUM_EDGE_ATTRS,
                               ape->op_id, ape->status);
    }

    POST_ASYNC_CALL(ape,
                    async_calls,
                    success,
                    "edge=" << ape->edge_handle);

    return success;
}

bool
hyper_stub_base :: add_index_async(aai_ptr_t aai,
                                   const node_handle_t &node_handle,
                                   const std::string &alias,
                                   const uint64_t &shard,
                                   std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                                   bool put_if_not_exist,
                                   bool tx)
{
    aai->putine      = put_if_not_exist;
    aai->tx          = tx;
    aai->node_handle = node_handle;
    aai->alias       = alias;
    // node handle
    aai->index_attrs[0].attr = edge_attrs[0];
    aai->index_attrs[0].value = aai->node_handle.c_str();
    aai->index_attrs[0].value_sz = aai->node_handle.size();
    aai->index_attrs[0].datatype = edge_dtypes[0];
    // shard
    aai->index_attrs[1].attr = edge_attrs[1];
    aai->index_attrs[1].value = (const char*)&shard;
    aai->index_attrs[1].value_sz = sizeof(int64_t);
    aai->index_attrs[1].datatype = edge_dtypes[1];
    aai->packed_sz = aai->index_attrs[0].value_sz
                   + aai->index_attrs[1].value_sz; 

    bool success;
    if (tx) {
        hyper_tx_func h = put_if_not_exist? &hyperdex_client_xact_put_if_not_exist
                                          : &hyperdex_client_xact_put;
        success = call_no_loop(h,
                               edge_space,
                               aai->alias.c_str(),
                               aai->alias.size(),
                               aai->index_attrs, 2,
                               aai->op_id, aai->status);
    } else {
        hyper_func h = put_if_not_exist? &hyperdex_client_put_if_not_exist
                                       : &hyperdex_client_put;
        success = call_no_loop(h,
                               edge_space,
                               aai->alias.c_str(),
                               aai->alias.size(),
                               aai->index_attrs, 2,
                               aai->op_id, aai->status);
    }


    POST_ASYNC_CALL(aai,
                    async_calls,
                    success,
                    "alias=" << aai->alias << ", node=" << aai->node_handle);

    return success;
}

// key should persist until async call has completed
// transactional call
bool
hyper_stub_base :: del_async(ad_ptr_t ad,
                             const char *key, size_t key_sz,
                             const char *space,
                             std::unordered_map<int64_t, async_call_ptr_t> &async_calls)
{
    ad->putine = false;
    ad->tx     = true;
    ad->key    = key;

    bool success = del_no_loop(space, key, key_sz, ad->op_id, ad->status);

    POST_ASYNC_CALL(ad,
                    async_calls,
                    success,
                    "key=" << key << ", space=" << space);

    return success;
}

bool
hyper_stub_base :: get_node_async(agn_ptr_t agn,
                                  db::node *n,
                                  std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                                  bool recreate_edges,
                                  bool tx)
{
    agn->tx             = tx;
    agn->n              = n;
    agn->recreate_edges = recreate_edges;

    bool success;
    success = get_no_loop(node_space,
                          n->get_handle().c_str(), n->get_handle().size(),
                          &agn->cl_attr, &agn->num_attrs,
                          agn->op_id, agn->status,
                          tx);

    POST_ASYNC_CALL(agn,
                    async_calls,
                    success,
                    "node=" << agn->n->get_handle());

    return success;
}

bool
hyper_stub_base :: get_edge_handle_async(ageh_ptr_t ageh,
                                         db::node *n,
                                         uint64_t edge_id,
                                         std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                                         bool tx)
{
    ageh->edge_id = (int64_t)edge_id;
    ageh->n       = n;
    ageh->tx      = tx;

    bool success;
    success = get_no_loop(edge_id_space,
                          (const char*)&ageh->edge_id, sizeof(int64_t),
                          &ageh->cl_attr, &ageh->num_attrs,
                          ageh->op_id, ageh->status,
                          tx);

    POST_ASYNC_CALL(ageh,
                    async_calls,
                    success,
                    "edge_id=" << edge_id);

    return success;
}

bool
hyper_stub_base :: get_edge_async(age_ptr_t age,
                                  db::node *n,
                                  const edge_handle_t &edge_handle,
                                  std::unordered_map<int64_t, async_call_ptr_t> &async_calls,
                                  bool tx)
{
    age->handle = edge_handle;
    age->n      = n;
    age->tx     = tx;

    bool success;
    success = get_no_loop(edge_space,
                          age->handle.c_str(), age->handle.size(),
                          &age->cl_attr, &age->num_attrs,
                          age->op_id, age->status,
                          tx);

    POST_ASYNC_CALL(age,
                    async_calls,
                    success,
                    "edge=" << edge_handle);

    return success;
}


void
hyper_stub_base :: pack_uint64(e::packer &pkr, uint64_t num)
{
    uint8_t intbuf[8];
    e::pack64le(num, intbuf);
    for (size_t i = 0; i < 8; i++) {
        pkr = pkr << intbuf[i];
    }
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
hyper_stub_base :: pack_uint32(e::packer &pkr, uint32_t num)
{
    uint8_t intbuf[4];
    e::pack32le(num, intbuf);
    for (size_t i = 0; i < 4; i++) {
        pkr = pkr << intbuf[i];
    }
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
hyper_stub_base :: pack_string(e::packer &packer, const std::string &s)
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

void
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
    e::packer packer = buf->pack();

    for (uint64_t i = 0; i < vec.size(); i++) {
        pack_uint32(packer, vec[i].size());
        pack_string(packer, vec[i]);
    }
}

// store the given unordered_set as a HYPERDATATYPE_SET_STRING
void
hyper_stub_base :: prepare_buffer(const std::unordered_set<std::string> &set, std::unique_ptr<e::buffer> &buf)
{
    std::vector<std::string> sorted;
    sorted.reserve(set.size());

    for (const std::string &s: set) {
        sorted.emplace_back(s);
    }

    sort_and_pack_as_set(sorted, buf);
}

void
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
    while (unpacker.remain() > 0) { \
        key.erase(); \
        unpack_uint32(unpacker, sz); \
        unpack_string(unpacker, key, sz); \
        set.insert(key); \
    }

// unpack the HYPERDATATYPE_SET_STRING in to the given set
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<std::string> &set)
{
    UNPACK_SET;
}

void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz,
    db::string_set &set)
{
    UNPACK_SET;
}

#undef UNPACK_SET

// pack as HYPERDATATYPE_LIST_STRING
void
hyper_stub_base :: prepare_buffer(const std::vector<std::shared_ptr<db::property>> &props, std::unique_ptr<e::buffer> &buf)
{
    std::vector<uint32_t> sz;
    sz.reserve(props.size());
    uint32_t buf_sz = 0;

    for (const std::shared_ptr<db::property> p: props) {
        sz.emplace_back(message::size(nullptr, p));
        buf_sz += sizeof(uint32_t)
                + sz.back();
    }

    buf.reset(e::buffer::create(buf_sz));
    e::packer packer = buf->pack();

    for (uint64_t i = 0; i < props.size(); i++) {
        pack_uint32(packer, sz[i]);
        message::pack_buffer(packer, nullptr, props[i]);
    }
}

// unpack from HYPERDATATYPE_LIST_STRING
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::vector<std::shared_ptr<db::property>> &props)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    uint32_t sz;

    while (unpacker.remain() > 0) {
        unpack_uint32(unpacker, sz);
        std::shared_ptr<db::property> p;
        message::unpack_buffer(unpacker, nullptr, p);

        props.emplace_back(std::move(p));
    }
}

// pack as HYPERDATATYPE_SET_INT64
void
hyper_stub_base :: prepare_buffer(const std::set<int64_t> &set,
                                  std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = sizeof(int64_t) * set.size();
    buf.reset(e::buffer::create(buf_sz));
    e::packer packer = buf->pack();

    for (int64_t i: set) {
        uint64_t ui = (uint64_t)i;
        pack_uint64(packer, ui);
    }
}

// unpack from HYPERDATATYPE_SET_INT64 in to std::set<int64_t>
void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz,
                                 std::set<int64_t> &set)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);

    while (unpacker.remain() > 0) {
        uint64_t ui;
        unpack_uint64(unpacker, ui);
        set.emplace((int64_t)ui);
    }
}


const char*
async_call_type_to_string(async_call_type t)
{
    switch (t) {
        case PUT_NODE:
            return "PUT_NODE";
        case PUT_EDGE_SET:
            return "PUT_EDGE_SET";
        case PUT_EDGE:
            return "PUT_EDGE";
        case PUT_EDGE_ID:
            return "PUT_EDGE_ID";
        case ADD_INDEX:
            return "ADD_INDEX";
        case DEL:
            return "DEL";
        default:
            return "BAD TYPE";
    }
}

void
hyper_stub_base :: debug_print_async_call(async_call_ptr_t ac_ptr)
{
    WDEBUG << std::endl
           << "async_call_type=" << async_call_type_to_string(ac_ptr->type) << std::endl
           << "returncode=" << hyperdex_client_returncode_to_string(ac_ptr->status) << std::endl
           << "op_id=" << ac_ptr->op_id << std::endl
           << "packed_sz=" << ac_ptr->packed_sz << std::endl
           << "putine=" << ac_ptr->putine << std::endl
           << "tx=" << ac_ptr->tx << std::endl;


    switch (ac_ptr->type) {
        case PUT_NODE: {
            apn_ptr_t apn = std::static_pointer_cast<async_put_node>(ac_ptr);
            WDEBUG << std::endl
                   << "\thandle=" << apn->handle << std::endl
                   << "\tnum_attrs=" << apn->num_attrs << std::endl
                   << std::endl;
            break;
        }

        case PUT_EDGE_SET: {
            apes_ptr_t apes = std::static_pointer_cast<async_put_edge_set>(ac_ptr);
            WDEBUG << std::endl
                   << "\tnode_handle=" << apes->node_handle << std::endl
                   << "\tmax_edge_id=" << apes->max_edge_id << std::endl
                   << std::endl;
            break;
        }

        case PUT_EDGE: {
            ape_ptr_t ape = std::static_pointer_cast<async_put_edge>(ac_ptr);
            WDEBUG << std::endl
                   << "\tedge_handle=" << ape->edge_handle << std::endl
                   << "\tnode_handle=" << ape->node_handle << std::endl
                   << "\tdel_after_call=" << ape->del_after_call << std::endl
                   << "\tedge_id=" << ape->edge_id << std::endl
                   << "\tshard=" << ape->shard << std::endl
                   << std::endl;
            break;
       }

        case PUT_EDGE_ID: {
            apei_ptr_t apei = std::static_pointer_cast<async_put_edge_id>(ac_ptr);
            WDEBUG << std::endl
                   << "\tedge_id=" << apei->edge_id << std::endl
                   << "\tedge_handle=" << apei->edge_handle << std::endl
                   << std::endl;
            break;
        }

        case ADD_INDEX: {
            aai_ptr_t aai = std::static_pointer_cast<async_add_index>(ac_ptr);
            WDEBUG << std::endl
                   << "\tnode_handle=" << aai->node_handle << std::endl
                   << "\talias=" << aai->alias << std::endl
                   << std::endl;
            break;
        }

        case DEL: {
            ad_ptr_t ad = std::static_pointer_cast<async_del>(ac_ptr);
            WDEBUG << std::endl
                   << "\tkey=" << ad->key << std::endl
                   << std::endl;
            break;
        }

        default:
            WDEBUG << "impossible async call type=" << async_call_type_to_string(ac_ptr->type) << std::endl;
    }
}
