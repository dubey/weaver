/*
 * ===============================================================
 *    Description:  Shard hyperdex stub implementation.
 *
 *        Created:  2014-02-18 15:32:42
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "db/shard_constants.h"
#include "db/hyper_stub.h"

using db::hyper_stub;

hyper_stub :: hyper_stub(uint64_t sid)
    : shard_id(sid)
{ }

void
hyper_stub :: restore_backup(bool &migr_token,
    std::unordered_map<node_handle_t, element::node*> *nodes,
    po6::threads::mutex *shard_mutexes)
{
    UNUSED(migr_token);
    UNUSED(nodes);
    UNUSED(shard_mutexes);
}

void
hyper_stub :: bulk_load(std::unordered_map<node_handle_t, element::node*> *nodes_arr)
{
    UNUSED(nodes_arr);
}

bool
hyper_stub :: put_mapping(const node_handle_t &handle, uint64_t loc)
{
    return put_nmap(handle, loc);
}

bool
hyper_stub :: put_mappings(std::unordered_map<node_handle_t, uint64_t> &map)
{
    begin_tx();
    bool success = put_nmap(map);

    if (success) {
        hyperdex_client_returncode commit_status = HYPERDEX_CLIENT_GARBAGE;
        commit_tx(commit_status);
        if (commit_status == HYPERDEX_CLIENT_SUCCESS) {
            return true;
        }
    } else {
        abort_tx();
    }

    return false;
}

#undef weaver_debug_
