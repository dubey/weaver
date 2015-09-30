/*
 * ===============================================================
 *    Description:  Implementation of Busybee wrapper.
 *
 *        Created:  2014-02-14 15:02:35
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <unordered_set>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "common/message_constants.h"
#include "common/comm_wrapper.h"
#include "common/passert.h"

#define ID_INCR (1ULL << 32ULL)
#define WEAVER_TO_BUSYBEE(x) (x+ID_INCR)
#define BUSYBEE_TO_WEAVER(x) (x-ID_INCR)
#define CLIENT_ID_INCR (CLIENT_ID + ID_INCR)

using common::comm_wrapper;

bool
comm_wrapper :: weaver_mapper :: lookup(uint64_t server_id, po6::net::location *loc)
{
    auto mlist_iter = mlist.find(WEAVER_TO_BUSYBEE(server_id));
    if (mlist_iter == mlist.end()) {
        return false;
    }
    *loc = mlist_iter->second;
    return true;
}

void
comm_wrapper :: weaver_mapper :: add_mapping(uint64_t server_id, const po6::net::location &loc)
{
    auto find_iter = mlist.find(server_id);
    if (find_iter == mlist.end()) {
        mlist[WEAVER_TO_BUSYBEE(server_id)] = loc;
    } else {
        PASSERT(find_iter->second == loc);
    }
}


comm_wrapper :: comm_wrapper(std::shared_ptr<po6::net::location> my_loc,
                             int nthr,
                             int to,
                             server_manager_link_wrapper *sm)
    : active_server_idx(NumVts, UINT64_MAX)
    , num_threads(nthr)
    , timeout(to)
    , sm_stub(sm)
{
    wmap.reset(new weaver_mapper());

    bool done = false;
    while (!done) {
        done = true;
        try {
            bb.reset(new busybee_mta(&bb_gc, wmap.get(), *my_loc, WEAVER_TO_BUSYBEE(bb_id), num_threads));
        } catch (std::runtime_error &e) {
            done = false;
            if (errno == EADDRINUSE) {
                // retry another port
                my_loc->port++;
            } else {
                WDEBUG << "exception " << e.what() << std::endl;
                throw e;
            }
        }
    }

    loc = my_loc;
    WDEBUG << "attaching to loc " << loc->address << ":" << loc->port << std::endl;
    bb->set_timeout(timeout);

    std::unique_ptr<e::garbage_collector::thread_state> gc_ts_ptr;
    for (int i = 0; i < num_threads; i++) {
        gc_ts_ptr.reset(new e::garbage_collector::thread_state());
        bb_gc.register_thread(gc_ts_ptr.get());
        bb_gc_ts.emplace_back(std::move(gc_ts_ptr));
    }
}

comm_wrapper :: ~comm_wrapper()
{
    for (auto &gc_ptr: bb_gc_ts) {
        bb_gc.deregister_thread(gc_ptr.get());
    }
}

void
comm_wrapper :: reconfigure(configuration &new_config, bool to_pause, uint64_t *num_active_vts)
{
    if (to_pause) {
        bb->pause();
    }

    reconfigure_internal(new_config);

    if (num_active_vts != nullptr) {
        *num_active_vts = 0;
        for (uint64_t i = 0; i < NumVts; i++) {
            if (active_server_idx[i] != UINT64_MAX) {
                *num_active_vts = (*num_active_vts) + 1;
            }
        }
    }

    if (to_pause) {
        bb->unpause();
    }
}

void
comm_wrapper :: reconfigure_internal(configuration &new_config)
{
    config = new_config;

    std::vector<server> servers = config.get_servers();

    std::unordered_set<uint64_t> shard_set;
    for (const server &srv: servers) {
        if (srv.type == server::SHARD) {
            shard_set.emplace(srv.virtual_id);
        }
    }
    uint64_t num_shards = shard_set.size();
    PASSERT(active_server_idx.size() <= (NumVts+num_shards));
    active_server_idx.resize(NumVts+num_shards, UINT64_MAX);

    for (const server &srv: servers) {
        PASSERT(srv.weaver_id != UINT64_MAX);

        if (srv.type != server::SHARD && srv.type != server::VT) {
            WDEBUG << "Server=" << srv.weaver_id
                   << ", loc=" << srv.bind_to
                   << ", role=" << server::to_string(srv.type)
                   << ", state=" << server::to_string(srv.state)
                   << "." << std::endl;
        } else {
            uint64_t factor = (srv.type == server::SHARD) ? 1 : 0;
            uint64_t vid = srv.virtual_id + NumVts*factor;
            PASSERT(vid < active_server_idx.size());

            WDEBUG << "Server=" << srv.weaver_id
                   << ", loc=" << srv.bind_to
                   << ", role=" << server::to_string(srv.type)
                   << ", state=" << server::to_string(srv.state)
                   << "." << std::endl;

            if (srv.state == server::AVAILABLE) {
                active_server_idx[vid] = srv.weaver_id;
                wmap->add_mapping(srv.weaver_id, srv.bind_to);
            }
        }
    }
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
busybee_returncode
comm_wrapper :: send(uint64_t send_to, std::auto_ptr<e::buffer> msg)
{
    busybee_returncode code = bb->send(active_server_idx[send_to], msg);
    if (code != BUSYBEE_SUCCESS) {
        WDEBUG << "busybee send returned " << code << std::endl;
    }
    return code;
}

busybee_returncode
comm_wrapper :: send_to_client(uint64_t send_to, std::auto_ptr<e::buffer> msg)
{
    busybee_returncode code = bb->send(send_to, msg);
    if (code != BUSYBEE_SUCCESS) {
        WDEBUG << "busybee send returned " << code << std::endl;
    }
    return code;
}

busybee_returncode
comm_wrapper :: recv(int tid, uint64_t *recv_from, std::auto_ptr<e::buffer> *msg)
{
    busybee_returncode rc = bb->recv(bb_gc_ts[tid].get(), recv_from, msg);

    if (rc == BUSYBEE_DISRUPTED) {
        handle_disruption(*recv_from);
    }

    return rc;
}

busybee_returncode
comm_wrapper :: recv(int tid, std::auto_ptr<e::buffer> *msg)
{
    uint64_t recv_from;
    busybee_returncode rc = bb->recv(bb_gc_ts[tid].get(), &recv_from, msg);

    if (rc == BUSYBEE_DISRUPTED) {
        handle_disruption(recv_from);
    }

    return rc;
}
#pragma GCC diagnostic pop

void
comm_wrapper :: quiesce_thread(int tid)
{
    bb_gc.quiescent_state(bb_gc_ts[tid].get());
}

void
comm_wrapper :: handle_disruption(uint64_t id)
{
    if (config.get_address(server_id(id)) != *loc) {
        sm_stub->report_tcp_disconnect(id);
    }
}

#undef weaver_debug_
