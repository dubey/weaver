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

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "common/message_constants.h"
#include "common/comm_wrapper.h"

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
        WDEBUG << "busybee map lookup failed for orig id " << server_id
               << ", busybee id " << WEAVER_TO_BUSYBEE(server_id) << std::endl;
    }
    assert(mlist_iter != mlist.end() && "busybee mapper lookup");
    *loc = mlist_iter->second;
    return true;
}

void
comm_wrapper :: weaver_mapper :: add_mapping(uint64_t server_id, po6::net::location *loc)
{
    assert(mlist.find(WEAVER_TO_BUSYBEE(server_id)) == mlist.end());
    mlist[WEAVER_TO_BUSYBEE(server_id)] = *loc;
}


comm_wrapper :: comm_wrapper(po6::net::location &my_loc, int nthr, int to)
    : active_server_idx(NumVts, UINT64_MAX)
    , num_threads(nthr)
    , timeout(to)
{
    wmap.reset(new weaver_mapper());

    bool done = false;
    while (!done) {
        done = true;
        try {
            bb.reset(new busybee_mta(&bb_gc, wmap.get(), my_loc, WEAVER_TO_BUSYBEE(bb_id), num_threads));
        } catch (po6::error e) {
            done = false;
            if (e == 98) {
                // retry another port
                my_loc.port++;
            } else {
                WDEBUG << "exception " << e << std::endl;
                throw e;
            }
        }
    }

    loc.reset(new po6::net::location(my_loc));
    WDEBUG << "attaching to loc " << loc->address << ":" << loc->port << std::endl;
    bb->set_timeout(timeout);
}

comm_wrapper :: ~comm_wrapper()
{
    for (auto &gc_ptr: bb_gc_ts) {
        bb_gc.deregister_thread(gc_ptr.get());
    }
}

void
comm_wrapper :: init(configuration &config)
{
    reconfigure_internal(config);

    std::unique_ptr<e::garbage_collector::thread_state> gc_ts_ptr;
    for (int i = 0; i < num_threads; i++) {
        gc_ts_ptr.reset(new e::garbage_collector::thread_state());
        bb_gc.register_thread(gc_ts_ptr.get());
        bb_gc_ts.emplace_back(std::move(gc_ts_ptr));
    }
}

void
comm_wrapper :: reconfigure(configuration &new_config, uint64_t *num_active_vts)
{
    bb->pause();
    reconfigure_internal(new_config);
    if (num_active_vts != NULL) {
        *num_active_vts = 0;
        for (uint64_t i = 0; i < NumVts; i++) {
            if (active_server_idx[i] != UINT64_MAX) {
                *num_active_vts = (*num_active_vts) + 1;
            }
        }
    }
    bb->unpause();
}

void
comm_wrapper :: reconfigure_internal(configuration &new_config)
{
    config = new_config;
    std::vector<std::pair<server_id, po6::net::location>> addresses;
    config.get_all_addresses(&addresses);

    uint64_t num_shards = 0;
    for (auto &p: addresses) {
        if (config.get_type(p.first) == server::SHARD) {
            num_shards++;
        }
    }
    assert(active_server_idx.size() <= (NumVts+num_shards));
    active_server_idx.resize(NumVts+num_shards, UINT64_MAX);

    for (auto &p: addresses) {
        assert(config.get_weaver_id(p.first) != UINT64_MAX);
        //uint64_t factor = config.get_shard_or_vt(p.first) ? 1 : 0;
        uint64_t factor = config.get_type(p.first) == server::SHARD ? 1 : 0;
        uint64_t wid = config.get_weaver_id(p.first) + NumVts*factor;
        if (active_server_idx[wid] < UINT64_MAX
         && config.get_state(p.first) != server::AVAILABLE) {
            // reconfigure to backup
        } else if (active_server_idx[wid] == UINT64_MAX) {
            active_server_idx[wid] = wid;
            wmap->add_mapping(wid, &p.second);
        }

        server::state_t st = config.get_state(p.first);
        if (st != server::AVAILABLE) {
            WDEBUG << "Server " << wid << " is in trouble, has state " << server::to_string(st) << std::endl;
        } else {
            WDEBUG << "Server " << wid << " is healthy, has state " << server::to_string(st) << std::endl;
        }
    }

    //while (primary >= NumEffectiveServers) {
    //    primary -= (NumEffectiveServers);
    //}
    //primary = active_server_idx[primary];
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
comm_wrapper :: recv(uint64_t *recv_from, std::auto_ptr<e::buffer> *msg)
{
    return bb->recv(recv_from, msg);
}

busybee_returncode
comm_wrapper :: recv(std::auto_ptr<e::buffer> *msg)
{
    uint64_t recv_from;
    return bb->recv(&recv_from, msg);
}
#pragma GCC diagnostic pop

void
comm_wrapper :: quiesce_thread(int tid)
{
    bb_gc.quiescent_state(bb_gc_ts[tid].get());
}

#undef weaver_debug_
