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


comm_wrapper :: comm_wrapper(uint64_t bbid, int nthr, int to)
    : active_server_idx(NumEffectiveServers, UINT64_MAX)
    , reverse_server_idx(NumActualServers, UINT64_MAX)
    , bb_id(bbid)
    , num_threads(nthr)
    , timeout(to)
{
    int inport;
    uint64_t id;
    std::string ipaddr;
    std::ifstream file;
    file.exceptions(std::ifstream::badbit);

    try {
        file.open("/usr/local/etc/weaver_shards.conf", std::ifstream::in);
        if (file == NULL || !file.good()) {
            file.open("/etc/weaver_shards.conf", std::ifstream::in);
        }
        // one of these locations should contain the shards configuration
        // TODO hardcoded for now, try to get sysconfdir
        assert(file != NULL);
        assert(file.good());

        while (file >> id >> ipaddr >> inport) {
            cluster[WEAVER_TO_BUSYBEE(id)] = po6::net::location(ipaddr.c_str(), inport);
            if (id == bb_id) {
                loc.reset(new po6::net::location(ipaddr.c_str(), inport));
            }
        }
        file.close();
    } catch (std::ifstream::failure e) {
        WDEBUG << "file exception" << std::endl;
    }
}

void
comm_wrapper :: init(configuration &config)
{
    uint64_t primary = bb_id;
    wmap.reset(new weaver_mapper(cluster));
    reconfigure_internal(config, primary);
    WDEBUG << "Busybee attaching to loc " << loc->address << ":" << loc->port << std::endl;
    bb.reset(new busybee_mta(&bb_gc, wmap.get(), *loc, WEAVER_TO_BUSYBEE(bb_id), num_threads));
    bb->set_timeout(timeout);

    std::unique_ptr<e::garbage_collector::thread_state> gc_ts_ptr;
    for (int i = 0; i < num_threads; i++) {
        gc_ts_ptr.reset(new e::garbage_collector::thread_state());
        bb_gc.register_thread(gc_ts_ptr.get());
        bb_gc_ts.emplace_back(std::move(gc_ts_ptr));
    }
}

void
comm_wrapper :: client_init()
{
    wmap.reset(new weaver_mapper(cluster));
    active_server_idx.resize(NumVts, 0);
    reverse_server_idx.resize(NumVts, 0);
    for (uint64_t i = 0; i < NumVts; i++) {
        active_server_idx[i] = i;
        reverse_server_idx[i] = i;
    }
    bb.reset(new busybee_mta(&bb_gc, wmap.get(), *loc, WEAVER_TO_BUSYBEE(bb_id), num_threads));

    std::unique_ptr<e::garbage_collector::thread_state> gc_ts_ptr;
    gc_ts_ptr.reset(new e::garbage_collector::thread_state());
    bb_gc.register_thread(gc_ts_ptr.get());
    bb_gc_ts.emplace_back(std::move(gc_ts_ptr));
}

uint64_t
comm_wrapper :: reconfigure(configuration &new_config)
{
    uint64_t primary = bb_id;
    bb->pause();
    reconfigure_internal(new_config, primary);
    bb->unpause();
    return primary;
}

void
comm_wrapper :: reconfigure_internal(configuration &new_config, uint64_t &primary)
{
    config = new_config;
    for (uint64_t i = 0; i < NumEffectiveServers; i++) {
        if (active_server_idx[i] < UINT64_MAX) {
            uint64_t srv_idx = active_server_idx[i];
            while (config.get_state(server_id(srv_idx)) != server::AVAILABLE) {
                srv_idx = srv_idx + NumEffectiveServers;
                if (srv_idx > NumActualServers) {
                    // all backups dead, not contactable
                    // cannot do anything
                    WDEBUG << "Caution! All backups for server " << i << " are dead\n";
                    break;
                }
            }
            if (srv_idx != active_server_idx[i]) {
                active_server_idx[i] = srv_idx;
                reverse_server_idx[srv_idx] = i;
            }
        } else {
            // server i is not yet up
            // check only i in the new config
            if (config.get_state(server_id(i)) == server::AVAILABLE) {
                active_server_idx[i] = i;
                reverse_server_idx[i] = i;
            }
        }
    }
    while (primary >= NumEffectiveServers) {
        primary -= (NumEffectiveServers);
    }
    primary = active_server_idx[primary];
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
busybee_returncode
comm_wrapper :: send(uint64_t send_to, std::auto_ptr<e::buffer> msg)
{
    busybee_returncode code = bb->send(send_to < CLIENT_ID? active_server_idx[send_to] : send_to, msg);
    if (code != BUSYBEE_SUCCESS) {
        WDEBUG << "busybee send returned " << code << std::endl;
    }
    return code;
}

busybee_returncode
comm_wrapper :: recv(uint64_t *recv_from, std::auto_ptr<e::buffer> *msg)
{
    uint64_t actual_server_id;
    busybee_returncode code = bb->recv(&actual_server_id, msg);
    if (code == BUSYBEE_SUCCESS) {
        *recv_from = actual_server_id < CLIENT_ID_INCR?
                     reverse_server_idx[BUSYBEE_TO_WEAVER(actual_server_id)] : BUSYBEE_TO_WEAVER(actual_server_id);
    } else if (code != BUSYBEE_TIMEOUT) {
        WDEBUG << "busybee recv returned " << code << std::endl;
    }
    return code;
}
#pragma GCC diagnostic pop

void
comm_wrapper :: quiesce_thread(int tid)
{
    bb_gc.quiescent_state(bb_gc_ts[tid].get());
}

#undef weaver_debug_
