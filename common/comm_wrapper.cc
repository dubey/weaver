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

void
comm_wrapper :: weaver_mapper :: add_mapping(uint64_t server_id, po6::net::location *loc)
{
    assert(mlist.find(WEAVER_TO_BUSYBEE(server_id)) == mlist.end());
    mlist[WEAVER_TO_BUSYBEE(server_id)] = *loc;
}


comm_wrapper :: comm_wrapper(po6::net::location &my_loc, int nthr, int to)
    : active_server_idx(NumEffectiveServers, UINT64_MAX)
    //, reverse_server_idx(NumActualServers, UINT64_MAX)
    , loc(new po6::net::location(my_loc))
    , num_threads(nthr)
    , timeout(to)
{
    //int inport;
    //uint64_t id;
    //std::string ipaddr;
    //std::ifstream file;
    //file.exceptions(std::ifstream::badbit);

    //try {
    //    // one of these locations should contain the shards configuration
    //    file.open(ShardsFile, std::ifstream::in);
    //    if (file == NULL || !file.good()) {
    //        file.open("./weaver_shards.conf", std::ifstream::in);
    //    }
    //    if (file == NULL || !file.good()) {
    //        file.open("/usr/local/etc/weaver_shards.conf", std::ifstream::in);
    //    }
    //    assert(file != NULL);
    //    assert(file.good());

    //    while (file >> id >> ipaddr >> inport) {
    //        cluster[WEAVER_TO_BUSYBEE(id)] = po6::net::location(ipaddr.c_str(), inport);
    //        if (id == bb_id) {
    //            loc.reset(new po6::net::location(ipaddr.c_str(), inport));
    //        }
    //    }
    //    file.close();
    //} catch (std::ifstream::failure e) {
    //    WDEBUG << "file exception" << std::endl;
    //}
}

void
comm_wrapper :: init(configuration &config)
{
    uint64_t primary = bb_id;
    //wmap.reset(new weaver_mapper(cluster));
    wmap.reset(new weaver_mapper());
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
comm_wrapper :: client_init(configuration config)
{
    //wmap.reset(new weaver_mapper(cluster));
    wmap.reset(new weaver_mapper());
    uint64_t dummy = 0;
    WDEBUG << "here" << std::endl;
    reconfigure_internal(config, dummy);
    WDEBUG << "here" << std::endl;
    active_server_idx.resize(NumVts, 0);
    //reverse_server_idx.resize(NumVts, 0);
    WDEBUG << "here" << std::endl;
    for (uint64_t i = 0; i < NumVts; i++) {
        active_server_idx[i] = i;
        //reverse_server_idx[i] = i;
    }
    WDEBUG << "here" << std::endl;
    bb.reset(new busybee_mta(&bb_gc, wmap.get(), *loc, WEAVER_TO_BUSYBEE(bb_id), num_threads));
    WDEBUG << "here" << std::endl;

    std::unique_ptr<e::garbage_collector::thread_state> gc_ts_ptr;
    gc_ts_ptr.reset(new e::garbage_collector::thread_state());
    bb_gc.register_thread(gc_ts_ptr.get());
    bb_gc_ts.emplace_back(std::move(gc_ts_ptr));
    WDEBUG << "here" << std::endl;
}

uint64_t
comm_wrapper :: reconfigure(configuration &new_config, uint64_t *num_active_vts)
{
    uint64_t primary = bb_id;
    bb->pause();
    reconfigure_internal(new_config, primary);
    if (num_active_vts != NULL) {
        *num_active_vts = 0;
        for (uint64_t i = 0; i < NumVts; i++) {
            if (active_server_idx[i] != UINT64_MAX) {
                *num_active_vts = (*num_active_vts) + 1;
            }
        }
    }
    bb->unpause();
    return primary;
}

void
comm_wrapper :: reconfigure_internal(configuration &new_config, uint64_t &primary)
{
    config = new_config;
    std::vector<std::pair<server_id, po6::net::location>> addresses;
    config.get_all_addresses(&addresses);

    for (auto &p: addresses) {
        WDEBUG << "config has " << p.first << "," << p.second << std::endl;
        if (config.get_weaver_id(p.first) != UINT64_MAX) {
            uint64_t factor = config.get_shard_or_vt(p.first) ? 1 : 0;
            uint64_t wid = config.get_weaver_id(p.first) + NumVts*factor;
            WDEBUG << "wid " << wid << " has been configured at some point" << std::endl;
            if (active_server_idx[wid] < UINT64_MAX
             && config.get_state(p.first) != server::AVAILABLE) {
                // reconfigure to backup
            } else if (active_server_idx[wid] == UINT64_MAX) {
                WDEBUG << "setting up " << wid << " now at loc  " << p.second << std::endl;
                active_server_idx[wid] = wid;
                //reverse_server_idx[wid] = wid;
                wmap->add_mapping(wid, &p.second);
            }
        }
    }

    //for (uint64_t i = 0; i < NumEffectiveServers; i++) {
    //    if (active_server_idx[i] < UINT64_MAX) {


    //        uint64_t srv_idx = active_server_idx[i];
    //        while (config.get_state(server_id(srv_idx)) != server::AVAILABLE) {
    //            srv_idx = srv_idx + NumEffectiveServers;
    //            if (srv_idx > NumActualServers) {
    //                // all backups dead, not contactable
    //                // cannot do anything
    //                WDEBUG << "Caution! All backups for server " << i << " are dead\n";
    //                break;
    //            }
    //        }
    //        if (srv_idx != active_server_idx[i]) {
    //            active_server_idx[i] = srv_idx;
    //            reverse_server_idx[srv_idx] = i;
    //        }
    //    } else {
    //        // server i is not yet up
    //        // check only i in the new config
    //        if (config.get_state(server_id(i)) == server::AVAILABLE) {
    //            active_server_idx[i] = i;
    //            reverse_server_idx[i] = i;
    //        }
    //    }
    //}
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
    busybee_returncode code = bb->send(send_to < NumActualServers? active_server_idx[send_to] : send_to, msg);
    if (code != BUSYBEE_SUCCESS) {
        WDEBUG << "busybee send returned " << code << std::endl;
    }
    return code;
}

busybee_returncode
comm_wrapper :: recv(uint64_t *recv_from, std::auto_ptr<e::buffer> *msg)
{
    return bb->recv(recv_from, msg);
    //  uint64_t actual_server_id;
    //  busybee_returncode code = bb->recv(&actual_server_id, msg);
    //  if (code == BUSYBEE_SUCCESS) {
    //      *recv_from = actual_server_id < CLIENT_ID_INCR?
    //                   reverse_server_idx[BUSYBEE_TO_WEAVER(actual_server_id)] : BUSYBEE_TO_WEAVER(actual_server_id);
    //  } else if (code != BUSYBEE_TIMEOUT) {
    //      WDEBUG << "busybee recv returned " << code << std::endl;
    //  }
    //  return code;
}

busybee_returncode
comm_wrapper :: recv(std::auto_ptr<e::buffer> *msg)
{
    uint64_t recv_from;
    busybee_returncode code = bb->recv(&recv_from, msg);
    //WDEBUG << "recvd from " << recv_from << std::endl;
    return code;
    //  uint64_t actual_server_id;
    //  busybee_returncode code = bb->recv(&actual_server_id, msg);
    //  if (code == BUSYBEE_SUCCESS) {
    //      *recv_from = actual_server_id < CLIENT_ID_INCR?
    //                   reverse_server_idx[BUSYBEE_TO_WEAVER(actual_server_id)] : BUSYBEE_TO_WEAVER(actual_server_id);
    //  } else if (code != BUSYBEE_TIMEOUT) {
    //      WDEBUG << "busybee recv returned " << code << std::endl;
    //  }
    //  return code;
}
#pragma GCC diagnostic pop

void
comm_wrapper :: quiesce_thread(int tid)
{
    bb_gc.quiescent_state(bb_gc_ts[tid].get());
}

#undef weaver_debug_
