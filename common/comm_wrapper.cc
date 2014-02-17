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

#define __WEAVER_DEBUG__
#include "comm_wrapper.h"

using common::comm_wrapper;
//using common::comm_wrapper::weaver_mapper;

comm_wrapper :: weaver_mapper :: weaver_mapper()
{
    for (uint64_t i = 0; i < NUM_VTS+NUM_SHARDS; i++) {
        active_server_idx[i] = MAX_UINT64;
    }
}

bool
comm_wrapper :: weaver_mapper :: lookup(uint64_t server_id, po6::net::location *loc)
{
    uint64_t incr_id = ID_INCR + server_id;
    if (mlist.find(incr_id) != mlist.end()) {
        *loc = mlist.at(incr_id);
        return true;
    } else {
        WDEBUG << "returning false from mapper lookup for id " << server_id << std::endl;
        return false;
    }
}

void
comm_wrapper :: weaver_mapper :: reconfigure(configuration &new_config, uint64_t &primary)
{
    config = new_config;
    for (uint64_t i = 0; i < NUM_VTS+NUM_SHARDS; i++) {
        if (active_server_idx[i] < MAX_UINT64) {
            uint64_t srv_idx = active_server_idx[i];
            while (config.get_state(server_id(srv_idx)) != server::AVAILABLE) {
                srv_idx = srv_idx + NUM_SHARDS;
                srv_idx = srv_idx % (NUM_VTS + NUM_SHARDS*(1+NUM_BACKUPS));
                if (srv_idx < NUM_VTS+NUM_SHARDS) {
                    srv_idx += NUM_VTS;
                }
                WDEBUG << "cur server for " << i << " died, checking " << srv_idx
                    << ", active server id " << active_server_idx[i] << std::endl;
                if (srv_idx == active_server_idx[i]) {
                    // all backups dead, not contactable
                    // cannot do anything
                    WDEBUG << "Caution! All backups for server " << i << " are dead\n";
                    break;
                }
            }
            if (srv_idx != active_server_idx[i]) {
                mlist[ID_INCR + i] = config.get_address(server_id(srv_idx));
                active_server_idx[i] = srv_idx;
                //now_primary = srv_idx;
            }
            WDEBUG << "server " << i << " at port " << mlist[ID_INCR + i].port << std::endl;
        } else {
            // server i is not yet up
            // check only i in the new config
            if (config.get_state(server_id(i)) == server::AVAILABLE) {
                mlist[ID_INCR + i] = config.get_address(server_id(i));
                active_server_idx[i] = i;
                //now_primary = i;
                WDEBUG << "server " << i << " at port " << mlist[ID_INCR + i].port << std::endl;
            } else {
                WDEBUG << "server " << i << " not yet up\n";
            }
        }
    }
    while (primary >= NUM_VTS + NUM_SHARDS) {
        primary -= NUM_SHARDS;
    }
    primary = active_server_idx[primary];
}


comm_wrapper :: comm_wrapper(uint64_t bbid, int nthr)
    : bb_id(bbid)
    , num_threads(nthr)
{
    int inport;
    uint64_t id;
    std::string ipaddr;

#ifdef __CLIENT__
    std::ifstream file(CLIENT_SHARDS_FILE);
    assert(file != NULL);
#else
    std::ifstream file(SHARDS_FILE);
    assert(file != NULL);
#endif

    while (file >> id >> ipaddr >> inport) {
        uint64_t incr_id = ID_INCR + id;
        cluster[incr_id] = po6::net::location(ipaddr.c_str(), inport);
        if (id == bb_id) {
            loc.reset(new po6::net::location(ipaddr.c_str(), inport));
        }
    }
    file.close();
}

void
comm_wrapper :: init(configuration &config)
{
    uint64_t primary = bb_id;
    wmap.reset(new weaver_mapper());
    wmap->reconfigure(config, primary);
    bb.reset(new busybee_mta(wmap.get(), *loc, bb_id+ID_INCR, num_threads));
}

uint64_t
comm_wrapper :: reconfigure(configuration &config)
{
    uint64_t primary = bb_id;
    wmap->reconfigure(config, primary);
    return primary;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
busybee_returncode
comm_wrapper :: send(uint64_t send_to, std::auto_ptr<e::buffer> msg)
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
    busybee_returncode code = bb->recv(recv_from, msg);
    if (code != BUSYBEE_SUCCESS) {
        WDEBUG << "busybee recv returned " << code << std::endl;
    }
    return code;
}
#pragma GCC diagnostic pop

#undef __WEAVER_DEBUG__
