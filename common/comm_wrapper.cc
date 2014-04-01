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
#include "comm_wrapper.h"

using common::comm_wrapper;

bool
comm_wrapper :: weaver_mapper :: lookup(uint64_t server_id, po6::net::location *loc)
{
    uint64_t incr_id = ID_INCR + server_id;
    if (mlist.find(incr_id) != mlist.end()) {
        *loc = mlist.at(incr_id);
        WDEBUG << "lookup for server id " << server_id << " returned ip: " << loc->address << ":" << loc->port << std::endl;
        return true;
    } else {
        WDEBUG << "returning false from mapper lookup for id " << server_id << ", incr id " << incr_id << std::endl;
        return false;
    }
}

comm_wrapper :: comm_wrapper(uint64_t bbid, int nthr, int to, bool client=false)
    : bb_id(bbid)
    , num_threads(nthr)
    , timeout(to)
{
    int inport;
    uint64_t id;
    std::string ipaddr;
    std::ifstream file;
    file.exceptions(std::ifstream::badbit);

    for (uint64_t i = 0; i < NUM_VTS+NUM_SHARDS; i++) {
        active_server_idx[i] = MAX_UINT64;
    }
    try {
        if (client) {
            file.open(CLIENT_SHARDS_FILE, std::ifstream::in);
        } else {
            file.open(SHARDS_FILE, std::ifstream::in);
        }
        assert(file != NULL);
        assert(file.good());

        while (file >> id >> ipaddr >> inport) {
            uint64_t incr_id = ID_INCR + id;
            cluster[incr_id] = po6::net::location(ipaddr.c_str(), inport);
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
    uint64_t changed = UINT64_MAX;
    wmap.reset(new weaver_mapper(cluster));
    reconfigure(config, changed);
    WDEBUG << "Busybee attaching to loc " << loc->address << ":" << loc->port << std::endl;
    bb.reset(new busybee_mta(wmap.get(), *loc, bb_id+ID_INCR, num_threads));
    bb->set_timeout(timeout);
}

void
comm_wrapper :: client_init()
{
    wmap.reset(new weaver_mapper(cluster));
    for (uint64_t i = 0; i < NUM_VTS; i++) {
        active_server_idx[i] = i;
    }
    bb.reset(new busybee_mta(wmap.get(), *loc, bb_id+ID_INCR, num_threads));
}

uint64_t
comm_wrapper :: reconfigure(configuration &new_config, uint64_t &changed)
{
    uint64_t primary = bb_id;
    config = new_config;
    for (uint64_t i = 0; i < NUM_VTS+NUM_SHARDS; i++) {
        if (active_server_idx[i] < MAX_UINT64) {
            uint64_t srv_idx = active_server_idx[i];
            while (config.get_state(server_id(srv_idx)) != server::AVAILABLE) {
                srv_idx = srv_idx + NUM_VTS + NUM_SHARDS;
                if (srv_idx > NUM_SERVERS) {
                    // all backups dead, not contactable
                    // cannot do anything
                    WDEBUG << "Caution! All backups for server " << i << " are dead\n";
                    break;
                }
            }
            if (srv_idx != active_server_idx[i]) {
                WDEBUG << "Idx " << i << ": " << active_server_idx[i] << " dead, now making " << srv_idx << " primary" << std::endl;
                active_server_idx[i] = srv_idx;
                changed = i;
            }
        } else {
            // server i is not yet up
            // check only i in the new config
            if (config.get_state(server_id(i)) == server::AVAILABLE) {
                wmap->server_alive(ID_INCR + i, config.get_address(server_id(i)));
                active_server_idx[i] = i;
            }
        }
    }
    while (primary >= NUM_VTS + NUM_SHARDS) {
        primary -= (NUM_VTS+NUM_SHARDS);
    }
    primary = active_server_idx[primary];
    return primary;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
busybee_returncode
comm_wrapper :: send(uint64_t send_to, std::auto_ptr<e::buffer> msg)
{
    busybee_returncode code = send_to < (NUM_VTS + NUM_SHARDS) ?
                              bb->send(active_server_idx[send_to], msg) :
                              bb->send(send_to, msg);
    if (code != BUSYBEE_SUCCESS) {
        WDEBUG << "busybee send returned " << code << std::endl;
    }
    return code;
}

busybee_returncode
comm_wrapper :: recv(uint64_t *recv_from, std::auto_ptr<e::buffer> *msg)
{
    busybee_returncode code = bb->recv(recv_from, msg);
    if (code != BUSYBEE_SUCCESS && code != BUSYBEE_TIMEOUT) {
        WDEBUG << "busybee recv returned " << code << std::endl;
    }
    return code;
}
#pragma GCC diagnostic pop

#undef weaver_debug_
