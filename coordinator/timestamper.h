/*
 * ===============================================================
 *    Description:  Coordinator vector timestamper class
 *                  definition. Vector timestampers receive client
 *                  requests, attach ordering related metadata,
 *                  and forward them to appropriate shards.
 *
 *        Created:  07/22/2013 12:23:33 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __COORD_VT__
#define __COORD_VT__

#include <vector>
#include <unordered_map>

#include "common/busybee_infra.h"
#include "common/vclock.h"
#include "common/message.h"
#include "transaction.h"
#include "nmap_stub.h"

namespace coordinator
{
    class timestamper
    {
        public:
            uint64_t vt_id; // this vector timestamper's id
            // messaging
            std::shared_ptr<po6::net::location> myloc;
            busybee_mta *bb;
            // timestamper state
            uint64_t loc_gen, handle_gen;
            vc::vclock vclk; // vector clock
            vc::qtimestamp_t qts; // queue timestamp
            std::unordered_map<uint64_t, pending_update> pending_updates;
            // node map client
            coordinator::nmap_stub nmap_client;
            // mutexes
            po6::threads::mutex mutex, loc_gen_mutex;
            // migration
            // permanent deletion
            // daemon

        public:
            timestamper(uint64_t id);
            busybee_returncode send(uint64_t shard_id, std::auto_ptr<e::buffer> buf);
            void unpack_tx(message::message &msg, coordinator::pending_tx &tx);
            void clean_nmap_space();
    };

    inline
    timestamper :: timestamper(uint64_t id)
        : vt_id(id)
        , loc_gen(0)
        , handle_gen(0)
        , vclk(id)
        , qts(NUM_SHARDS, 0)
    {
        // initialize array of server locations
        initialize_busybee(bb, vt_id, myloc, NUM_THREADS);
    }

    inline busybee_returncode
    timestamper :: send(uint64_t shard_id, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        if ((ret = bb->send(shard_id, buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "message sending error: " << ret << std::endl;
        }
        return ret;
    }

    inline void
    timestamper :: unpack_tx(message::message &msg, pending_tx &tx)
    {
        message::unpack_client_tx(msg, tx);

        // lookup mappings
        std::vector<uint64_t> get_map;
        std::unordered_map<uint64_t, uint64_t> requested_map;
        for (auto upd: tx.writes) {
            switch (upd->type) {
                case message::NODE_CREATE_REQ:
                    // assign shard for this node
                    loc_gen_mutex.lock();
                    loc_gen = (loc_gen + 1) % NUM_SHARDS;
                    upd->loc1 = loc_gen + SHARD_ID_INCR; // node will be placed on this shard
                    loc_gen_mutex.unlock();
                    requested_map.emplace(upd->handle, upd->loc1);
                    break;

                case message::EDGE_CREATE_REQ:
                    if (requested_map.find(upd->elem1) == requested_map.end()) {
                        requested_map.emplace(upd->elem1, 0);
                        get_map.emplace_back(upd->elem1);
                    }
                    if (requested_map.find(upd->elem2) == requested_map.end()) {
                        requested_map.emplace(upd->elem2, 0);
                        get_map.emplace_back(upd->elem2);
                    }
                    break;

                case message::NODE_DELETE_REQ:
                case message::EDGE_DELETE_REQ:
                    if (requested_map.find(upd->elem1) == requested_map.end()) {
                        requested_map.emplace(upd->elem1, 0);
                        get_map.emplace_back(upd->elem1);
                    }
                    break;

                default:
                    DEBUG << "bad type" << std::endl;
            }
        }
        if (!get_map.empty()) {
            std::vector<int64_t> get_results = nmap_client.get_mappings(get_map);
            for (uint64_t i = 0; i < get_map.size(); i++) {
                uint64_t handle = get_map.at(i);
                assert(requested_map.find(handle) != requested_map.end());
                requested_map.at(handle) = (uint64_t)get_results.at(i);
            }
        }

        // insert mappings
        std::vector<std::pair<uint64_t, int64_t>> put_map;
        for (auto upd: tx.writes) {
            switch (upd->type) {
                case message::NODE_CREATE_REQ:
                    put_map.emplace_back(std::make_pair(upd->handle, (int64_t)upd->loc1));
                    break;

                case message::EDGE_CREATE_REQ:
                    assert(requested_map.find(upd->elem1) != requested_map.end());
                    assert(requested_map.find(upd->elem2) != requested_map.end());
                    upd->loc1 = requested_map.at(upd->elem1);
                    upd->loc2 = requested_map.at(upd->elem2);
                    put_map.emplace_back(std::make_pair(upd->handle, (int64_t)upd->loc1));
                    break;

                case message::NODE_DELETE_REQ:
                case message::EDGE_DELETE_REQ:
                    assert(requested_map.find(upd->elem1) != requested_map.end());
                    upd->loc1 = requested_map.at(upd->elem1);
                    break;

                default:
                    DEBUG << "bad type" << std::endl;
            }
        }
        nmap_client.put_mappings(put_map);
    }

    inline void
    timestamper :: clean_nmap_space()
    {
        nmap_client.clean_up_space();
    }

 }

#endif
