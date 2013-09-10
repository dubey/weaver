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
#include "common/message_tx_coord.h"
#include "transaction.h"
#include "nmap_stub.h"

namespace coordinator
{
    struct tx_reply
    {
        uint64_t count;
        uint64_t client_id;
    };

    class timestamper
    {
        public:
            uint64_t vt_id, shifted_id, id_gen; // this vector timestamper's id
            // messaging
            std::shared_ptr<po6::net::location> myloc;
            busybee_mta *bb;
            // timestamper state
            uint64_t loc_gen, handle_gen;
            vc::vclock vclk; // vector clock
            vc::qtimestamp_t qts; // queue timestamp
            std::unordered_map<uint64_t, tx_reply> tx_replies;
            // node map client
            coordinator::nmap_stub nmap_client;
            // mutexes
            po6::threads::mutex mutex, loc_gen_mutex, id_gen_mutex;
            // migration
            // permanent deletion
            // daemon

        public:
            timestamper(uint64_t id);
            busybee_returncode send(uint64_t shard_id, std::auto_ptr<e::buffer> buf);
            void unpack_tx(message::message &msg, coordinator::pending_tx &tx, uint64_t client_id);
            void clean_nmap_space();
            uint64_t generate_id();
    };

    inline
    timestamper :: timestamper(uint64_t id)
        : vt_id(id)
        , shifted_id(id << (64-ID_BITS))
        , id_gen(0)
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
    timestamper :: unpack_tx(message::message &msg, pending_tx &tx, uint64_t client_id)
    {
        message::unpack_client_tx(msg, tx);
        tx.client_id = client_id;

        // lookup mappings
        std::unordered_map<uint64_t, uint64_t> request_element_mappings;
        std::unordered_set<uint64_t> mappings_to_get;
        for (auto upd: tx.writes) {
            switch (upd->type) {
                case message::NODE_CREATE_REQ:
                    // assign shard for this node
                    loc_gen_mutex.lock();
                    loc_gen = (loc_gen + 1) % NUM_SHARDS;
                    upd->loc1 = loc_gen + SHARD_ID_INCR; // node will be placed on this shard
                    loc_gen_mutex.unlock();
                    request_element_mappings.emplace(upd->handle, upd->loc1);
                    break;

                case message::EDGE_CREATE_REQ:
                    if (request_element_mappings.find(upd->elem1) == request_element_mappings.end()) {
                        //request_element_mappings.emplace(upd->elem1, 0);
                        mappings_to_get.insert(upd->elem1);
                    }
                    if (request_element_mappings.find(upd->elem2) == request_element_mappings.end()) {
                        //request_element_mappings.emplace(upd->elem2, 0);
                        mappings_to_get.insert(upd->elem2);
                    }
                    break;

                case message::NODE_DELETE_REQ:
                case message::EDGE_DELETE_REQ:
                    if (request_element_mappings.find(upd->elem1) == request_element_mappings.end()) {
                        //request_element_mappings.emplace(upd->elem1, 0);
                        mappings_to_get.insert(upd->elem1);
                    }
                    break;

                default:
                    DEBUG << "bad type" << std::endl;
            }
        }
        if (!mappings_to_get.empty()) {
            nmap_client.get_mappings(mappings_to_get, request_element_mappings);
        }

        // insert mappings
        std::vector<std::pair<uint64_t, uint64_t>> put_map;
        for (auto upd: tx.writes) {
            switch (upd->type) {
                case message::NODE_CREATE_REQ:
                    put_map.emplace_back(std::make_pair(upd->handle, (int64_t)upd->loc1));
                    break;

                case message::EDGE_CREATE_REQ:
                    assert(request_element_mappings.find(upd->elem1) != request_element_mappings.end());
                    assert(request_element_mappings.find(upd->elem2) != request_element_mappings.end());
                    upd->loc1 = request_element_mappings.at(upd->elem1);
                    upd->loc2 = request_element_mappings.at(upd->elem2);
                    put_map.emplace_back(std::make_pair(upd->handle, (int64_t)upd->loc1));
                    break;

                case message::NODE_DELETE_REQ:
                case message::EDGE_DELETE_REQ:
                    assert(request_element_mappings.find(upd->elem1) != request_element_mappings.end());
                    upd->loc1 = request_element_mappings.at(upd->elem1);
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

    inline uint64_t
    timestamper :: generate_id()
    {
        uint64_t new_id;
        id_gen_mutex.lock();
        new_id = (++id_gen) >> ID_BITS;
        new_id |= shifted_id;
        id_gen_mutex.unlock();
        return new_id;
    }

 }

#endif
