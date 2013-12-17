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
#include <bitset>
#include <unordered_map>
#include <unordered_set>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "common/busybee_infra.h"
#include "common/vclock.h"
#include "common/message.h"
#include "common/transaction.h"
#include "common/clock.h"
#include "common/nmap_stub.h"
#include "node_prog/triangle_program.h"

namespace coordinator
{
    struct tx_reply
    {
        uint64_t count;
        uint64_t client_id;
    };

    class prog_reply
    {
        public:
            uint64_t req_id;
            std::bitset<NUM_SHARDS> replied;

            prog_reply(uint64_t rid) : req_id(rid) { }

            bool operator==(const prog_reply &rp) const { return req_id == rp.req_id; }
    };
}

namespace std
{
    template <>
    struct hash<coordinator::prog_reply>
    {
        size_t operator()(const coordinator::prog_reply &pr) const
        {
            return std::hash<uint64_t>()(pr.req_id);
        }
    };
}

namespace coordinator
{
    class timestamper
    {
        public:
            uint64_t vt_id, shifted_id, id_gen; // this vector timestamper's id
            // timestamper state
            uint64_t loc_gen;
            vc::vclock vclk; // vector clock
            vc::qtimestamp_t qts; // queue timestamp
            std::unordered_map<uint64_t, tx_reply> tx_replies;
            timespec tspec;
            uint64_t nop_time_millis, nop_time_nanos, first_nop_time_millis, clock_update_acks, nop_acks;
            std::bitset<NUM_SHARDS> to_nop;
            std::vector<uint64_t> nop_last_qts;
            bool first_clock_update;
            // node prog
            // map from req_id to client_id that ensures a single response to a node program
            std::unordered_map<uint64_t, uint64_t> outstanding_node_progs;
            std::unordered_map<uint64_t, std::pair<int,node_prog::triangle_params>> outstanding_triangle_progs;
            std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>> outstanding_req_ids;
            std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>> done_req_ids;
            uint64_t max_done_id;
            std::unordered_map<node_prog::prog_type,
                    std::unordered_map<uint64_t, std::bitset<NUM_SHARDS>>> done_reqs;
            // node map client
            std::vector<nmap::nmap_stub*> nmap_client;
            // mutexes
            po6::threads::mutex mutex // big mutex for clock and timestamper DS
                    , loc_gen_mutex
                    , periodic_update_mutex // make sure to not send out clock update before getting ack from other VTs
                    , graph_load_mutex;
            po6::threads::cond periodic_cond;
            // initial graph loading
            uint32_t load_count;
            uint64_t max_load_time;
            // migration
            uint64_t migr_client;
            std::vector<uint64_t> shard_node_count;
            uint64_t msg_count, msg_count_acks;
            // permanent deletion
            // daemon
            // messaging
            std::shared_ptr<po6::net::location> myloc;
            busybee_mta *bb;

        public:
            timestamper(uint64_t id);
            busybee_returncode send(uint64_t shard_id, std::auto_ptr<e::buffer> buf);
            void unpack_tx(message::message &msg, transaction::pending_tx &tx, uint64_t client_id, int tid);
            //void clean_nmap_space();
            uint64_t generate_id();
    };

    inline
    timestamper :: timestamper(uint64_t id)
        : vt_id(id)
        , shifted_id(id << (64-ID_BITS))
        , id_gen(0)
        , loc_gen(0)
        , vclk(id, 0)
        , qts(NUM_SHARDS, 0)
        , clock_update_acks(NUM_VTS-1)
        , nop_acks(NUM_SHARDS)
        , nop_last_qts(NUM_SHARDS, 0)
        , first_clock_update(true)
        , max_done_id(0)
        , periodic_cond(&periodic_update_mutex)
        , load_count(0)
        , max_load_time(0)
        , shard_node_count(NUM_SHARDS, 0)
        , msg_count(0)
        , msg_count_acks(0)
    {
        // initialize array of server locations
        initialize_busybee(bb, vt_id, myloc, NUM_THREADS);
        //bb->set_timeout(VT_BB_TIMEOUT);
        nop_time_millis = wclock::get_time_elapsed_millis(tspec);
        nop_time_nanos = wclock::get_time_elapsed(tspec);
        first_nop_time_millis = nop_time_millis;
        // initialize empty vector of done reqs for each prog type
        std::unordered_map<uint64_t, std::bitset<NUM_SHARDS>> empty_map;
        done_reqs.emplace(node_prog::REACHABILITY, empty_map);
        done_reqs.emplace(node_prog::DIJKSTRA, empty_map);
        done_reqs.emplace(node_prog::CLUSTERING, empty_map);
        for (int i = 0; i < NUM_THREADS; i++) {
            nmap_client.push_back(new nmap::nmap_stub());
        }
        to_nop.set(); // set to_nop to 1 for each shard
    }

    inline busybee_returncode
    timestamper :: send(uint64_t shard_id, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        if ((ret = bb->send(shard_id, buf)) != BUSYBEE_SUCCESS) {
            WDEBUG << "message sending error: " << ret << std::endl;
        }
        return ret;
    }

    inline void
    timestamper :: unpack_tx(message::message &msg, transaction::pending_tx &tx, uint64_t client_id, int thread_id)
    {
        message::unpack_client_tx(msg, tx);
        tx.client_id = client_id;

        // lookup mappings
        std::unordered_map<uint64_t, uint64_t> mappings_to_put;
        std::unordered_set<uint64_t> mappings_to_get;
        for (auto upd: tx.writes) {
            switch (upd->type) {
                case transaction::NODE_CREATE_REQ:
                    // assign shard for this node
                    loc_gen_mutex.lock();
                    loc_gen = (loc_gen + 1) % NUM_SHARDS;
                    upd->loc1 = loc_gen + SHARD_ID_INCR; // node will be placed on this shard
                    loc_gen_mutex.unlock();
                    mappings_to_put.emplace(upd->handle, upd->loc1);
                    break;

                case transaction::EDGE_CREATE_REQ:
                    if (mappings_to_put.find(upd->elem1) == mappings_to_put.end()) {
                        mappings_to_get.insert(upd->elem1);
                    }
                    if (mappings_to_put.find(upd->elem2) == mappings_to_put.end()) {
                        mappings_to_get.insert(upd->elem2);
                    }
                    break;

                case transaction::NODE_DELETE_REQ:
                case transaction::NODE_SET_PROPERTY:
                    if (mappings_to_put.find(upd->elem1) == mappings_to_put.end()) {
                        mappings_to_get.insert(upd->elem1);
                    }
                    break;

                case transaction::EDGE_DELETE_REQ:
                case transaction::EDGE_SET_PROPERTY:
                    if (mappings_to_put.find(upd->elem2) == mappings_to_put.end()) {
                        mappings_to_get.insert(upd->elem2);
                    }
                    break;

                default:
                    WDEBUG << "bad type" << std::endl;
            }
        }

        // insert mappings
        nmap_client[thread_id]->put_mappings(mappings_to_put, true);

        // get mappings
        if (!mappings_to_get.empty()) {
            for (auto &toAdd: nmap_client[thread_id]->get_mappings(mappings_to_get, true)) {
                mappings_to_put.emplace(toAdd);
            }
        }

        // unpack get responses from hyperdex
        for (auto upd: tx.writes) {
            switch (upd->type) {

                case transaction::EDGE_CREATE_REQ:
                    assert(mappings_to_put.find(upd->elem1) != mappings_to_put.end());
                    assert(mappings_to_put.find(upd->elem2) != mappings_to_put.end());
                    upd->loc1 = mappings_to_put.at(upd->elem1);
                    upd->loc2 = mappings_to_put.at(upd->elem2);
                    break;

                case transaction::NODE_DELETE_REQ:
                case transaction::NODE_SET_PROPERTY:
                    assert(mappings_to_put.find(upd->elem1) != mappings_to_put.end());
                    upd->loc1 = mappings_to_put.at(upd->elem1);
                    break;

                case transaction::EDGE_DELETE_REQ:
                case transaction::EDGE_SET_PROPERTY:
                    assert(mappings_to_put.find(upd->elem2) != mappings_to_put.end());
                    upd->loc1 = mappings_to_put.at(upd->elem2);
                    break;

                default:
                    continue;
            }
        }
    }

    // caution: assuming caller holds mutex
    inline uint64_t
    timestamper :: generate_id()
    {
        uint64_t new_id;
        new_id = (++id_gen) & TOP_MASK;
        new_id |= shifted_id;
        return new_id;
    }

}

#endif
