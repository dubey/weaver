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

#ifndef weaver_coordinator_timestamper_h_
#define weaver_coordinator_timestamper_h_

#include <vector>
#include <bitset>
#include <unordered_map>
#include <unordered_set>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "common/ids.h"
#include "common/clock.h"
#include "common/vclock.h"
#include "common/message.h"
#include "common/transaction.h"
#include "common/configuration.h"
#include "common/comm_wrapper.h"
#include "common/server_manager_link_wrapper.h"
#include "common/nmap_stub.h"
#include "coordinator/current_tx.h"
#include "coordinator/current_prog.h"
#include "coordinator/hyper_stub.h"
//#include "node_prog/triangle_program.h"

namespace coordinator
{
    class timestamper
    {
        typedef std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>> req_queue_t;
        typedef std::unordered_map<uint64_t, std::bitset<NUM_SHARDS>> prog_reply_t;

        public:
            // messaging
            common::comm_wrapper comm;

            // server manager
            server_manager_link_wrapper sm_stub;
            configuration config;
            bool active_backup, first_config;
            po6::threads::cond backup_cond, first_config_cond;
            
            // Hyperdex stub
            std::vector<hyper_stub*> hstub;

            // timestamper state
            uint64_t vt_id, shifted_id, id_gen; // this vector timestamper's id
            server_id server;
            uint64_t loc_gen;

            // consistency
            vc::vclock vclk; // vector clock
            vc::qtimestamp_t qts; // queue timestamp
            uint64_t clock_update_acks;
            std::bitset<NUM_SHARDS> to_nop;

            // write transactions
            std::unordered_map<uint64_t, current_tx> outstanding_tx;

            // node prog
            std::unordered_map<uint64_t, current_prog> outstanding_progs;

            // prog cleanup and permanent deletion
            req_queue_t pend_prog_queue;
            req_queue_t done_prog_queue;
            uint64_t max_done_id; // permanent deletion of migrated nodes
            std::unique_ptr<vc::vclock_t> max_done_clk; // permanent deletion
            std::unordered_map<node_prog::prog_type, prog_reply_t> done_reqs; // prog state cleanup

            // node map client
            std::vector<nmap::nmap_stub*> nmap_client;

            // mutexes
        private:
            po6::threads::mutex loc_gen_mutex;
        public:
            po6::threads::mutex clk_mutex // vclock and queue timestamp
                    , tx_prog_mutex // state for outstanding and completed node progs, transactions
                    , periodic_update_mutex // make sure to not send out clock update before getting ack from other VTs
                    , id_gen_mutex
                    , msg_count_mutex
                    , migr_mutex
                    , graph_load_mutex
                    , config_mutex;

            // initial graph loading
            uint32_t load_count;
            uint64_t max_load_time;

            // migration
            uint64_t migr_client;
            std::vector<uint64_t> shard_node_count;
            uint64_t msg_count, msg_count_acks;

        public:
            timestamper(uint64_t vt, uint64_t server);
            void init();
            void restore_backup();
            void reconfigure();
            bool unpack_tx(message::message &msg, transaction::pending_tx &tx, uint64_t client_id, int tid);
            uint64_t generate_id();
    };

    inline
    timestamper :: timestamper(uint64_t vtid, uint64_t serverid)
        : comm(serverid, NUM_THREADS, -1, false)
        , sm_stub(server_id(serverid), comm.get_loc())
        , active_backup(false)
        , first_config(false)
        , backup_cond(&config_mutex)
        , first_config_cond(&config_mutex)
        , vt_id(vtid)
        , shifted_id(vtid << (64-ID_BITS))
        , id_gen(0)
        , server(serverid)
        , loc_gen(0)
        , vclk(vtid, 0)
        , qts(NUM_SHARDS, 0)
        , clock_update_acks(NUM_VTS-1)
        , max_done_id(0)
        , max_done_clk(new vc::vclock_t(NUM_VTS, 0))
        , load_count(0)
        , max_load_time(0)
        , shard_node_count(NUM_SHARDS, 0)
        , msg_count(0)
        , msg_count_acks(0)
    {
        // initialize empty vector of done reqs for each prog type
        std::unordered_map<uint64_t, std::bitset<NUM_SHARDS>> empty_map;
        done_reqs.emplace(node_prog::REACHABILITY, empty_map);
        done_reqs.emplace(node_prog::DIJKSTRA, empty_map);
        done_reqs.emplace(node_prog::CLUSTERING, empty_map);
        for (int i = 0; i < NUM_THREADS; i++) {
            nmap_client.push_back(new nmap::nmap_stub());
        }
        to_nop.set(); // set to_nop to 1 for each shard
        for (int i = 0; i < NUM_THREADS; i++) {
            hstub.push_back(new hyper_stub(vt_id));
        }
    }

    // initialize msging layer
    // caution: holding config_mutex
    inline void
    timestamper :: init()
    {
        comm.init(config);
    }

    // restore state when backup becomes primary due to failure
    inline void
    timestamper :: restore_backup()
    {
        std::unordered_map<uint64_t, transaction::pending_tx> txs;
        hstub.back()->restore_backup(txs);
        // TODO restore tx state
    }

    // reconfigure timestamper according to new cluster configuration
    inline void
    timestamper :: reconfigure()
    {
        WDEBUG << "Cluster reconfigure triggered\n";
        for (uint64_t i = 0; i < NUM_SERVERS; i++) {
            server::state_t st = config.get_state(server_id(i));
            if (st != server::AVAILABLE) {
                WDEBUG << "Server " << i << " is in trouble, has state " << st << std::endl;
            } else {
                WDEBUG << "Server " << i << " is healthy, has state " << st << std::endl;
            }
        }
        if (comm.reconfigure(config) == server.get()) {
            // this server is now primary for the shard
            active_backup = true;
            backup_cond.signal();
        }
    }

    // return false if the main node in the transaction is not found in node map
    // this is not a complete sanity check, e.g. create_edge(n1, n2) will succeed even if n2 is garbage
    // similarly edge ids are not checked
    inline bool
    timestamper :: unpack_tx(message::message &msg, transaction::pending_tx &tx, uint64_t client_id, int thread_id)
    {
        message::unpack_client_tx(msg, tx);
        tx.id = generate_id();
        //hstub[thread_id]->put_tx(tx.id, msg);
        tx.client_id = client_id;

        // lookup mappings
        std::unordered_map<uint64_t, uint64_t> mappings_to_put;
        std::unordered_set<uint64_t> mappings_to_get;
        for (auto upd: tx.writes) {
            switch (upd->type) {
                case transaction::NODE_CREATE_REQ:
                    // randomly assign shard for this node
                    loc_gen_mutex.lock();
                    loc_gen = (loc_gen + 1) % NUM_SHARDS;
                    upd->loc1 = loc_gen + SHARD_ID_INCR; // node will be placed on this shard
                    loc_gen_mutex.unlock();
                    mappings_to_put.emplace(upd->id, upd->loc1);
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
        nmap_client[thread_id]->put_mappings(mappings_to_put);

        // get mappings
        if (!mappings_to_get.empty()) {
            for (auto &toAdd: nmap_client[thread_id]->get_mappings(mappings_to_get)) {
                mappings_to_put.emplace(toAdd);
            }
        }

        // unpack get responses from hyperdex
        for (auto upd: tx.writes) {
            switch (upd->type) {

                case transaction::EDGE_CREATE_REQ:
                    if (mappings_to_put.find(upd->elem1) == mappings_to_put.end()) {
                        return false;
                    }
                    if (mappings_to_put.find(upd->elem2) == mappings_to_put.end()) {
                        return false;
                    }
                    upd->loc1 = mappings_to_put.at(upd->elem1);
                    upd->loc2 = mappings_to_put.at(upd->elem2);
                    break;

                case transaction::NODE_DELETE_REQ:
                case transaction::NODE_SET_PROPERTY:
                    if (mappings_to_put.find(upd->elem1) == mappings_to_put.end()) {
                        return false;
                    }
                    upd->loc1 = mappings_to_put.at(upd->elem1);
                    break;

                case transaction::EDGE_DELETE_REQ:
                case transaction::EDGE_SET_PROPERTY:
                    if (mappings_to_put.find(upd->elem2) == mappings_to_put.end()) {
                        return false;
                    }
                    upd->loc1 = mappings_to_put.at(upd->elem2);
                    break;

                default:
                    continue;
            }
        }
        return true;
    }

    inline uint64_t
    timestamper :: generate_id()
    {
        uint64_t new_id;
        id_gen_mutex.lock();
        new_id = (++id_gen) & TOP_MASK;
        id_gen_mutex.unlock();
        new_id |= shifted_id;
        return new_id;
    }

}

#endif
