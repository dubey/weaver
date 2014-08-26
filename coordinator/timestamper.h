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
#define VT_TIMEOUT_NANO 1000000 // number of nanoseconds between successive nops
#define VT_CLK_TIMEOUT_NANO 1000000 // number of nanoseconds between vt gossip

#include <vector>
#include <unordered_map>
#include <po6/threads/mutex.h>
#include <po6/threads/rwlock.h>
#include <po6/threads/cond.h>

#include "common/ids.h"
#include "common/vclock.h"
#include "common/message.h"
#include "common/configuration.h"
#include "common/comm_wrapper.h"
#include "common/server_manager_link_wrapper.h"
#include "coordinator/current_tx.h"
#include "coordinator/current_prog.h"
#include "coordinator/hyper_stub.h"

namespace coordinator
{
    using transaction::upd_ptr_t;
    using transaction::nop_ptr_t;
    using transaction::tx_ptr_t;
    class greater_tx_ptr
    {
        public:
            bool operator() (const tx_ptr_t &lhs, const tx_ptr_t &rhs)
            {
                return lhs->timestamp.get_clock() > rhs->timestamp.get_clock();
            }
    };
    using req_queue_t = std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>>;
    using tx_queue_t = std::priority_queue<tx_ptr_t, std::vector<tx_ptr_t>, greater_tx_ptr>;
    using prog_reply_t = std::unordered_map<uint64_t, std::vector<bool>>;

    class timestamper
    {
        public:
            // messaging
            common::comm_wrapper comm;

            // server manager
            server_manager_link_wrapper sm_stub;
            configuration config;
            bool active_backup, first_config, vts_init;
            uint64_t num_active_vts;
            po6::threads::cond backup_cond, first_config_cond, vts_init_cond, start_all_vts_cond;
            
            // Hyperdex stub
            std::vector<hyper_stub*> hstub;

            // timestamper state
        public:
            server_id server;
        private:
            uint64_t vt_id; // this vector timestamper's id
            uint64_t shifted_id;
            uint64_t reqid_gen, loc_gen;
            po6::threads::mutex reqid_gen_mtx, loc_gen_mtx;

        public:
            // consistency
            vc::vclock vclk; // vector clock
            vc::qtimestamp_t qts; // queue timestamp
            uint64_t clock_update_acks, clk_updates;
            std::vector<bool> to_nop;
            std::vector<uint64_t> nop_ack_qts;

            // transactions
            std::unordered_map<uint64_t, current_tx> outstanding_tx;

            // node prog
            std::unordered_map<uint64_t, current_prog> outstanding_progs;

            // prog cleanup and permanent deletion
            req_queue_t pend_prog_queue;
            req_queue_t done_prog_queue;
            uint64_t max_done_id; // permanent deletion of migrated nodes
            std::unique_ptr<vc::vclock_t> max_done_clk; // permanent deletion
            std::unordered_map<node_prog::prog_type, prog_reply_t> done_reqs; // prog state cleanup

            // mutexes
        public:
            po6::threads::mutex clk_mutex // vclock and queue timestamp
                    , tx_prog_mutex // state for outstanding and completed node progs, transactions
                    , periodic_update_mutex // make sure to not send out clock update before getting ack from other VTs
                    , migr_mutex
                    , graph_load_mutex
                    , config_mutex
                    , exit_mutex
                    , tx_out_queue_mtx;
            po6::threads::rwlock clk_rw_mtx;

            // initial graph loading
            uint32_t load_count;
            uint64_t max_load_time;

            // migration
            uint64_t migr_client;
            std::vector<uint64_t> shard_node_count;

            // fault tolerance
            uint64_t out_queue_clk;
            tx_queue_t tx_out_queue;

            // exit
            bool to_exit;

        public:
            timestamper(uint64_t serverid, po6::net::location &loc);
            void init(uint64_t vtid);
            void init_hstub();
            void restore_backup();
            void reconfigure();
            void update_members_new_config();
            uint64_t generate_req_id();
            uint64_t generate_loc();
            void enqueue_tx(tx_ptr_t tx);
            std::pair<tx_ptr_t, std::vector<tx_ptr_t>> process_tx_queue();
    };

    inline
    timestamper :: timestamper(uint64_t serverid, po6::net::location &loc)
        : comm(loc, NUM_THREADS, -1)
        , sm_stub(server_id(serverid), comm.get_loc())
        , active_backup(false)
        , first_config(false)
        , vts_init(false)
        , num_active_vts(0)
        , backup_cond(&config_mutex)
        , first_config_cond(&config_mutex)
        , vts_init_cond(&config_mutex)
        , start_all_vts_cond(&config_mutex)
        , server(serverid)
        , vt_id(UINT64_MAX)
        , shifted_id(UINT64_MAX)
        , reqid_gen(0)
        , loc_gen(0)
        , vclk(UINT64_MAX, 0)
        , qts(NumShards, 0)
        , clock_update_acks(NumVts-1)
        , clk_updates(0)
        , to_nop(NumShards, true)
        , nop_ack_qts(NumShards, 0)
        , max_done_id(0)
        , max_done_clk(new vc::vclock_t(NumVts, 0))
        , load_count(0)
        , max_load_time(0)
        , shard_node_count(NumShards, 0)
        , to_exit(false)
    {
        // initialize empty vector of done reqs for each prog type
        std::unordered_map<uint64_t, std::vector<bool>> empty_map;
        done_reqs.emplace(node_prog::REACHABILITY, empty_map);
        done_reqs.emplace(node_prog::DIJKSTRA, empty_map);
        done_reqs.emplace(node_prog::CLUSTERING, empty_map);
    }

    // initialize msging layer
    // caution: holding config_mutex
    inline void
    timestamper :: init(uint64_t vtid)
    {
        vt_id = vtid;
        shifted_id = vt_id << (64-ID_BITS);
        vclk.vt_id = vt_id;
        comm.init(config);
        update_members_new_config();
    }

    inline void
    timestamper :: init_hstub()
    {
        for (int i = 0; i < NUM_THREADS; i++) {
            hstub.push_back(new hyper_stub(vt_id));
        }
    }

    // restore state when backup becomes primary due to failure
    inline void
    timestamper :: restore_backup()
    {
        std::unordered_map<uint64_t, current_tx> prepare_txs;
        //hstub.back()->restore_backup(prepare_txs, outstanding_tx);
        WDEBUG << "num prep " << prepare_txs.size()
               << ", outst txs " << outstanding_tx.size() << std::endl;
        // TODO learn qts, clk values from shards
        // TODO restore tx state, reexec prepared txs
    }

    // reconfigure timestamper according to new cluster configuration
    inline void
    timestamper :: reconfigure()
    {
        WDEBUG << "Cluster reconfigure triggered\n";

        uint64_t prev_active_vts = num_active_vts;
        comm.reconfigure(config, &num_active_vts);
        update_members_new_config();
        //if (comm.reconfigure(config, &num_active_vts) == server.get()
        // && !active_backup
        // && server.get() > NumEffectiveServers) {
        //    // this server is now primary for the timestamper 
        //    active_backup = true;
        //    backup_cond.signal();
        //}
        assert(prev_active_vts <= num_active_vts);
        assert(num_active_vts <= NumVts);
        if (num_active_vts > prev_active_vts) {
            WDEBUG << "#active vts = " << num_active_vts << std::endl;
            start_all_vts_cond.signal();
        }

        // resend unacked transactions and nop for new shard
        // TODO flesh out
        //uint64_t max_qts = 0;
        //bool pending_tx = false;
        //if (changed >= ShardIdIncr && changed < (ShardIdIncr+NumShards)) {
        //    // transactions
        //    uint64_t sid = changed - ShardIdIncr;
        //    for (auto &entry: outstanding_tx) {
        //        std::vector<transaction::pending_tx> &tv = *entry.second.tx_vec;
        //        if (!tv[sid].writes.empty()) {
        //            pending_tx = true;
        //            // TODO resend transactions
        //            if (tv[sid].writes.back()->qts[sid] > max_qts) {
        //                max_qts = tv[sid].writes.back()->qts[sid];
        //            }
        //        }
        //    }
        //    assert(qts[sid] >= max_qts);

        //    // nop
        //    if (!to_nop[sid]) {
        //        nop_ack_qts[sid] = qts[sid]; // artificially 'ack' old nop
        //        to_nop.set(sid);
        //        WDEBUG << "resetting to_nop for shard " << changed << std::endl;
        //    }

        //    if (!pending_tx) {
        //        // only nop was pending, if any
        //        // send msg to advance clock
        //        message::message msg;
        //        msg.prepare_message(message::SET_QTS, vt_id, qts[sid]);
        //        comm.send(changed, msg.buf);
        //        WDEBUG << "sent set qts msg to shard " << changed << std::endl;
        //    }
        //}
    }

    inline void
    timestamper :: update_members_new_config()
    {
        std::vector<std::pair<server_id, po6::net::location>> addresses;
        config.get_all_addresses(&addresses);

        uint64_t num_shards = 0;
        for (auto &p: addresses) {
            if (config.get_type(p.first) == server::SHARD) {
                num_shards++;
            }
        }

        periodic_update_mutex.lock();
        to_nop.resize(num_shards, true);
        nop_ack_qts.resize(num_shards, 0);
        shard_node_count.resize(num_shards, 0);
        periodic_update_mutex.unlock();

        clk_rw_mtx.wrlock();
        qts.resize(num_shards, 0);
        clk_rw_mtx.unlock();

        update_config_constants(num_shards);
    }

    inline uint64_t
    timestamper :: generate_req_id()
    {
        uint64_t new_id;
        reqid_gen_mtx.lock();
        new_id = (++reqid_gen) & TOP_MASK;
        reqid_gen_mtx.unlock();
        new_id |= shifted_id;
        return new_id;
    }

    inline uint64_t
    timestamper :: generate_loc()
    {
        uint64_t new_loc;
        uint64_t num_shards = get_num_shards();
        loc_gen_mtx.lock();
        loc_gen = (loc_gen+1) % num_shards;
        new_loc = loc_gen + ShardIdIncr;
        loc_gen_mtx.unlock();
        return new_loc;
    }

    inline void
    timestamper :: enqueue_tx(tx_ptr_t tx)
    {
        uint64_t seq = tx->timestamp.get_clock();
        tx_out_queue_mtx.lock();
        bool to_add = (!tx_out_queue.empty()) // queue not empty, have to add
                   || (seq != out_queue_clk) // not the next expected clk
                   || (seq == out_queue_clk && tx->type != transaction::FAIL); // next expected clk with non-empty tx, handled by subsequent process_tx_queue()
        if (to_add) {
            tx_out_queue.emplace(tx);
        } else {
            out_queue_clk++;
        }
        tx_out_queue_mtx.unlock();
    }

    // return list of tx components, factored per shard
    inline std::pair<tx_ptr_t, std::vector<tx_ptr_t>>
    timestamper :: process_tx_queue()
    {
        tx_ptr_t tx = NULL;

        tx_out_queue_mtx.lock();

        if (!tx_out_queue.empty()) {
            tx_ptr_t top = tx_out_queue.top();
            if (top->timestamp.get_clock() == out_queue_clk) {
                if (top->type != transaction::FAIL) {
                    tx = top;
                }
                out_queue_clk++;
                tx_out_queue.pop();
            }
        }

        std::vector<tx_ptr_t> factored_tx;
        if (tx != NULL) {
            assert(tx->type != transaction::FAIL);

            uint64_t num_shards = tx->shard_write.size();
            factored_tx.reserve(num_shards);
            for (uint64_t i = 0; i < num_shards; i++) {
                tx_ptr_t new_tx(new transaction::pending_tx(tx->type));
                factored_tx.emplace_back(new_tx);
                new_tx->id = tx->id;
                new_tx->timestamp = tx->timestamp;
            }

            if (tx->type == transaction::NOP) {
                // nop
                nop_ptr_t nop = tx->nop;
                for (uint64_t i = 0; i < num_shards; i++) {
                    tx_ptr_t this_tx = factored_tx[i];
                    if (tx->shard_write[i]) {
                        this_tx->qts = qts[i]++;
                        this_tx->nop.reset(new transaction::nop_data());
                        nop_ptr_t this_nop = this_tx->nop;
                        this_nop->max_done_id = nop->max_done_id;
                        this_nop->max_done_clk = nop->max_done_clk;
                        this_nop->outstanding_progs = nop->outstanding_progs;
                        this_nop->done_reqs[i] = nop->done_reqs[i];
                        this_nop->shard_node_count = nop->shard_node_count;
                    }
                }
            } else {
                // write tx
                for (upd_ptr_t upd: tx->writes) {
                    uint64_t shard_idx = upd->loc1-ShardIdIncr;
                    tx_ptr_t this_tx = factored_tx[shard_idx];
                    if (this_tx->writes.empty()) {
                        this_tx->qts = qts[shard_idx]++;
                    }
                    this_tx->writes.emplace_back(upd);
                }
            }

        }

        tx_out_queue_mtx.unlock();

        return std::make_pair(tx, factored_tx);
    }
}

#endif
