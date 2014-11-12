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
#include <unordered_map>
#include <po6/threads/mutex.h>
#include <po6/threads/rwlock.h>
#include <po6/threads/cond.h>

#include "common/ids.h"
#include "common/vclock.h"
#include "common/message.h"
#include "common/configuration.h"
#include "common/comm_wrapper.h"
#include "common/event_order.h"
#include "common/server_manager_link_wrapper.h"
#include "coordinator/vt_constants.h"
#include "coordinator/current_prog.h"
#include "coordinator/blocked_prog.h"
#include "coordinator/hyper_stub.h"

namespace coordinator
{
    class greater_tx_ptr
    {
        public:
            bool operator() (const std::shared_ptr<const transaction::pending_tx> lhs, const std::shared_ptr<const transaction::pending_tx> rhs)
            {
                if (lhs->timestamp.get_epoch() == rhs->timestamp.get_epoch()) {
                    return lhs->vt_seq > rhs->vt_seq;
                } else {
                    return lhs->timestamp.get_epoch() > rhs->timestamp.get_epoch();
                }
            }
    };

    using tx_queue_t = std::priority_queue<std::shared_ptr<transaction::pending_tx>, std::vector<std::shared_ptr<transaction::pending_tx>>, greater_tx_ptr>;
    using prog_queue_t = std::vector<std::unique_ptr<blocked_prog>>;
    using prog_reply_t = std::unordered_map<uint64_t, std::vector<bool>>;

    class timestamper
    {
        public:
            // messaging
            common::comm_wrapper comm;

            // server manager
            server_manager_link_wrapper sm_stub;
            configuration config, prev_config, periodic_update_config;
            bool is_backup, active_backup, first_config, pause_bb;
            uint64_t num_active_vts;
            po6::threads::cond backup_cond, first_config_cond, start_all_vts_cond;

            // Hyperdex stub
            std::vector<hyper_stub*> hstub, hstub_uninit;

            // time oracle
            std::vector<order::oracle*> time_oracles, time_oracles_uninit;

            // timestamper state
        public:
            server_id serv_id;
            uint64_t weaver_id; // unique server id that counts up from 0, assigned by server manager
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
            std::unordered_map<uint64_t, std::shared_ptr<transaction::pending_tx>> outstanding_tx;

            // node prog
            //std::unordered_map<uint64_t, current_prog> outstanding_progs;

            // prog cleanup and permanent deletion
            std::vector<current_prog*> pend_progs, done_progs;
            int prog_done_cnt;
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
            std::pair<uint64_t, uint64_t> out_queue_clk; // (epoch num, out clk)
            uint64_t out_queue_counter;
            tx_queue_t tx_out_queue;
            prog_queue_t prog_queue;
            po6::threads::rwlock restore_mtx;
            uint16_t restore_status;

            // exit
            bool to_exit;

        public:
            timestamper(uint64_t serverid, po6::net::location &loc, bool backup);
            void init(uint64_t vtid, uint64_t weaverid);
            void restore_backup();
            void reconfigure();
            void update_members_new_config();
            uint64_t generate_req_id();
            uint64_t generate_loc();
            void enqueue_tx(std::shared_ptr<transaction::pending_tx> tx);
            bool process_tx_queue(std::shared_ptr<transaction::pending_tx> &tx_ptr, std::vector<std::shared_ptr<transaction::pending_tx>> &factored_tx);
            void factor_tx(std::shared_ptr<transaction::pending_tx> tx, std::vector<std::shared_ptr<transaction::pending_tx>> &factored_tx);
            void tx_queue_loop();
            void reset_out_queue_clk(uint64_t epoch);

#ifdef weaver_benchmark_
        public:
            po6::threads::mutex test_mtx;
            int outstanding_cnt, max_outstanding_cnt;
#endif
    };

    inline
    timestamper :: timestamper(uint64_t serverid, po6::net::location &loc, bool backup)
        : comm(loc, NUM_VT_THREADS, -1)
        , sm_stub(server_id(serverid), comm.get_loc())
        , is_backup(backup)
        , active_backup(false)
        , first_config(false)
        , pause_bb(false)
        , num_active_vts(0)
        , backup_cond(&config_mutex)
        , first_config_cond(&config_mutex)
        , start_all_vts_cond(&config_mutex)
        , serv_id(serverid)
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
        , prog_done_cnt(0)
        , max_done_id(0)
        , max_done_clk(new vc::vclock_t(ClkSz, 0))
        , load_count(0)
        , max_load_time(0)
        , shard_node_count(NumShards, 0)
        , out_queue_clk(std::make_pair(0,1))
        , out_queue_counter(0)
        , to_exit(false)
#ifdef weaver_benchmark_
        , outstanding_cnt(0)
        , max_outstanding_cnt(0)
#endif
    {
        // initialize empty vector of done reqs for each prog type
        std::unordered_map<uint64_t, std::vector<bool>> empty_map;
        done_reqs.emplace(node_prog::REACHABILITY, empty_map);
        done_reqs.emplace(node_prog::DIJKSTRA, empty_map);
        done_reqs.emplace(node_prog::CLUSTERING, empty_map);

        for (int i = 0; i < NUM_VT_THREADS; i++) {
            hstub_uninit.push_back(new hyper_stub());
            time_oracles_uninit.push_back(new order::oracle());
        }
    }

    // initialize msging layer
    // caution: holding config_mutex
    inline void
    timestamper :: init(uint64_t vtid, uint64_t weaverid)
    {
        vt_id = vtid;
        weaver_id = weaverid;
        shifted_id = weaver_id << (64-ID_BITS);
        vclk.vt_id = vt_id;

        hstub = std::move(hstub_uninit);
        for (hyper_stub *hs: hstub) {
            hs->init(vt_id);
        }
        time_oracles = std::move(time_oracles_uninit);
    }

    // restore state when backup becomes primary due to failure
    inline void
    timestamper :: restore_backup()
    {
        std::vector<std::shared_ptr<transaction::pending_tx>> txs;
        hstub.back()->restore_backup(txs);

        greater_tx_ptr comp_obj;
        std::sort(txs.begin(), txs.end(), comp_obj);

        for (std::shared_ptr<transaction::pending_tx> tx: txs) {
            outstanding_tx.emplace(tx->id, tx);

            message::message msg;
            std::vector<std::shared_ptr<transaction::pending_tx>> factored_tx;
            factor_tx(tx, factored_tx);

            for (uint64_t i = 0; i < tx->shard_write.size(); i++) {
                if (tx->shard_write[i]) {
                    msg.prepare_message(message::TX_INIT, vt_id, factored_tx[i]->timestamp, factored_tx[i]->qts, *factored_tx[i]);
                    comm.send(i+ShardIdIncr, msg.buf);
                }
            }
        }
    }

    // reconfigure timestamper according to new cluster configuration
    inline void
    timestamper :: reconfigure()
    {
        WDEBUG << "Cluster reconfigure triggered\n";

        uint64_t prev_active_vts = num_active_vts;
        comm.reconfigure(config, pause_bb, &num_active_vts);
        assert(prev_active_vts <= num_active_vts);
        assert(num_active_vts <= NumVts);
        if (num_active_vts > prev_active_vts) {
            WDEBUG << "#active vts = " << num_active_vts << std::endl;
            start_all_vts_cond.signal();
        }

        update_members_new_config();
    }

    inline void
    timestamper :: update_members_new_config()
    {
        std::vector<server> servers = config.get_servers();

        // update num shards
        std::unordered_set<uint64_t> shard_set;
        for (const server &srv: servers) {
            if (srv.type == server::SHARD
             && srv.state != server::ASSIGNED) {
                shard_set.emplace(srv.virtual_id);
            }
        }
        uint64_t num_shards = shard_set.size();
        bool direct_reset_out_queue_clk = (vt_id == UINT64_MAX);

        // resize periodic msg ds
        periodic_update_mutex.lock();
        to_nop.resize(num_shards, true);
        nop_ack_qts.resize(num_shards, 0);
        shard_node_count.resize(num_shards, 0);
        std::fill(max_done_clk->begin(), max_done_clk->end(), 0);
        max_done_clk->at(0) = config.version(); 

        // update the periodic_update_config which is used while sending periodic vt updates
        periodic_update_config = config;

        // update config constants
        update_config_constants(num_shards);

        // activate if backup
        uint64_t vid = config.get_virtual_id(serv_id);
        if (is_backup && !active_backup) {
            if (vid != UINT64_MAX) {
                active_backup = true;
                backup_cond.signal();
            }
            direct_reset_out_queue_clk = true;
        }

        clk_rw_mtx.wrlock();

        // resize qts
        qts.resize(num_shards, 0);

        std::shared_ptr<transaction::pending_tx> epoch_tx;
        if (direct_reset_out_queue_clk) {
            // out_queue is empty
            reset_out_queue_clk(config.version());
            vclk.new_epoch(config.version());
            WDEBUG << "first config, out_queue_clk is now " << out_queue_clk.first << "," << out_queue_clk.second << std::endl;
        } else {
            // restart vclock with new epoch number from configuration
            vclk.increment_clock();
            out_queue_counter++;
            epoch_tx = std::make_shared<transaction::pending_tx>(transaction::EPOCH_CHANGE);
            epoch_tx->timestamp = vclk;
            epoch_tx->vt_seq = out_queue_counter;
            epoch_tx->new_epoch = config.version();
            vclk.new_epoch(config.version());
        }

#ifdef weaver_benchmark_
        // kill if server died
        std::vector<server> delta = prev_config.delta(config);
        for (const server &srv: delta) {
            if (srv.type == server::SHARD || srv.type == server::VT) {
                server::state_t prev_state = prev_config.get_state(srv.id);
                if ((prev_state == server::AVAILABLE || prev_state == server::ASSIGNED)
                 && (srv.state != server::AVAILABLE && srv.state != server::ASSIGNED)) {
                    WDEBUG << server::to_string(srv.type) << " " << srv.virtual_id << " died, exiting now" << std::endl;
                    exit(-1);
                }
            }
        }
#endif

        // reset qts if a shard died
        std::vector<server> delta = prev_config.delta(config);
        for (const server &srv: delta) {
            if (srv.type == server::SHARD) {
                bool to_reset = false;
                bool restore = false;
                server::state_t prv_state = prev_config.get_state(srv.id);
                server::type_t prv_type = prev_config.get_type(srv.id);

                if (prv_state == server::ASSIGNED
                 && srv.state == server::AVAILABLE) {
                    // conservatively reset whenever a shard transitions from
                    // assigned to available
                    to_reset = true;
                } else if (prv_type == server::BACKUP_SHARD) {
                    // reset if server type changes from backup_shard to shard
                    to_reset = true;
                    restore = true;
                }

                if (to_reset) {
                    uint64_t shard_id = srv.virtual_id;
                    // reset qts for shard_id
                    qts[shard_id] = 0;
                    to_nop[shard_id] = true;
                    nop_ack_qts[shard_id] = 0;
                    WDEBUG << "reset qts for shard " << (shard_id+ShardIdIncr) << std::endl;
                }

                if (restore) {
                    restore_mtx.wrlock();
                    restore_status++;
                    restore_mtx.unlock();
                }
            }
        }

        clk_rw_mtx.unlock();
        periodic_update_mutex.unlock();

        if (!direct_reset_out_queue_clk) {
            enqueue_tx(epoch_tx);
            tx_queue_loop();
        }
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
    timestamper :: enqueue_tx(std::shared_ptr<transaction::pending_tx> tx)
    {
        uint64_t seq = tx->vt_seq;
        uint64_t epoch = tx->timestamp.get_epoch();
        bool done = false;

        tx_out_queue_mtx.lock();

        if (tx_out_queue.empty()
         && epoch == out_queue_clk.first
         && seq == out_queue_clk.second) {
            if (tx->type == transaction::FAIL) {
                out_queue_clk.second++;
                done = true;
            } else if (tx->type == transaction::EPOCH_CHANGE) {
                reset_out_queue_clk(tx->new_epoch);
                done = true;
            }
        }

        if (!done) {
            tx_out_queue.emplace(tx);
        }

        tx_out_queue_mtx.unlock();
    }

    // return list of tx components, factored per shard
    // return true if this call did pop off out queue
    inline bool
    timestamper :: process_tx_queue(std::shared_ptr<transaction::pending_tx> &tx_ptr, std::vector<std::shared_ptr<transaction::pending_tx>> &factored_tx)
    {
        tx_ptr = nullptr;
        bool ret = false;

        tx_out_queue_mtx.lock();

        if (!tx_out_queue.empty()) {
            std::shared_ptr<transaction::pending_tx> top = tx_out_queue.top();
            uint64_t this_clk = top->vt_seq;
            uint64_t this_epoch = top->timestamp.get_epoch();

            if (this_epoch == out_queue_clk.first && this_clk == out_queue_clk.second) {
                tx_out_queue.pop();
                ret = true;

                if (top->type == transaction::EPOCH_CHANGE) {
                    reset_out_queue_clk(top->new_epoch);
                } else {
                    out_queue_clk.second++;
                    if (top->type != transaction::FAIL) {
                        tx_ptr = top;
                    }
                }
            }
        }

        if (tx_ptr != nullptr) {
            factor_tx(tx_ptr, factored_tx);
        }

        tx_out_queue_mtx.unlock();

        return ret;
    }

    inline void
    timestamper :: factor_tx(std::shared_ptr<transaction::pending_tx> tx, std::vector<std::shared_ptr<transaction::pending_tx>> &factored_tx)
    {
        assert(tx != nullptr);
        assert(tx->type != transaction::FAIL);

        uint64_t num_shards = tx->shard_write.size();
        factored_tx.reserve(num_shards);
        for (uint64_t i = 0; i < num_shards; i++) {
            std::shared_ptr<transaction::pending_tx> new_tx = std::make_shared<transaction::pending_tx>(tx->type);
            factored_tx.emplace_back(new_tx);
            new_tx->id = tx->id;
            new_tx->timestamp = tx->timestamp;
        }

        if (tx->type == transaction::NOP) {
            // nop
            std::shared_ptr<transaction::nop_data> nop = tx->nop;
            assert(nop != nullptr);
            for (uint64_t i = 0; i < num_shards; i++) {
                std::shared_ptr<transaction::pending_tx> this_tx = factored_tx[i];
                if (tx->shard_write[i]) {
                    this_tx->qts = ++qts[i];
                    this_tx->nop = std::make_shared<transaction::nop_data>();
                    std::shared_ptr<transaction::nop_data> this_nop = this_tx->nop;
                    this_nop->max_done_id = nop->max_done_id;
                    this_nop->max_done_clk = nop->max_done_clk;
                    this_nop->outstanding_progs = nop->outstanding_progs;
                    this_nop->done_reqs[i] = nop->done_reqs[i];
                    this_nop->shard_node_count = nop->shard_node_count;
                }
            }
        } else {
            // write tx
            for (std::shared_ptr<transaction::pending_update> upd: tx->writes) {
                uint64_t shard_idx = upd->loc1-ShardIdIncr;
                std::shared_ptr<transaction::pending_tx> this_tx = factored_tx[shard_idx];
                if (this_tx->writes.empty()) {
                    this_tx->qts = ++qts[shard_idx];
                }
                this_tx->writes.emplace_back(upd);
            }
        }
    }

    void
    timestamper :: tx_queue_loop()
    {
        restore_mtx.rdlock();
        if (restore_status > 0) {
            restore_mtx.unlock();
            return;
        } else {
            restore_mtx.unlock();
        }

        while (true) {
            std::shared_ptr<transaction::pending_tx> tx;
            std::vector<std::shared_ptr<transaction::pending_tx>> factored_tx;
            if (!process_tx_queue(tx, factored_tx)) {
                assert(tx == nullptr);
                break;
            } else if (tx == nullptr) {
                continue;
            }

            assert(tx->type != transaction::FAIL && tx->type != transaction::EPOCH_CHANGE);

            // tx succeeded, send to shards
            uint64_t num_shards = tx->shard_write.size();
            bool nop = (tx->type == transaction::NOP);

            if (!nop) {
                tx_prog_mutex.lock();
                outstanding_tx.emplace(tx->id, tx);
                tx_prog_mutex.unlock();
            }

            // send tx batches
            message::message msg;
            for (uint64_t i = 0; i < num_shards; i++) {
                if (tx->shard_write[i]) {
                    msg.prepare_message(message::TX_INIT, vt_id, factored_tx[i]->timestamp, factored_tx[i]->qts, *factored_tx[i]);
                    comm.send(i+ShardIdIncr, msg.buf);
                }
            }
        }
    }

    // assuming hold out_queue_mtx
    inline void
    timestamper :: reset_out_queue_clk(uint64_t epoch)
    {
        out_queue_clk = std::make_pair(epoch, 1);
        out_queue_counter = 0;
    }
}

#endif
