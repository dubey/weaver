/*
 * ===============================================================
 *    Description:  Vector timestamper server loop and request
 *                  processing methods.
 *
 *        Created:  07/22/2013 02:42:28 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <iostream>
#include <vector>
#include <deque>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>
#include <e/popt.h>
#include <pthread.h>
#include <dlfcn.h>

#define weaver_debug_
#include "common/vclock.h"
#include "common/transaction.h"
#include "common/event_order.h"
#include "common/config_constants.h"
#include "common/bool_vector.h"
#include "common/prog_write_and_dlopen.h"
#include "node_prog/dynamic_prog_table.h"
#include "node_prog/node_prog_type.h"
#include "coordinator/timestamper.h"

DECLARE_CONFIG_CONSTANTS;

using coordinator::current_prog;
using coordinator::blocked_prog;
using transaction::done_req_t;
using node_prog::Node_Parameters_Base;
static coordinator::timestamper *vts;
static uint64_t vt_id;

// tx functions
void prepare_tx(std::shared_ptr<transaction::pending_tx> tx, coordinator::hyper_stub *hstub, order::oracle *time_oracle);
void end_tx(uint64_t tx_id, coordinator::hyper_stub *hstub, uint64_t shard_id);


// assign locations to all node create ops
// then pass the tx to hstub object
void
prepare_tx(std::shared_ptr<transaction::pending_tx> tx, coordinator::hyper_stub *hstub, order::oracle *time_oracle)
{
    for (std::shared_ptr<transaction::pending_update> upd: tx->writes) {
        switch (upd->type) {

            case transaction::NODE_CREATE_REQ:
                // randomly assign shard for this node
                upd->loc1 = vts->generate_loc(); // node will be placed on this shard
                break;

            default:
                continue;
        }
    }

#undef CHECK_LOC
#undef HANDLE_OR_ALIAS

    tx->id = vts->generate_req_id();
    uint64_t sender = tx->sender;
    bool error = false;
    bool ready = false;

    while (!ready && !error) {
        vts->clk_rw_mtx.wrlock();
        vts->vclk.increment_clock();
        vts->out_queue_counter++;
        tx->timestamp = vts->vclk;
        tx->vt_seq = vts->out_queue_counter;
        vts->clk_rw_mtx.unlock();

        // write tx in warp
        // gets/puts/dels node mappings
        // sets error if any warp operation returns error
        // sets upd->loc for each upd in tx
        // sets tx->shard_write bool_vector (shard_write[i] = true iff there is a tx component at shard i)
        hstub->do_tx(tx, ready, error, time_oracle);

        assert(!(ready && error)); // can't be ready after some error

        std::shared_ptr<transaction::pending_tx> to_enq;
        if (!ready || error) {
            to_enq = tx->copy_fail_transaction();
        } else {
            to_enq = tx;
        }
        vts->enqueue_tx(to_enq);
    }

    message::message msg;
    if (error) {
        // fail tx
        msg.prepare_message(message::CLIENT_TX_ABORT);
    } else {
        msg.prepare_message(message::CLIENT_TX_SUCCESS);
    }
    vts->comm.send_to_client(sender, msg.buf);
    vts->tx_queue_loop();
}


// if all replies have been received, ack to client
void
end_tx(uint64_t tx_id, uint64_t shard_id, coordinator::hyper_stub *hstub)
{
    vts->tx_prog_mutex.lock();
    auto find_iter = vts->outstanding_tx.find(tx_id);
    assert(find_iter != vts->outstanding_tx.end());

    std::shared_ptr<transaction::pending_tx> tx = find_iter->second;
    uint64_t shard_idx = shard_id - ShardIdIncr;
    assert(tx->shard_write[shard_idx]);
    tx->shard_write[shard_idx] = false;

    if (weaver_util::none(tx->shard_write)) {
        // done tx
        hstub->clean_tx(tx_id);

        vts->outstanding_tx.erase(tx_id);
        vts->done_txs.emplace(tx_id, tx->shard_write);
    }
    vts->tx_prog_mutex.unlock();
}

// single dedicated thread which wakes up after given timeout, sends updates, and sleeps
void
nop_function()
{
    timespec sleep_time;
    int sleep_ret;
    int sleep_flags = 0;
    std::shared_ptr<transaction::pending_tx> tx = nullptr;

    uint64_t loop_count = 0;
    bool kronos_call;
    chronos_client kronos(KronosIpaddr.c_str(), KronosPort);
    vc::vclock_t kronos_cleanup_clk(ClkSz, 0);

    sleep_time.tv_sec  = VT_TIMEOUT_NANO / NANO;
    sleep_time.tv_nsec = VT_TIMEOUT_NANO % NANO;

    while (true) {
        sleep_ret = clock_nanosleep(CLOCK_REALTIME, sleep_flags, &sleep_time, nullptr);
        assert((sleep_ret == 0 || sleep_ret == EINTR) && "error in clock_nanosleep");

        if (++loop_count > 100000000) {
            kronos_call = true;
        } else {
            kronos_call = false;
        }

        vts->periodic_update_mutex.lock();

        // send nops and state cleanup info to shards
        if (weaver_util::any(vts->to_nop)) {
            tx = std::make_shared<transaction::pending_tx>(transaction::NOP);
            tx->nop = std::make_shared<transaction::nop_data>();

            tx->id = vts->generate_req_id();
            vts->clk_rw_mtx.wrlock();
            vts->vclk.increment_clock();
            vts->out_queue_counter++;
            tx->timestamp = vts->vclk;
            tx->vt_seq = vts->out_queue_counter;
            tx->shard_write = vts->to_nop;
            vts->clk_rw_mtx.unlock();

            vts->tx_prog_mutex.lock();
            tx->nop->max_done_clk = vts->m_max_done_clk;
            if (kronos_call) {
                if (vts->outstanding_progs.empty()) {
                    kronos_cleanup_clk = tx->timestamp.clock;
                } else if (kronos_cleanup_clk[vt_id+1] < vts->m_max_done_clk[vt_id+1]) {
                    kronos_cleanup_clk = vts->m_max_done_clk;
                }
            }
            tx->nop->outstanding_progs = vts->pend_progs.size();
            tx->nop->shard_node_count = vts->shard_node_count;

            for (auto &p: vts->done_txs) {
                for (uint64_t shard_id = 0; shard_id < p.second.size(); shard_id++) {
                    if (vts->to_nop[shard_id] && !p.second[shard_id]) {
                        p.second[shard_id] = true;
                    }
                }
                if (weaver_util::all(p.second)) {
                    tx->nop->done_txs.emplace_back(p.first);
                }
            }
            for (uint64_t r: tx->nop->done_txs) {
                vts->done_txs.erase(r);
            }

            vts->tx_prog_mutex.unlock();

            weaver_util::reset_all(vts->to_nop);
        } else {
            kronos_call = false;
        }

        vts->periodic_update_mutex.unlock();

        if (tx != nullptr) {
            vts->enqueue_tx(tx);
            vts->tx_queue_loop();
            tx = nullptr;
        }

        if (kronos_call) {
            int64_t id;
            chronos_returncode call_code, wait_code;

            uint64_t decref_count = UINT64_MAX;
            id = kronos.weaver_cleanup(kronos_cleanup_clk, vt_id, &call_code, &decref_count);
            id = kronos.wait(id, 100000, &wait_code);

            if (call_code != CHRONOS_SUCCESS || wait_code != CHRONOS_SUCCESS) {
                WDEBUG << "Kronos weaver_cleanup: call code " << chronos_returncode_to_string(call_code)
                       << ", wait code " << chronos_returncode_to_string(wait_code) << std::endl;
            }

            // for debugging
            //ssize_t ret;
            //chronos_stats stats;
            //id = kronos.get_stats(&call_code, &stats, &ret);
            //id = kronos.wait(id, 100000, &wait_code);

            //if (call_code == CHRONOS_SUCCESS && wait_code == CHRONOS_SUCCESS) {
            //    WDEBUG << "Kronos stats:" << std::endl;
            //    WDEBUG << "\tTime:\t" << stats.time << std::endl;
            //    WDEBUG << "\tEvents:\t" << stats.events << std::endl;
            //    WDEBUG << "\tCount Weaver order:\t" << stats.count_weaver_order << std::endl;
            //    WDEBUG << "\tCount Weaver cleanup:\t" << stats.count_weaver_cleanup << std::endl;
            //} else {
            //    WDEBUG << "Kronos get_stats: call code " << call_code << ", wait code " << wait_code << std::endl;
            //}

            loop_count = 0;
        }
    }
}

void*
clk_update_function(void*)
{
    timespec sleep_time;
    int sleep_ret;
    int sleep_flags = 0;
    message::message msg;
    vc::vclock vclk(vt_id, 0);
    uint64_t config_version;
    std::vector<server::state_t> vts_state(NumVts, server::NOT_AVAILABLE);

    sleep_time.tv_sec  = VT_CLK_TIMEOUT_NANO / NANO;
    sleep_time.tv_nsec = VT_CLK_TIMEOUT_NANO % NANO;

    vts->periodic_update_mutex.lock();
    config_version = vts->periodic_update_config.version();
    for (const server &srv: vts->periodic_update_config.get_servers()) {
        if (srv.type == server::VT && srv.state == server::AVAILABLE) {
            vts_state[srv.virtual_id] = server::AVAILABLE;
        }
    }
    vts->periodic_update_mutex.unlock();

    while (true) {
        sleep_ret = clock_nanosleep(CLOCK_REALTIME, sleep_flags, &sleep_time, nullptr);
        if (sleep_ret != 0 && sleep_ret != EINTR) {
            assert(false);
        }
        vts->periodic_update_mutex.lock();

        if (config_version < vts->periodic_update_config.version()) {
            config_version = vts->periodic_update_config.version();

            for (server::state_t &s: vts_state) {
                s = server::NOT_AVAILABLE;
            }

            for (const server &srv: vts->periodic_update_config.get_servers()) {
                if (srv.type == server::VT && srv.state == server::AVAILABLE) {
                    vts_state[srv.virtual_id] = server::AVAILABLE;
                }
            }
        }

        // update vclock at other timestampers
        vts->clk_rw_mtx.rdlock();
        vclk.clock = vts->vclk.clock;
        vts->clk_rw_mtx.unlock();
        for (uint64_t i = 0; i < NumVts; i++) {
            if (i == vt_id || vts_state[i] != server::AVAILABLE) {
                continue;
            }
            msg.prepare_message(message::VT_CLOCK_UPDATE, nullptr, vclk);
            vts->comm.send(i, msg.buf);
        }

        vts->periodic_update_mutex.unlock();
    }
}

// unpack client message for a node program, prepare shard msges, and send out
void
unpack_and_forward_node_prog(std::unique_ptr<message::message> msg,
                             uint64_t clientID,
                             coordinator::hyper_stub *hstub)
{
    WDEBUG << "got node prog" << std::endl;
    vts->restore_mtx.lock();
    if (vts->restore_status > 0) {
        vts->prog_queue->emplace_back(blocked_prog(clientID, std::move(msg)));
        vts->restore_mtx.unlock();
        return;
    } else {
        vts->restore_mtx.unlock();
    }

    std::string prog_type;
    msg->unpack_partial_message(message::CLIENT_NODE_PROG_REQ, prog_type);
    WDEBUG << "node prog type=" << prog_type << std::endl;

    void *prog_handle = nullptr;
    vts->m_dyn_prog_mtx.lock();
    auto prog_iter = vts->m_dyn_prog_map.find(prog_type);
    if (prog_iter != vts->m_dyn_prog_map.end()) {
        prog_handle = (void*)prog_iter->second.get();
    }
    vts->m_dyn_prog_mtx.unlock();

    if (prog_handle == nullptr) {
        msg->prepare_message(message::NODE_PROG_BADPROGTYPE);
        vts->comm.send_to_client(clientID, msg->buf);
        return;
    }

    std::vector<std::pair<node_handle_t, std::shared_ptr<Node_Parameters_Base>>> initial_args;
    msg->unpack_message(message::CLIENT_NODE_PROG_REQ,
                        prog_handle,
                        prog_type,
                        initial_args);
    
    // map from locations to a list of start_node_params to send to that shard
    std::unordered_map<uint64_t, std::deque<std::pair<node_handle_t, std::shared_ptr<Node_Parameters_Base>>>> initial_batches; 

    // lookup mappings
    std::unordered_map<node_handle_t, uint64_t> loc_map;
    std::unordered_set<node_handle_t> get_set;

    for (const auto &initial_arg : initial_args) {
        get_set.emplace(initial_arg.first);
    }

    if (!get_set.empty()) {
        loc_map = hstub->get_mappings(get_set);

        bool success = true;
        if (loc_map.size() < get_set.size() && AuxIndex) {
            std::unordered_map<std::string, std::pair<node_handle_t, uint64_t>> alias_map;
            std::pair<node_handle_t, uint64_t> empty;
            for (const node_handle_t &h: get_set) {
                if (loc_map.find(h) == loc_map.end()) {
                    alias_map.emplace(h, empty);
                }
            }

            assert((alias_map.size() + loc_map.size()) == get_set.size());

            success = hstub->get_idx(alias_map);

            if (success) {
                for (auto &arg: initial_args) {
                    auto iter = alias_map.find(arg.first);
                    if (iter != alias_map.end()) {
                        arg.first = iter->second.first;
                        loc_map.emplace(iter->second.first, iter->second.second);
                    } else {
                        assert(loc_map.find(arg.first) != loc_map.end());
                    }
                }
            }
        } else if (loc_map.size() < get_set.size()) {
            success = false;
        }

        if (!success) {
            // some node handles bad, return immediately
            WDEBUG << "bad node handles in node prog request: ";
            for (auto &h: get_set) {
                std::cerr << h << " ";
            }
            std::cerr << std::endl;
            msg->prepare_message(message::NODE_PROG_NOTFOUND);
            vts->comm.send_to_client(clientID, msg->buf);
            return;
        }
    }

    // process loc map
    // hack around bug: should be storing shard id in HyperDex, not server
    if (ShardIdIncr > 1) {
        uint64_t increment = ShardIdIncr - 1;
        for (auto &p: loc_map) {
            p.second += increment;
        }
    }

    for (const auto &p: initial_args) {
        initial_batches[loc_map[p.first]].emplace_back(p);
    }

    vts->clk_rw_mtx.wrlock();
    vts->vclk.increment_clock();
    vc::vclock req_timestamp = vts->vclk;
    assert(req_timestamp.clock.size() == ClkSz);

    vts->tx_prog_mutex.lock();
    vts->clk_rw_mtx.unlock();

    uint64_t req_id = vts->generate_req_id();
    current_prog *cp = new current_prog(req_id, clientID, req_timestamp);
    uint64_t cp_int = (uint64_t)cp;
    vts->pend_progs.emplace_back(cp);
    vts->outstanding_progs.emplace(req_id);
    vts->tx_prog_mutex.unlock();

    message::message msg_to_send;
    for (auto &batch_pair: initial_batches) {
        msg_to_send.prepare_message(message::NODE_PROG,
                                    prog_handle,
                                    prog_type,
                                    vt_id,
                                    req_timestamp,
                                    req_id,
                                    cp_int,
                                    batch_pair.second);
        vts->comm.send(batch_pair.first, msg_to_send.buf);
        WDEBUG << "send node prog=" << req_id << " to shard=" << batch_pair.first << std::endl;
    }

#ifdef weaver_benchmark_
    vts->test_mtx.lock();
    vts->outstanding_cnt++;
    if (vts->outstanding_cnt > vts->max_outstanding_cnt) {
        vts->max_outstanding_cnt = vts->outstanding_cnt;
    }
    vts->test_mtx.unlock();
#endif
}

// remove a completed node program from pending_prog data structure
// update 'max_done_clk' accordingly
// return true if successfully process prog_done, false if already processed this prog
bool
node_prog_done(uint64_t req_id, current_prog *cp)
{
    vts->tx_prog_mutex.lock();

    auto &done_progs = vts->done_progs;
    auto &outstanding_progs = vts->outstanding_progs;

    if (outstanding_progs.find(req_id) == outstanding_progs.end()) {
        return false;
    }

    outstanding_progs.erase(req_id);
    done_progs.emplace_back(cp);

    //if (++vts->prog_done_cnt % 100 == 0) {
    if (++vts->prog_done_cnt % 1 == 0) {
        vts->prog_done_cnt = 0;
    } else {
        return true;
    }

    vts->process_pend_progs();

    vts->tx_prog_mutex.unlock();

    return true;
}

bool
register_node_prog(std::unique_ptr<message::message> msg, uint64_t client)
{
    std::vector<uint8_t> buf;
    std::string prog_handle;
    msg->unpack_message(message::REGISTER_NODE_PROG, nullptr, prog_handle, buf);

    if (prog_handle != weaver_util::sha256_chararr(buf, buf.size())) {
        WDEBUG << "register_node_prog error: "
               << "sha256 does not match, "
               << "prog_handle=" << prog_handle
               << ", sha256=" << weaver_util::sha256_chararr(buf, buf.size())
               << std::endl;
        return false;
    }

    std::shared_ptr<dynamic_prog_table> prog_table = write_and_dlopen(buf);
    if (prog_table == nullptr) {
        return false;
    }

    std::unordered_set<uint64_t> shards;

    vts->periodic_update_mutex.lock();
    for (const server &srv: vts->periodic_update_config.get_servers()) {
        if (srv.type == server::SHARD && srv.state == server::AVAILABLE) {
            shards.emplace(srv.virtual_id+NumVts);
        }
    }
    vts->periodic_update_mutex.unlock();

    WDEBUG << "registering node prog " << prog_handle << " at #" << shards.size() << " shards" << std::endl;

    for (uint64_t s: shards) {
        message::message m;
        m.prepare_message(message::REGISTER_NODE_PROG,
                          nullptr,
                          prog_handle,
                          vt_id,
                          buf);
        vts->comm.send(s, m.buf);
        WDEBUG << "register node prog at shard " << s << std::endl;
    }

    coordinator::register_node_prog_state reg_state;
    reg_state.client = client;
    reg_state.shards = shards;

    vts->m_dyn_prog_mtx.lock();
    vts->m_dyn_prog_map[prog_handle] = prog_table;
    vts->m_register_prog_status[prog_handle] = reg_state;
    vts->m_dyn_prog_mtx.unlock();

    return true;
}

static bool
block_signals()
{
    sigset_t ss;
    if (sigfillset(&ss) < 0) {
        std::cerr << "sigfillset" << std::endl;
        return false;
    }

    sigdelset(&ss, SIGPROF);
    if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0) {
        std::cerr << "could not block signals" << std::endl;
        return false;
    }

    return true;
}

void
shard_register_node_prog(const std::string &prog_handle,
                         uint64_t shard,
                         bool success)
{
    vts->m_dyn_prog_mtx.lock();
    coordinator::register_node_prog_state &state = vts->m_register_prog_status[prog_handle];
    state.shards.erase(shard);
    bool overall_success = success && state.success;
    state.success = overall_success;
    bool done = state.shards.empty();
    uint64_t client = state.client;
    if (done) {
        vts->m_register_prog_status.erase(prog_handle);
    }
    vts->m_dyn_prog_mtx.unlock();

    if (done) {
        message::msg_type send_type = overall_success? message::REGISTER_NODE_PROG_SUCCESSFUL : message::REGISTER_NODE_PROG_FAILED;
        message::message m;
        m.prepare_message(send_type);
        vts->comm.send_to_client(client, m.buf);
    }
}

void*
server_loop(void *args)
{
    uint64_t thread_id_long = (uint64_t)args;
    int thread_id = (int)thread_id_long;
    if (!block_signals()) {
        return nullptr;
    }

    busybee_returncode ret;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> msg;
    uint64_t tx_id, client_sender, shard_id;
    std::string prog_type;
    coordinator::hyper_stub *hstub = vts->hstub[thread_id];
    order::oracle *time_oracle = vts->time_oracles[thread_id];
    std::shared_ptr<transaction::pending_tx> tx;

    while (true) {
        vts->comm.quiesce_thread(thread_id);
        msg.reset(new message::message());
        ret = vts->comm.recv(thread_id, &client_sender, &msg->buf);
        if (ret != BUSYBEE_SUCCESS && ret != BUSYBEE_TIMEOUT) {
            continue;
        } else {
            // good to go, unpack msg
            mtype = msg->unpack_message_type();

            switch (mtype) {
                // client messages
                case message::CLIENT_TX_INIT: {
                    tx = std::make_shared<transaction::pending_tx>(transaction::UPDATE);
                    msg->unpack_message(message::CLIENT_TX_INIT, nullptr, tx->id, tx->writes);
                    tx->sender = client_sender;
                    prepare_tx(tx, hstub, time_oracle);
                    break;
                }

                case message::VT_CLOCK_UPDATE: {
                    vc::vclock rec_clk;
                    msg->unpack_message(message::VT_CLOCK_UPDATE, nullptr, rec_clk);
                    vts->clk_rw_mtx.wrlock();
                    vts->clk_updates++;
                    vts->vclk.update_clock(rec_clk);
                    vts->clk_rw_mtx.unlock();
                    break;
                }

                //case message::VT_CLOCK_UPDATE_ACK:
                //    vts->periodic_update_mutex.lock();
                //    vts->clock_update_acks++;
                //    assert(vts->clock_update_acks < NumVts);
                //    vts->periodic_update_mutex.unlock();
                //    break;

                case message::VT_NOP_ACK: {
                    uint64_t shard_node_count, nop_qts, sid, sender;
                    std::unordered_map<uint64_t, uint64_t> node_recovery_counts;
                    msg->unpack_message(message::VT_NOP_ACK, nullptr, sender, nop_qts, shard_node_count, node_recovery_counts);
                    sid = sender - ShardIdIncr;
                    vts->periodic_update_mutex.lock();
                    if (nop_qts > vts->nop_ack_qts[sid]) {
                        vts->shard_node_count[sid] = shard_node_count;
                        vts->to_nop[sid] = true;
                        vts->nop_ack_qts[sid] = nop_qts;
                    }

                    for (const auto &p: node_recovery_counts) {
                        auto iter = vts->node_recovery_counts.find(p.first);
                        if (iter == vts->node_recovery_counts.end()) {
                            vts->node_recovery_counts.emplace(p.first, std::make_pair(1, p.second));
                            iter = vts->node_recovery_counts.find(p.first);
                        } else {
                            iter->second.first++;
                            iter->second.second += p.second;
                        }

                        WDEBUG << "prog=" << p.first << ", # node recoveries=" << iter->second.second << std::endl;
                        if (iter->second.first == get_num_shards()) {
                            WDEBUG << "done prog=" << p.first << ", # node recoveries=" << iter->second.second << std::endl;
                            vts->node_recovery_counts.erase(p.first);
                        }
                    }
                    vts->periodic_update_mutex.unlock();
                    break;
                }

                case message::CLIENT_NODE_COUNT: {
                    vts->periodic_update_mutex.lock();
                    msg->prepare_message(message::NODE_COUNT_REPLY, nullptr, vts->shard_node_count);
                    vts->periodic_update_mutex.unlock();
                    vts->comm.send_to_client(client_sender, msg->buf);
                    break;
                }

                case message::TX_DONE:
                    msg->unpack_message(message::TX_DONE, nullptr, tx_id, shard_id);
                    end_tx(tx_id, shard_id, hstub);
                    break;

                //case message::START_MIGR: {
                //    uint64_t hops = UINT64_MAX;
                //    msg->prepare_message(message::MIGRATION_TOKEN, hops, vt_id);
                //    vts->comm.send(ShardIdIncr, msg->buf); 
                //    break;
                //}

                case message::ONE_STREAM_MIGR: {
                    uint64_t hops = get_num_shards();
                    vts->migr_mutex.lock();
                    vts->migr_client = client_sender;
                    vts->migr_mutex.unlock();
                    msg->prepare_message(message::MIGRATION_TOKEN, nullptr, hops, hops, vt_id);
                    vts->comm.send(ShardIdIncr, msg->buf);
                    break;
                }

                case message::MIGRATION_TOKEN: {
                    vts->migr_mutex.lock();
                    uint64_t client = vts->migr_client;
                    vts->migr_mutex.unlock();
                    msg->prepare_message(message::DONE_MIGR);
                    vts->comm.send_to_client(client, msg->buf);
                    WDEBUG << "Shard node counts are:";
                    for (uint64_t &x: vts->shard_node_count) {
                        std::cerr << " " << x;
                    }
                    std::cerr << std::endl;
                    break;
                }

                case message::CLIENT_NODE_PROG_REQ:
                    unpack_and_forward_node_prog(std::move(msg), client_sender, hstub);
                    break;

                // node program response from a shard
                case message::NODE_PROG_RETURN: {
                    uint64_t req_id, cp_int, client;
                    msg->unpack_partial_message(message::NODE_PROG_RETURN, prog_type, req_id, cp_int); // don't unpack rest
                    WDEBUG << "prog_type=" << prog_type
                           << " req_id=" << req_id
                           << " cp_int=" << cp_int
                           << std::endl;
                    current_prog *cp = (current_prog*)cp_int;
                    client = cp->client;

                    bool to_process = node_prog_done(req_id, cp);

                    if (to_process) {
                        vts->comm.send_to_client(client, msg->buf);
                        WDEBUG << "done node prog=" << req_id << " and sent to client=" << client << std::endl;
#ifdef weaver_benchmark_
                        vts->test_mtx.lock();
                        vts->outstanding_cnt--;
                        vts->test_mtx.unlock();
#endif
                    }
                    break;
                }

                case message::REGISTER_NODE_PROG:
                    WDEBUG << "got REGISTER_NODE_PROG msg" << std::endl;
                    if (!register_node_prog(std::move(msg), client_sender)) {
                        WDEBUG << "failed register node prog" << std::endl;
                        msg->prepare_message(message::REGISTER_NODE_PROG_FAILED);
                        vts->comm.send_to_client(client_sender, msg->buf);
                    }
                    break;

                case message::REGISTER_NODE_PROG_SUCCESSFUL: {
                    std::string prog_handle;
                    uint64_t shard;
                    msg->unpack_message(mtype,
                                        nullptr,
                                        prog_handle,
                                        shard);
                    WDEBUG << "got REGISTER_NODE_PROG_SUCCESS "
                           << "prog=" << prog_handle
                           << ", shard=" << shard
                           << std::endl;

                    shard_register_node_prog(prog_handle, shard, true);
                    break;
                }

                case message::REGISTER_NODE_PROG_FAILED: {
                    std::string prog_handle;
                    uint64_t shard;
                    msg->unpack_message(mtype,
                                        nullptr,
                                        prog_handle,
                                        shard);
                    WDEBUG << "got REGISTER_NODE_PROG_FAILED "
                           << "prog=" << prog_handle
                           << ", shard=" << shard
                           << std::endl;

                    shard_register_node_prog(prog_handle, shard, false);
                    break;
                }

                case message::RESTORE_DONE: {
                    vts->restore_mtx.lock();
                    assert(vts->restore_status > 0);
                    vts->restore_status--;
                    coordinator::prog_queue_t progs = std::move(vts->prog_queue);
                    vts->prog_queue.reset(new std::vector<blocked_prog>());
                    vts->restore_mtx.unlock();

                    vts->tx_queue_loop();
                    for (blocked_prog &bp: *progs) {
                        unpack_and_forward_node_prog(std::move(bp.msg), bp.client, hstub);
                    }
                    break;
                }

                default:
                    WDEBUG << "unexpected msg type " << message::to_string(mtype) << std::endl;
                    assert(false);
            }
        }
    }
}

bool
generate_token(uint64_t* token)
{
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0)
    {
        return false;
    }

    if (sysrand.read(token, sizeof(*token)) != sizeof(*token))
    {
        return false;
    }

    return true;
}

struct sm_link_loop_data
{
    po6::net::hostname sm_host;
    po6::net::location loc;
    bool backup;
};

void*
server_manager_link_loop(void *args)
{
    sm_link_loop_data *data = (sm_link_loop_data*)args;
    po6::net::hostname &sm_host = data->sm_host;
    po6::net::location &loc = data->loc;
    bool &backup = data->backup;

    // Most of the following code has been 'borrowed' from
    // Robert Escriva's HyperDex.
    // see https://github.com/rescrv/HyperDex for the original code.

    if (!block_signals()) {
        return nullptr;
    }

    vts->sm_stub.set_server_manager_address(sm_host.address.c_str(), sm_host.port);

    server::type_t type = backup? server::BACKUP_VT : server::VT;
    if (!vts->sm_stub.register_id(vts->serv_id, loc, type))
    {
        return nullptr;
    }

    bool cluster_jump = false;

    while (!vts->sm_stub.should_exit())
    {
        vts->exit_mutex.lock();
        if (vts->to_exit) {
            vts->sm_stub.request_shutdown();
            vts->to_exit = false;
        }
        vts->exit_mutex.unlock();

        if (!vts->sm_stub.maintain_link())
        {
            continue;
        }
        const configuration& old_config(vts->config);
        const configuration& new_config(vts->sm_stub.config());

        if (old_config.cluster() != 0 &&
            old_config.cluster() != new_config.cluster())
        {
            cluster_jump = true;
            break;
        }

        if (old_config.version() > new_config.version())
        {
            WDEBUG << "received new configuration version=" << new_config.version()
                   << " that's older than our current configuration version="
                   << old_config.version();
            continue;
        }
        // if old_config.version == new_config.version, still fetch

        vts->config_mutex.lock();
        vts->prev_config = vts->config;
        vts->config = new_config;
        if (!vts->first_config) {
            vts->first_config = true;
            vts->first_config_cond.signal();
        }
        vts->reconfigure();
        vts->config_mutex.unlock();

        // let the coordinator know we've moved to this config
        vts->sm_stub.config_ack(new_config.version());
    }

    if (cluster_jump)
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because the server manager changed on us.\n"
               << "This is most likely an operations error."
               << "================================================================================";
    }
    else if (vts->sm_stub.should_exit() && !vts->sm_stub.config().exists(vts->serv_id))
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because the server manager says it doesn't know about this node.\n"
               << "================================================================================";
    }
    else if (vts->sm_stub.should_exit())
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because server manager stub says we should exit.\n"
               << "Most likely because we requested shutdown due to program interrupt.\n"
               << "================================================================================\n";
    }
    exit(0);
}

void
install_signal_handler(int signum, void (*handler)(int))
{
    struct sigaction sa;
    sa.sa_handler = handler;
    sigfillset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    int ret = sigaction(signum, &sa, nullptr);
    assert(ret == 0);
}

// caution: assume holding vts->config_mutex for vts->pause_bb
void
init_worker_threads(std::vector<std::shared_ptr<pthread_t>> &threads)
{
    for (uint64_t i = 0; i < NUM_VT_THREADS; i++) {
        std::shared_ptr<pthread_t> t(new pthread_t());
        int rc = pthread_create(t.get(), nullptr, &server_loop, (void*)i);
        assert(rc == 0);
        threads.emplace_back(t);
    }
    vts->pause_bb = true;
}

// caution: assume holding vts->config_mutex
void
init_vt()
{
    std::vector<server> servers = vts->config.get_servers();
    vt_id = UINT64_MAX;
    uint64_t weaver_id = UINT64_MAX;
    for (const server &srv: servers) {
        WDEBUG << "server " << srv.weaver_id << " has address " << srv.bind_to << std::endl;
        if (srv.id == vts->serv_id) {
            assert(srv.type == server::VT);
            vt_id = srv.virtual_id;
            weaver_id = srv.weaver_id;
        }
    }
    assert(vt_id != UINT64_MAX);

    // registered this server with server_manager, we now know the vt_id
    vts->init(vt_id, weaver_id);
}

void
end_program(int signum)
{
    std::cerr << "Ending program, signum = " << signum << std::endl;
    vts->clk_rw_mtx.wrlock();
    WDEBUG << "num vclk updates " << vts->clk_updates << std::endl;
    vts->clk_rw_mtx.unlock();

#ifdef weaver_benchmark_
    WDEBUG << "max outstanding prog cnt = " << vts->max_outstanding_cnt << std::endl;
    WDEBUG << "outstanding prog cnt = " << vts->outstanding_cnt << std::endl;
#endif
}

static void
dummy(int)
{ }

static void
exit_on_signal(int sig)
{
    WDEBUG << "got signal " << strsignal(sig) << ", exiting now" << std::endl;
    exit(EXIT_SUCCESS);
}

int
main(int argc, const char *argv[])
{
    install_signal_handler(SIGHUP, exit_on_signal);
    install_signal_handler(SIGINT, exit_on_signal);
    install_signal_handler(SIGTERM, exit_on_signal);
    install_signal_handler(SIGUSR1, dummy);

    if (!block_signals()) {
        return EXIT_FAILURE;
    }

    // command line params
    const char* listen_host = "127.0.0.1";
    long listen_port = 5200;
    const char *config_file = "./weaver.yaml";
    bool backup = false;
    const char *log_file_name = nullptr;
    bool log_immediate = false;
    // arg parsing borrowed from HyperDex
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('l', "listen")
            .description("listen on a specific IP address (default: 127.0.0.1)")
            .metavar("IP").as_string(&listen_host);
    ap.arg().name('p', "listen-port")
            .description("listen on an alternative port (default: 5200)")
            .metavar("port").as_long(&listen_port);
    ap.arg().name('b', "backup-vt")
            .description("make this a backup timestamper")
            .set_true(&backup);
    ap.arg().long_name("config-file")
            .description("full path of weaver.yaml configuration file (default ./weaver.yaml)")
            .metavar("filename").as_string(&config_file);
    ap.arg().long_name("log-file")
            .description("full path of file to write log to (default: stderr)")
            .metavar("filename").as_string(&log_file_name);
    ap.arg().long_name("log-immediate")
            .description("flush log immediately")
            .set_true(&log_immediate);

    if (!ap.parse(argc, argv) || ap.args_sz() != 0) {
        std::cerr << "args parsing failure" << std::endl;
        return -1;
    }

    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    if (log_file_name == nullptr) {
        google::LogToStderr();
    } else {
        google::SetLogDestination(google::INFO, log_file_name);
    }

    if (log_immediate) {
        FLAGS_logbufsecs = 0;
    }

    // configuration file parse
    if (!init_config_constants(config_file)) {
        WDEBUG << "error in init_config_constants, exiting now." << std::endl;
        return -1;
    }

    std::shared_ptr<po6::net::location> my_loc(new po6::net::location(listen_host, listen_port));
    uint64_t sid;
    assert(generate_token(&sid));
    vts = new coordinator::timestamper(sid, my_loc, backup);

    // server manager link
    std::shared_ptr<pthread_t> sm_thr(new pthread_t());
    sm_link_loop_data sm_args;
    sm_args.sm_host = po6::net::hostname(ServerManagerIpaddr.c_str(), ServerManagerPort);
    sm_args.loc = *my_loc;
    sm_args.backup = backup;
    int rc = pthread_create(sm_thr.get(), nullptr, &server_manager_link_loop, (void*)&sm_args);
    assert(rc == 0);

    vts->config_mutex.lock();

    // wait for first config to arrive from server manager
    while (!vts->first_config) {
        vts->first_config_cond.wait();
    }

    std::vector<std::shared_ptr<pthread_t>> worker_threads;

    if (backup) {
        while (!vts->active_backup) {
            vts->backup_cond.wait();
        }

        init_vt();

        vts->config_mutex.unlock();
        // release config_mutex while restoring vt which may take a while
        vts->restore_backup();

        vts->config_mutex.lock();
        init_worker_threads(worker_threads);
    } else {
        init_vt();

        init_worker_threads(worker_threads);
    }

    // initial wait for all vector timestampers to start
    while (vts->num_active_vts != NumVts) {
        vts->start_all_vts_cond.wait();
    }
    vts->config_mutex.unlock();

    std::cout << "Vector timestamper " << vt_id << std::endl;
    std::cout << "THIS IS AN ALPHA RELEASE WHICH SHOULD NOT BE USED IN PRODUCTION" << std::endl;

    std::shared_ptr<pthread_t> clk_update_thr;
    if (NumVts > 1) {
        // periodic vector clock update to other timestampers
        clk_update_thr.reset(new pthread_t());
        int rc = pthread_create(clk_update_thr.get(), nullptr, &clk_update_function, nullptr);
        assert(rc == 0);
    }

    // periodic nops to shard
    nop_function();

    for (std::shared_ptr<pthread_t> t: worker_threads) {
        pthread_join(*t, nullptr);
    }
    pthread_join(*sm_thr, nullptr);
    if (clk_update_thr != nullptr) {
        pthread_join(*clk_update_thr, nullptr);
    }

    return EXIT_SUCCESS;
}
