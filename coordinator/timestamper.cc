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
#include <thread>
#include <vector>
#include <deque>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>

#define weaver_debug_
#include "common/vclock.h"
#include "common/transaction.h"
#include "common/event_order.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
#include "timestamper.h"

using coordinator::current_prog;
using coordinator::current_tx;
static coordinator::timestamper *vts;
static uint64_t vt_id;

// SIGINT handler
void
end_program(int signum)
{
    std::cerr << "Ending program, signum = " << signum << std::endl;
    vts->clk_rw_mtx.wrlock();
    WDEBUG << "num vclk updates " << vts->clk_updates << std::endl;
    vts->clk_rw_mtx.unlock();
    if (signum == SIGINT) {
        vts->exit_mutex.lock();
        vts->to_exit = true;
        vts->exit_mutex.unlock();
    } else {
        WDEBUG << "Got interrupt signal other than SIGINT, exiting immediately." << std::endl;
        exit(0);
    }
}

void
prepare_del_transaction(transaction::pending_tx &tx, std::vector<uint64_t> &del_elems)
{
    current_tx cur_tx(tx.client_id, tx);
    cur_tx.count = NUM_VTS - 1;
    vts->busy_mtx.lock();
    vts->del_tx[tx.id] = cur_tx;
    vts->busy_mtx.unlock();
    for (uint64_t i = 0; i < NUM_VTS; i++) {
        if (i != vt_id) {
            message::message msg;
            msg.prepare_message(message::PREP_DEL_TX, vt_id, tx.id, tx.client_id, del_elems);
            vts->comm.send(i, msg.buf);
        }
    }
}

// expects an input of list of writes that are part of this transaction
// for all writes, node mapper lookups should have already been performed
// for create requests, instead of lookup an entry for new handle should have been inserted
void
begin_transaction(transaction::pending_tx &tx, coordinator::hyper_stub *hstub)
{
    typedef std::vector<transaction::pending_tx> tx_vec_t;
    std::shared_ptr<tx_vec_t> tx_vec(new tx_vec_t(NUM_SHARDS, transaction::pending_tx()));
    tx_vec_t &tv = *tx_vec;

    vts->clk_rw_mtx.wrlock();
    for (std::shared_ptr<transaction::pending_update> upd: tx.writes) {
        vts->qts.at(upd->loc1-SHARD_ID_INCR)++;
        upd->qts = vts->qts;
        tv[upd->loc1-SHARD_ID_INCR].writes.emplace_back(upd);
    }
    vts->vclk.increment_clock();
    tx.timestamp = vts->vclk;
    vts->clk_rw_mtx.unlock();

    current_tx cur_tx(tx.client_id, tx);
    for (uint64_t i = 0; i < NUM_SHARDS; i++) {
        if (!tv[i].writes.empty()) {
            cur_tx.count++;
        }
    }

    // record txs as outstanding for reply bookkeeping and fault tolerance
    uint64_t buf_sz = message::size(tx);
    std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack_at(0);
    message::pack_buffer(packer, tx);
    hstub->put_tx(tx.id, buf);
    vts->tx_prog_mutex.lock();
    vts->outstanding_tx.emplace(tx.id, cur_tx);
    vts->tx_prog_mutex.unlock();

    // send tx batches
    message::message msg;
    for (uint64_t i = 0; i < NUM_SHARDS; i++) {
        if (!tv[i].writes.empty()) {
            tv[i].timestamp = tx.timestamp;
            tv[i].id = tx.id;
            msg.prepare_message(message::TX_INIT, vt_id, tx.timestamp, tv[i].writes.at(0)->qts, tx.id, tv[i].writes);
            vts->comm.send(tv[i].writes.at(0)->loc1, msg.buf);
        }
    }
}

// decrement reply count. if all replies have been received, ack to client
void
end_transaction(uint64_t tx_id, coordinator::hyper_stub *hstub)
{
    vts->tx_prog_mutex.lock();
    if (--vts->outstanding_tx.at(tx_id).count == 0) {
        // done tx
        hstub->del_tx(tx_id);
        uint64_t client_id = vts->outstanding_tx[tx_id].client;
        transaction::pending_tx tx = vts->outstanding_tx[tx_id].tx;
        vts->outstanding_tx.erase(tx_id);
        vts->tx_prog_mutex.unlock();

        // unbusy elements
        std::vector<uint64_t> busy;
        for (auto upd: tx.writes) {
            switch (upd->type) {
                case transaction::NODE_CREATE_REQ:
                    busy.emplace_back(upd->id);
                    break;

                case transaction::EDGE_CREATE_REQ:
                    busy.emplace_back(upd->id);
                    busy.emplace_back(upd->elem1);
                    busy.emplace_back(upd->elem2);
                    break;

                case transaction::NODE_SET_PROPERTY:
                    busy.emplace_back(upd->elem1);
                    break;

                case transaction::EDGE_DELETE_REQ:
                    busy.emplace_back(upd->elem2);
                    break;

                case transaction::EDGE_SET_PROPERTY:
                    busy.emplace_back(upd->elem1);
                    busy.emplace_back(upd->elem2);
                    break;

                default:
                    WDEBUG << "bad type" << std::endl;
            }
        }

        vts->busy_mtx.lock();
        for (uint64_t e: busy) {
            assert(vts->deleted_elems.find(e) == vts->deleted_elems.end());
            assert(vts->other_deleted_elems.find(e) == vts->other_deleted_elems.end());
            assert(vts->busy_elems.find(e) != vts->busy_elems.end());
            if (--vts->busy_elems[e] == 0) {
                vts->busy_elems.erase(e);
            }
        }
        vts->busy_mtx.unlock();

        // send response to client
        message::message msg;
        msg.prepare_message(message::CLIENT_TX_DONE);
        vts->comm.send(client_id, msg.buf);
    } else {
        vts->tx_prog_mutex.unlock();
    }
}

// single dedicated thread which wakes up after given timeout, sends updates, and sleeps
void
nop_function()
{
    timespec sleep_time;
    int sleep_ret;
    int sleep_flags = 0;
    vc::vclock vclk(vt_id, 0);
    vc::qtimestamp_t qts;
    uint64_t req_id, max_done_id;
    vc::vclock_t max_done_clk;
    uint64_t num_outstanding_progs;
    typedef std::vector<std::pair<uint64_t, node_prog::prog_type>> done_req_t;
    std::vector<done_req_t> done_reqs(NUM_SHARDS, done_req_t());
    std::vector<uint64_t> del_done_reqs;
    message::message msg;
    //bool nop_sent, clock_synced;

    sleep_time.tv_sec  = VT_TIMEOUT_NANO / NANO;
    sleep_time.tv_nsec = VT_TIMEOUT_NANO % NANO;

    while (true) {
        sleep_ret = clock_nanosleep(CLOCK_REALTIME, sleep_flags, &sleep_time, NULL);
        if (sleep_ret != 0 && sleep_ret != EINTR) {
            assert(false);
        }
        //nop_sent = false;
        //clock_synced = false;
        vts->periodic_update_mutex.lock();

        // send nops and state cleanup info to shards
        if (vts->to_nop.any()) {
            req_id = vts->generate_id();
            vts->clk_rw_mtx.wrlock();
            vts->vclk.increment_clock();
            vclk.clock = vts->vclk.clock;
            for (uint64_t shard_id = 0; shard_id < NUM_SHARDS; shard_id++) {
                if (vts->to_nop[shard_id]) {
                    vts->qts[shard_id]++;
                    done_reqs[shard_id].clear();
                }
            }
            qts = vts->qts;
            vts->clk_rw_mtx.unlock();

            del_done_reqs.clear();
            vts->tx_prog_mutex.lock();
            max_done_id = vts->max_done_id;
            max_done_clk = *vts->max_done_clk;
            num_outstanding_progs = vts->pend_prog_queue.size();
            for (auto &x: vts->done_reqs) {
                // x.first = node prog type
                // x.second = unordered_map <req_id -> bitset<NUM_SHARDS>>
                for (auto &reply: x.second) {
                    // reply.first = req_id
                    // reply.second = bitset<NUM_SHARDS>
                    for (uint64_t shard_id = 0; shard_id < NUM_SHARDS; shard_id++) {
                        if (vts->to_nop[shard_id] && !reply.second[shard_id]) {
                            reply.second.set(shard_id);
                            done_reqs[shard_id].emplace_back(std::make_pair(reply.first, x.first));
                        }
                    }
                    if (reply.second.all()) {
                        del_done_reqs.emplace_back(reply.first);
                    }
                }
                for (auto &del: del_done_reqs) {
                    x.second.erase(del);
                }
            }
            vts->tx_prog_mutex.unlock();

            for (uint64_t shard_id = 0; shard_id < NUM_SHARDS; shard_id++) {
                if (vts->to_nop[shard_id]) {
                    assert(vclk.clock.size() == NUM_VTS);
                    assert(max_done_clk.size() == NUM_VTS);
                    msg.prepare_message(message::VT_NOP, vt_id, vclk, qts, req_id,
                        done_reqs[shard_id], max_done_id, max_done_clk,
                        num_outstanding_progs, vts->shard_node_count);
                    vts->comm.send(shard_id + SHARD_ID_INCR, msg.buf);
                }
            }
            vts->to_nop.reset();
            //nop_sent = true;
        }

        // update vclock at other timestampers
        //if (vts->clock_update_acks == (NUM_VTS-1) && NUM_VTS > 1) {
        //clock_synced = true;
        //vts->clock_update_acks = 0;
        //if (!nop_sent) {
        //    vts->clk_mutex.lock();
        //    vclk.clock = vts->vclk.clock;
        //    vts->clk_mutex.unlock();
        //}
        //for (uint64_t i = 0; i < NUM_VTS; i++) {
        //    if (i == vt_id) {
        //        continue;
        //    }
        //    msg.prepare_message(message::VT_CLOCK_UPDATE, vt_id, vclk.clock[vt_id]);
        //    vts->comm.send(i, msg.buf);
        //}
        ////}

        //if (nop_sent && !clock_synced) {
        ////    WDEBUG << "nop yes, clock no" << std::endl;
        //} else if (!nop_sent && clock_synced) {
        ////    WDEBUG << "clock yes, nop no" << std::endl;
        //}

        vts->periodic_update_mutex.unlock();
    }
}

void
clk_update_function()
{
    timespec sleep_time;
    int sleep_ret;
    int sleep_flags = 0;
    message::message msg;
    vc::vclock vclk(vt_id, 0);

    sleep_time.tv_sec  = VT_CLK_TIMEOUT_NANO / NANO;
    sleep_time.tv_nsec = VT_CLK_TIMEOUT_NANO % NANO;

    while (true) {
        sleep_ret = clock_nanosleep(CLOCK_REALTIME, sleep_flags, &sleep_time, NULL);
        if (sleep_ret != 0 && sleep_ret != EINTR) {
            assert(false);
        }
        vts->periodic_update_mutex.lock();

        // update vclock at other timestampers
        vts->clk_rw_mtx.rdlock();
        vclk.clock = vts->vclk.clock;
        vts->clk_rw_mtx.unlock();
        for (uint64_t i = 0; i < NUM_VTS; i++) {
            if (i == vt_id) {
                continue;
            }
            msg.prepare_message(message::VT_CLOCK_UPDATE, vt_id, vclk.clock[vt_id]);
            vts->comm.send(i, msg.buf);
        }

        vts->periodic_update_mutex.unlock();
    }
}

// unpack client message for a node program, prepare shard msges, and send out
template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, nmap::nmap_stub *nmap_cl)
{
    node_prog::prog_type pType;
    std::vector<std::pair<uint64_t, ParamsType>> initial_args;

    msg->unpack_message(message::CLIENT_NODE_PROG_REQ, pType, initial_args);
    
    // map from locations to a list of start_node_params to send to that shard
    std::unordered_map<uint64_t, std::deque<std::pair<uint64_t, ParamsType>>> initial_batches; 

    // lookup mappings
    std::unordered_map<uint64_t, uint64_t> request_element_mappings;
    std::unordered_set<uint64_t> mappings_to_get;
    for (auto &initial_arg : initial_args) {
        uint64_t c_id = initial_arg.first;
        mappings_to_get.insert(c_id);
    }
    if (!mappings_to_get.empty()) {
        auto results = nmap_cl->get_mappings(mappings_to_get);
        assert(results.size() == mappings_to_get.size());
        for (auto &toAdd : results) {
            request_element_mappings.emplace(toAdd);
        }
    }

    for (std::pair<uint64_t, ParamsType> &node_params_pair: initial_args) {
        uint64_t loc = request_element_mappings[node_params_pair.first];
        initial_batches[loc].emplace_back(std::make_pair(node_params_pair.first,
                    std::move(node_params_pair.second)));
    }
    
    vts->clk_rw_mtx.wrlock();
    vts->vclk.increment_clock();
    vc::vclock req_timestamp = vts->vclk;
    assert(req_timestamp.clock.size() == NUM_VTS);
    vts->clk_rw_mtx.unlock();

    vts->tx_prog_mutex.lock();
    uint64_t req_id = vts->generate_id();
    vts->outstanding_progs.emplace(req_id, current_prog(clientID, req_timestamp.clock));
    vts->pend_prog_queue.emplace(req_id);
    vts->tx_prog_mutex.unlock();

    message::message msg_to_send;
    for (auto &batch_pair : initial_batches) {
        msg_to_send.prepare_message(message::NODE_PROG, pType, vt_id, req_timestamp, req_id, batch_pair.second);
        vts->comm.send(batch_pair.first, msg_to_send.buf);
    }
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> ::
    unpack_and_run_db(std::unique_ptr<message::message>)
{ }

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> ::
    unpack_context_reply_db(std::unique_ptr<message::message>)
{ }

// remove a completed node program from outstanding requests data structure
// update 'max_done_id' and 'max_done_clk' accordingly
// caution: need to hold vts->tx_prog_mutex
void
mark_req_finished(uint64_t req_id)
{
    assert(vts->seen_done_id.find(req_id) == vts->seen_done_id.end());
    vts->seen_done_id.emplace(req_id);
    if (vts->pend_prog_queue.top() == req_id) {
        assert(vts->max_done_id < vts->pend_prog_queue.top());
        vts->max_done_id = req_id;
        auto outstanding_prog_iter = vts->outstanding_progs.find(vts->max_done_id);
        assert(outstanding_prog_iter != vts->outstanding_progs.end());
        vts->max_done_clk = std::move(outstanding_prog_iter->second.vclk);
        vts->pend_prog_queue.pop();
        vts->outstanding_progs.erase(vts->max_done_id);
        while (!vts->pend_prog_queue.empty() && !vts->done_prog_queue.empty()
            && vts->pend_prog_queue.top() == vts->done_prog_queue.top()) {
            assert(vts->max_done_id < vts->pend_prog_queue.top());
            vts->max_done_id = vts->pend_prog_queue.top();
            outstanding_prog_iter = vts->outstanding_progs.find(vts->max_done_id);
            assert(outstanding_prog_iter != vts->outstanding_progs.end());
            vts->max_done_clk = std::move(outstanding_prog_iter->second.vclk);
            vts->pend_prog_queue.pop();
            vts->done_prog_queue.pop();
            vts->outstanding_progs.erase(vts->max_done_id);
        }
    } else {
        vts->done_prog_queue.emplace(req_id);
    }
}

void
server_loop(int thread_id)
{
    busybee_returncode ret;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> msg;
    uint64_t sender, tx_id;
    node_prog::prog_type pType;
    coordinator::hyper_stub *hstub = vts->hstub[thread_id];
    nmap::nmap_stub *nmap_cl = vts->nmap_client[thread_id];

    while (true) {
        vts->comm.quiesce_thread(thread_id);
        msg.reset(new message::message());
        ret = vts->comm.recv(&sender, &msg->buf);
        if (ret != BUSYBEE_SUCCESS && ret != BUSYBEE_TIMEOUT) {
            continue;
        } else {
            // good to go, unpack msg
            mtype = msg->unpack_message_type();
            sender -= ID_INCR;

            switch (mtype) {
                // client messages
                case message::CLIENT_TX_INIT: {
                    transaction::pending_tx tx;
                    std::vector<uint64_t> del_elems;
                    if (!vts->unpack_tx(nmap_cl, *msg, tx, sender, del_elems)) {
                        msg->prepare_message(message::CLIENT_TX_FAIL);
                        vts->comm.send(sender, msg->buf);
                    } else if (del_elems.size() > 0 && NUM_VTS > 1) {
                        prepare_del_transaction(tx, del_elems);
                    } else {
                        begin_transaction(tx, hstub);
                    }
                    break;
                }

                case message::PREP_DEL_TX: {
                    std::vector<uint64_t> del_elems;
                    uint64_t tx_id, tx_vt, client;
                    msg->unpack_message(message::PREP_DEL_TX, tx_vt, tx_id, client, del_elems);
                    vts->busy_mtx.lock();
                    for (uint64_t d: del_elems) {
                        assert(vts->deleted_elems.find(d) == vts->deleted_elems.end());
                        assert(vts->other_deleted_elems.find(d) == vts->other_deleted_elems.end());
                        assert(vts->busy_elems.find(d) == vts->busy_elems.end());
                        vts->other_deleted_elems[d] = client;
                    }
                    vts->busy_mtx.unlock();
                    msg->prepare_message(message::DONE_DEL_TX, tx_id);
                    vts->comm.send(tx_vt, msg->buf);
                    break;
                }

                case message::DONE_DEL_TX: {
                    uint64_t tx_id;
                    msg->unpack_message(message::DONE_DEL_TX, tx_id);
                    vts->busy_mtx.lock();
                    assert(vts->del_tx.find(tx_id) != vts->del_tx.end());
                    auto &cur_tx = vts->del_tx[tx_id];
                    if (--cur_tx.count == 0) {
                        // ready to run
                        auto tx = cur_tx.tx;
                        vts->del_tx.erase(tx_id);
                        vts->busy_mtx.unlock();
                        begin_transaction(tx, hstub);
                    } else {
                        vts->busy_mtx.unlock();
                    }
                    break;
                }

                case message::VT_CLOCK_UPDATE: {
                    uint64_t rec_vtid, rec_clock;
                    msg->unpack_message(message::VT_CLOCK_UPDATE, rec_vtid, rec_clock);
                    vts->clk_rw_mtx.wrlock();
                    vts->clk_updates++;
                    vts->vclk.update_clock(rec_vtid, rec_clock);
                    vts->clk_rw_mtx.unlock();
                    //msg->prepare_message(message::VT_CLOCK_UPDATE_ACK);
                    //vts->comm.send(rec_vtid, msg->buf);
                    break;
                }

                //case message::VT_CLOCK_UPDATE_ACK:
                //    vts->periodic_update_mutex.lock();
                //    vts->clock_update_acks++;
                //    assert(vts->clock_update_acks < NUM_VTS);
                //    vts->periodic_update_mutex.unlock();
                //    break;

                case message::VT_NOP_ACK: {
                    uint64_t shard_node_count, nop_qts, sid;
                    msg->unpack_message(message::VT_NOP_ACK, sender, nop_qts, shard_node_count);
                    sid = sender - SHARD_ID_INCR;
                    vts->periodic_update_mutex.lock();
                    if (nop_qts > vts->nop_ack_qts[sid]) {
                        vts->shard_node_count[sid] = shard_node_count;
                        vts->to_nop.set(sid);
                        vts->nop_ack_qts[sid] = nop_qts;
                    }
                    vts->periodic_update_mutex.unlock();
                    break;
                }

                case message::CLIENT_MSG_COUNT: {
                    vts->msg_count_mutex.lock();
                    vts->msg_count = 0;
                    vts->msg_count_acks = 0;
                    vts->msg_count_mutex.unlock();
                    for (uint64_t i = SHARD_ID_INCR; i < (SHARD_ID_INCR + NUM_SHARDS); i++) {
                        msg->prepare_message(message::MSG_COUNT, vt_id);
                        vts->comm.send(i, msg->buf);
                    }
                    break;
                }

                case message::CLIENT_NODE_COUNT: {
                    vts->periodic_update_mutex.lock();
                    msg->prepare_message(message::NODE_COUNT_REPLY, vts->shard_node_count);
                    vts->periodic_update_mutex.unlock();
                    vts->comm.send(sender, msg->buf);
                    break;
                }

                // shard messages
                case message::LOADED_GRAPH: {
                    uint64_t load_time;
                    msg->unpack_message(message::LOADED_GRAPH, load_time);
                    vts->graph_load_mutex.lock();
                    if (load_time > vts->max_load_time) {
                        vts->max_load_time = load_time;
                    }
                    if (++vts->load_count == NUM_SHARDS) {
                        WDEBUG << "Graph loaded on all machines, time taken = " << vts->max_load_time << " nanosecs." << std::endl;
                    }
                    vts->graph_load_mutex.unlock();
                    break;
                }

                case message::TX_DONE:
                    msg->unpack_message(message::TX_DONE, tx_id);
                    end_transaction(tx_id, hstub);
                    break;

                case message::START_MIGR: {
                    uint64_t hops = UINT64_MAX;
                    msg->prepare_message(message::MIGRATION_TOKEN, hops, vt_id);
                    vts->comm.send(START_MIGR_ID, msg->buf); 
                    break;
                }

                case message::ONE_STREAM_MIGR: {
                    uint64_t hops = NUM_SHARDS;
                    vts->migr_mutex.lock();
                    vts->migr_client = sender;
                    vts->migr_mutex.unlock();
                    msg->prepare_message(message::MIGRATION_TOKEN, hops, vt_id);
                    vts->comm.send(START_MIGR_ID, msg->buf);
                    break;
                }

                case message::MIGRATION_TOKEN: {
                    vts->migr_mutex.lock();
                    uint64_t client = vts->migr_client;
                    vts->migr_mutex.unlock();
                    msg->prepare_message(message::DONE_MIGR);
                    vts->comm.send(client, msg->buf);
                    WDEBUG << "Shard node counts are:";
                    for (uint64_t &x: vts->shard_node_count) {
                        std::cerr << " " << x;
                    }
                    std::cerr << std::endl;
                    break;
                }

                case message::CLIENT_NODE_PROG_REQ:
                    msg->unpack_partial_message(message::CLIENT_NODE_PROG_REQ, pType);
                    node_prog::programs.at(pType)->unpack_and_start_coord(std::move(msg), sender, nmap_cl);
                    break;

                // node program response from a shard
                case message::NODE_PROG_RETURN: {
                    uint64_t req_id;
                    node_prog::prog_type type;
                    msg->unpack_partial_message(message::NODE_PROG_RETURN, type, req_id); // don't unpack rest
                    vts->tx_prog_mutex.lock();
                    auto outstanding_prog_iter = vts->outstanding_progs.find(req_id);
                    if (outstanding_prog_iter != vts->outstanding_progs.end()) { 
                        uint64_t client = outstanding_prog_iter->second.client;
                        vts->done_reqs[type].emplace(req_id, std::bitset<NUM_SHARDS>());
                        vts->comm.send(client, msg->buf);
                        mark_req_finished(req_id);
                    } else {
                        WDEBUG << "node prog return for already completed or never existed req id" << std::endl;
                    }
                    vts->tx_prog_mutex.unlock();
                    break;
                }

                case message::MSG_COUNT: {
                    uint64_t shard, msg_count;
                    msg->unpack_message(message::MSG_COUNT, shard, msg_count);
                    vts->msg_count_mutex.lock();
                    vts->msg_count += msg_count;
                    if (++vts->msg_count_acks == NUM_SHARDS) {
                        WDEBUG << "Msg count = " << vts->msg_count << std::endl;
                    }
                    vts->msg_count_mutex.unlock();
                    break;
                }

                default:
                    std::cerr << "unexpected msg type " << mtype << std::endl;
            }
        }
    }
}

void
server_manager_link_loop(po6::net::hostname sm_host)
{
    // Most of the following code has been 'borrowed' from
    // Robert Escriva's HyperDex.
    // see https://github.com/rescrv/HyperDex for the original code.

    vts->sm_stub.set_server_manager_address(sm_host.address.c_str(), sm_host.port);

    if (!vts->sm_stub.register_id(vts->server, *vts->comm.get_loc()))
    {
        return;
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
        vts->config = new_config;
        if (!vts->first_config) {
            vts->first_config = true;
            vts->first_config_cond.signal();
        } else {
            while (!vts->vts_init) {
                vts->vts_init_cond.wait();
            }
            vts->reconfigure();
        }
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
    else if (vts->sm_stub.should_exit() && !vts->sm_stub.config().exists(vts->server))
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
    int ret = sigaction(signum, &sa, NULL);
    assert(ret == 0);
}

int
main(int argc, char *argv[])
{
    if (argc < 2 || argc > 3) {
        WDEBUG << "Usage,   primary vt:" << argv[0] << " <vector_timestamper_id>" << std::endl
               << "          backup vt:" << argv[0] << " <vector_timestamper_id> <backup_number>" << std::endl; 
        return -1;
    }

    install_signal_handler(SIGINT, end_program);
    install_signal_handler(SIGHUP, end_program);
    install_signal_handler(SIGTERM, end_program);

    sigset_t ss;
    if (sigfillset(&ss) < 0) {
        WDEBUG << "sigfillset failed" << std::endl;
        return -1;
    }
    sigdelset(&ss, SIGPROF);
    if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0) {
        WDEBUG << "pthread sigmask failed" << std::endl;
        return -1;
    }

    // vt setup
    vt_id = atoi(argv[1]);
    if (argc == 3) {
        vts = new coordinator::timestamper(vt_id, atoi(argv[2]));
        assert((atoi(argv[2]) - vt_id) % (NUM_VTS+NUM_SHARDS) == 0);
    } else {
        vts = new coordinator::timestamper(vt_id, vt_id);
    }

    // server manager link
    std::thread sm_thr(server_manager_link_loop,
        po6::net::hostname(SERVER_MANAGER_IPADDR, SERVER_MANAGER_PORT));
    sm_thr.detach();

    vts->config_mutex.lock();

    // wait for first config to arrive from server manager
    while (!vts->first_config) {
        vts->first_config_cond.wait();
    }

    // registered this server with server_manager, config has fairly recent value
    vts->init();
    vts->vts_init = true;
    vts->vts_init_cond.signal();

    vts->config_mutex.unlock();

    // start all threads
    std::thread *thr;
    for (int i = 0; i < NUM_THREADS; i++) {
        thr = new std::thread(server_loop, i);
        thr->detach();
    }

    if (argc == 3) {
        // wait till this server becomes primary vt
        vts->config_mutex.lock();
        while (!vts->active_backup) {
            vts->backup_cond.wait();
        }
        vts->config_mutex.unlock();
        WDEBUG << "backup " << atoi(argv[2]) << " now primary for vt " << vt_id << std::endl;
        vts->restore_backup();
    } else {
        // this server is primary vt, start now
        std::cout << "Vector timestamper " << vt_id << std::endl;
    }

    // initial wait for all vector timestampers to start
    // TODO change this to use config pushed by server manager
    timespec sleep_time;
    sleep_time.tv_sec =  INITIAL_TIMEOUT_NANO / NANO;
    sleep_time.tv_nsec = INITIAL_TIMEOUT_NANO % NANO;
    int ret = clock_nanosleep(CLOCK_REALTIME, 0, &sleep_time, NULL);
    assert(ret == 0);
    WDEBUG << "Initial setup delay complete" << std::endl;

    UNUSED(ret);

    // periodic vector clock update to other timestampers
    std::thread clk_update_thr(clk_update_function);
    clk_update_thr.detach();

    // periodic nops to shard
    nop_function();
}

#undef weaver_debug_
