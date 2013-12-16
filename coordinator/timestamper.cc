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
#include <signal.h>

#define __WEAVER_DEBUG__
#include "common/vclock.h"
#include "common/transaction.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
//#include "node_prog/triangle_program.h"
#include "timestamper.h"

static coordinator::timestamper *vts;
static uint64_t vt_id;

// SIGINT handler
void
end_program(int param)
{
    std::cerr << "Ending program, param = " << param << std::endl;
    exit(0);
}

// expects an input of list of writes that are part of this transaction
// for all writes, node mapper lookups should have already been performed
// for create requests, instead of lookup an entry for new handle should have been inserted
inline void
begin_transaction(transaction::pending_tx &tx)
{
    message::message msg;
    std::vector<transaction::pending_tx> tx_vec(NUM_SHARDS, transaction::pending_tx());

    vts->mutex.lock();
    for (std::shared_ptr<transaction::pending_update> upd: tx.writes) {
        vts->qts.at(upd->loc1-SHARD_ID_INCR)++;
        upd->qts = vts->qts;
        tx_vec[upd->loc1-SHARD_ID_INCR].writes.emplace_back(upd);
    }
    vts->vclk.increment_clock();
    tx.timestamp = vts->vclk;
    tx.id = vts->generate_id();
    vts->tx_replies[tx.id].client_id = tx.client_id;
    // record txs as outstanding for reply bookkeeping
    for (uint64_t i = 0; i < NUM_SHARDS; i++) {
        if (!tx_vec[i].writes.empty()) {
            vts->tx_replies[tx.id].count++;
        }
    }
    vts->mutex.unlock();

    // send tx batches
    for (uint64_t i = 0; i < NUM_SHARDS; i++) {
        if (!tx_vec[i].writes.empty()) {
            tx_vec[i].timestamp = tx.timestamp;
            tx_vec[i].id = tx.id;
            message::prepare_message(msg, message::TX_INIT, vt_id, tx.timestamp, tx_vec[i].writes.at(0)->qts, tx.id, tx_vec[i].writes);
            vts->send(tx_vec[i].writes.at(0)->loc1, msg.buf);
        }
    }
}

// decrement reply count. if all replies have been received, ack to client
inline void
end_transaction(uint64_t tx_id)
{
    vts->mutex.lock();
    if (--vts->tx_replies.at(tx_id).count == 0) {
        // done tx
        uint64_t client_id = vts->tx_replies[tx_id].client_id;
        vts->tx_replies.erase(tx_id);
        vts->mutex.unlock();
        message::message msg;
        message::prepare_message(msg, message::CLIENT_TX_DONE);
        vts->send(client_id, msg.buf);
    } else {
        vts->mutex.unlock();
    }
}

// send nop to shard which has id 'shard_id'
// nops also include state cleanup related info:
// 'max_done_id' is the maximum request id timestamped by this VT which has completed
// 'done_reqs' is a list of request ids timestamped by this VT which have completed
inline void
nop(uint64_t shard_id)
{
    vts->mutex.lock();
    vts->vclk.increment_clock();
    vts->qts[shard_id - SHARD_ID_INCR]++;
    vc::qtimestamp_t new_qts(vts->qts);
    vc::vclock vclk(vt_id, vts->vclk.clock);
    uint64_t req_id = vts->generate_id();
    uint64_t max_done_id = vts->max_done_id;
    std::vector<std::pair<uint64_t, node_prog::prog_type>> done_reqs;
    std::vector<uint64_t> del_done_reqs;
    uint64_t shard_idx = shard_id - SHARD_ID_INCR;
    for (auto &x: vts->done_reqs) {
        // x.first = node prog type
        // x.second = unordered_map <req_id -> bitset<NUM_SHARDS>>
        for (auto &reply: x.second) {
            // reply.first = req_id
            // reply.second = bitset<NUM_SHARDS>
            if (!reply.second[shard_idx]) {
                reply.second.set(shard_idx);
                done_reqs.emplace_back(std::make_pair(reply.first, x.first));
            }
            if (reply.second.all()) {
                del_done_reqs.emplace_back(reply.first);
            }
        }
        for (auto &del: del_done_reqs) {
            x.second.erase(del);
        }
    }
    vts->mutex.unlock();

    message::message msg;
    message::prepare_message(msg, message::VT_NOP, vt_id, vclk,
            new_qts, req_id, done_reqs, max_done_id);
    vts->send(shard_id, msg.buf);
}

// method to update vector clock at other VTs
// called periodically from server_loop()
inline void
periodic_update()
{
    bool first_update = true;
    
    vts->periodic_update_mutex.lock();
    uint64_t cur_time_millis = wclock::get_time_elapsed_millis(vts->tspec);
    uint64_t diff_millis = cur_time_millis - vts->nop_time_millis;

    if (diff_millis > VT_NOP_TIMEOUT) {
        vts->nop_time_millis = cur_time_millis;
        if (vts->first_clock_update) {
            uint64_t first_diff_millis = cur_time_millis - vts->first_nop_time_millis;
            first_update = first_diff_millis > VT_INITIAL_CLKUPDATE_DELAY;
        }
        if (first_update && vts->clock_update_acks == (NUM_VTS-1)) {
            vts->first_clock_update = false;
            vts->clock_update_acks = 0;

            vts->mutex.lock();
            uint64_t my_clock = vts->vclk.clock[vt_id];
            vts->mutex.unlock();

            message::message msg;
            for (uint64_t i = 0; i < NUM_VTS; i++) {
                if (i == vt_id) {
                    continue;
                }
                message::prepare_message(msg, message::VT_CLOCK_UPDATE, vt_id, my_clock);
                vts->send(i, msg.buf);
            }
        }
    }

    vts->periodic_update_mutex.unlock();
}

// alternate nop/periodic update implementation
// single dedicated thread which wakes up after given timeout, sends updates, and sleeps
inline void
timer_function()
{
    timespec sleep_time;
    int sleep_ret;
    int sleep_flags = 0;
    vc::vclock vclk(vt_id, 0);
    vc::qtimestamp_t qts;
    uint64_t req_id, max_done_id;
    typedef std::vector<std::pair<uint64_t, node_prog::prog_type>> done_req_t;
    std::vector<done_req_t> done_reqs(NUM_SHARDS, done_req_t());
    std::vector<uint64_t> del_done_reqs;
    message::message msg;
    sleep_time.tv_sec = VT_NOP_TIMEOUT / VT_NANO;
    sleep_time.tv_nsec = VT_NOP_TIMEOUT % VT_NANO;

    vts->periodic_update_mutex.lock();
    while (true) {
        //WDEBUG << "loop in timer function()\n";
        sleep_ret = clock_nanosleep(CLOCK_REALTIME, sleep_flags, &sleep_time, NULL);
        assert(sleep_ret == 0 || sleep_ret == EINTR);

        // update vclock at other timestampers
        if (vts->clock_update_acks == (NUM_VTS-1) && NUM_VTS > 1) {
            vts->clock_update_acks = 0;
            vts->mutex.lock();
            vclk.clock = vts->vclk.clock;
            vts->mutex.unlock();
            for (uint64_t i = 0; i < NUM_VTS; i++) {
                if (i == vt_id) {
                    continue;
                }
                message::prepare_message(msg, message::VT_CLOCK_UPDATE, vt_id, vclk.clock[vt_id]);
                vts->send(i, msg.buf);
            }
            WDEBUG << "updating vector clock at other shards\n";
        } else if (NUM_VTS > 1) {
            vts->periodic_cond.wait();
        }

        // send nops and state cleanup info to shards
        if (vts->to_nop.any()) {
            vts->mutex.lock();
            vts->vclk.increment_clock();
            vclk.clock = vts->vclk.clock;
            req_id = vts->generate_id();
            max_done_id = vts->max_done_id;
            del_done_reqs.clear();
            for (uint64_t shard_id = 0; shard_id < NUM_SHARDS; shard_id++) {
                if (vts->to_nop[shard_id]) {
                    vts->qts[shard_id]++;
                    done_reqs[shard_id].clear();
                }
            }
            qts = vts->qts;
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
            vts->mutex.unlock();

            for (uint64_t shard_id = 0; shard_id < NUM_SHARDS; shard_id++) {
                if (vts->to_nop[shard_id]) {
                    message::message msg;
                    message::prepare_message(msg, message::VT_NOP, vt_id, vclk,
                            qts, req_id, done_reqs[shard_id], max_done_id, vts->shard_node_count);
                    vts->send(shard_id + SHARD_ID_INCR, msg.buf);
                }
            }
            vts->to_nop.reset();
        } else {
            vts->periodic_cond.wait();
        }
    }
    vts->periodic_update_mutex.unlock();
}

// unpack client message for a node program, prepare shard msges, and send out
template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, int thread_id)
{
    node_prog::prog_type pType;
    std::vector<std::pair<uint64_t, ParamsType>> initial_args;

    message::unpack_message(*msg, message::CLIENT_NODE_PROG_REQ, pType, initial_args);
    
    // map from locations to a list of start_node_params to send to that shard
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> initial_batches; 
    bool global_req = false;

    // lookup mappings
    std::unordered_map<uint64_t, uint64_t> request_element_mappings;
    std::unordered_set<uint64_t> mappings_to_get;
    for (auto &initial_arg : initial_args) {
        uint64_t c_id = initial_arg.first;
        if (c_id == -1) { // max uint64_t means its a global thing like triangle count
            WDEBUG << "timestamper GOIN GLOBAL" << std::endl;
            assert(mappings_to_get.empty()); // dont mix global req with normal nodes
            assert(initial_args.size() == 1);
            global_req = true;
            break;
        }
        mappings_to_get.insert(c_id);
    }
    if (!mappings_to_get.empty()) {
        auto results = vts->nmap_client[thread_id]->get_mappings(mappings_to_get, true);
        assert(results.size() == mappings_to_get.size());
        for (auto &toAdd : results) {
            request_element_mappings.emplace(toAdd);
        }
    }

    if (global_req) {
        // send copy of params to each shard
        for (int i = 0; i < NUM_SHARDS; i++) {
            initial_batches[i + SHARD_ID_INCR].emplace_back(std::make_tuple(initial_args[0].first,
                    initial_args[0].second, db::element::remote_node()));
        }
    } else { // regular style node program
        for (std::pair<uint64_t, ParamsType> &node_params_pair: initial_args) {
            uint64_t loc = request_element_mappings[node_params_pair.first];
            initial_batches[loc].emplace_back(std::make_tuple(node_params_pair.first,
                    std::move(node_params_pair.second), db::element::remote_node()));
        }
    }
    
    vts->mutex.lock();
    vts->vclk.increment_clock();
    vc::vclock req_timestamp = vts->vclk;
    assert(req_timestamp.clock.size() == NUM_VTS);
    uint64_t req_id = vts->generate_id();

/*
    if (global_req) {
        vts->outstanding_triangle_progs.emplace(req_id, std::make_pair(NUM_SHARDS, node_prog::triangle_params()));
    }
    */
    vts->outstanding_node_progs.emplace(req_id, clientID);
    vts->outstanding_req_ids.emplace(req_id);
    vts->mutex.unlock();

    message::message msg_to_send;
    for (auto &batch_pair : initial_batches) {
        message::prepare_message(msg_to_send, message::NODE_PROG, pType, global_req, vt_id, req_timestamp, req_id, batch_pair.second);
        vts->send(batch_pair.first, msg_to_send.buf);
    }
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> ::
    unpack_and_run_db(std::unique_ptr<message::message>)
{ }

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> ::
    unpack_context_reply_db(std::unique_ptr<message::message>)
{ }

// remove a completed node program from outstanding requests data structure
// update 'max_done_id' accordingly
inline void mark_req_finished(uint64_t req_id) {
    if (vts->outstanding_req_ids.top() == req_id) {
        assert(vts->max_done_id < vts->outstanding_req_ids.top());
        vts->max_done_id = req_id;
        vts->outstanding_req_ids.pop();
        while (!vts->outstanding_req_ids.empty() && !vts->done_req_ids.empty()
                && vts->outstanding_req_ids.top() == vts->done_req_ids.top()) {
            assert(vts->max_done_id < vts->outstanding_req_ids.top());
            vts->max_done_id = vts->outstanding_req_ids.top();
            vts->outstanding_req_ids.pop();
            vts->done_req_ids.pop();
        }
    } else {
        vts->done_req_ids.emplace(req_id);
    }
}

void
server_loop(int thread_id)
{
    busybee_returncode ret;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> msg;
    uint64_t sender, tx_id;
    node_prog::prog_type pType;

    while (true) {
        msg.reset(new message::message());
        ret = vts->bb->recv(&sender, &msg->buf);
        if (ret != BUSYBEE_SUCCESS && ret != BUSYBEE_TIMEOUT) {
            WDEBUG << "msg recv error: " << ret << std::endl;
            continue;
        //} else if (ret == BUSYBEE_TIMEOUT) {
        //    periodic_update();
        //    continue;
        } else {
            // good to go, unpack msg
            uint64_t _size;
            msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code >> _size;
            mtype = (enum message::msg_type)code;
            sender -= ID_INCR;

            switch (mtype) {
                // client messages
                case message::CLIENT_TX_INIT: {
                    transaction::pending_tx tx;
                    vts->unpack_tx(*msg, tx, sender, thread_id);
                    begin_transaction(tx);
                    break;
                }

                case message::VT_CLOCK_UPDATE: {
                    uint64_t rec_vtid, rec_clock;
                    message::unpack_message(*msg, message::VT_CLOCK_UPDATE, rec_vtid, rec_clock);
                    vts->mutex.lock();
                    vts->vclk.update_clock(rec_vtid, rec_clock);
                    vts->mutex.unlock();
                    message::prepare_message(*msg, message::VT_CLOCK_UPDATE_ACK);
                    vts->send(rec_vtid, msg->buf);
                    break;
                }

                case message::VT_CLOCK_UPDATE_ACK:
                    vts->periodic_update_mutex.lock();
                    vts->clock_update_acks++;
                    assert(vts->clock_update_acks < NUM_VTS);
                    vts->periodic_cond.signal();
                    vts->periodic_update_mutex.unlock();
                    break;

                case message::VT_NOP_ACK: {
                    uint64_t shard_node_count;
                    message::unpack_message(*msg, message::VT_NOP_ACK, sender, shard_node_count);
                    vts->periodic_update_mutex.lock();
                    vts->shard_node_count[sender - SHARD_ID_INCR] = shard_node_count;
                    vts->to_nop.set(sender - SHARD_ID_INCR);
                    vts->periodic_cond.signal();
                    vts->periodic_update_mutex.unlock();
                    break;
                }

                // shard messages
                case message::LOADED_GRAPH: {
                    uint64_t load_time;
                    message::unpack_message(*msg, message::LOADED_GRAPH, load_time);
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
                    message::unpack_message(*msg, message::TX_DONE, tx_id);
                    end_transaction(tx_id);
                    break;

                case message::START_MIGR: {
                    uint64_t hops = MAX_UINT64;
                    message::prepare_message(*msg, message::MIGRATION_TOKEN, hops, vt_id);
                    vts->send(START_MIGR_ID, msg->buf); 
                    break;
                }

                case message::ONE_STREAM_MIGR: {
                    uint64_t hops = NUM_SHARDS;
                    vts->mutex.lock();
                    vts->migr_client = sender;
                    vts->mutex.unlock();
                    message::prepare_message(*msg, message::MIGRATION_TOKEN, hops, vt_id);
                    vts->send(START_MIGR_ID, msg->buf);
                    break;
                }

                case message::MIGRATION_TOKEN: {
                    vts->mutex.lock();
                    uint64_t client = vts->migr_client;
                    vts->mutex.unlock();
                    message::prepare_message(*msg, message::DONE_MIGR);
                    vts->send(client, msg->buf);
                    WDEBUG << "Shard node counts are:";
                    for (uint64_t &x: vts->shard_node_count) {
                        std::cerr << " " << x;
                    }
                    std::cerr << std::endl;
                    break;
                }

                case message::CLIENT_NODE_PROG_REQ:
                    message::unpack_partial_message(*msg, message::CLIENT_NODE_PROG_REQ, pType);
                    node_prog::programs.at(pType)->unpack_and_start_coord(std::move(msg), sender, thread_id);
                    break;

                // node program response from a shard
                case message::NODE_PROG_RETURN:
                    uint64_t req_id;
                    node_prog::prog_type type;
                    message::unpack_partial_message(*msg, message::NODE_PROG_RETURN, type, req_id); // don't unpack rest
                    vts->mutex.lock();
                    if (vts->outstanding_node_progs.find(req_id) != vts->outstanding_node_progs.end()) { // TODO: change to .count (AD: why?)
                        uint64_t client_to_ret = vts->outstanding_node_progs.at(req_id);

/*
                        if (vts->outstanding_triangle_progs.count(req_id) > 0) { // a triangle prog response
                            std::pair<int, node_prog::triangle_params>& p = vts->outstanding_triangle_progs.at(req_id);
                            p.first--; // count of shards responded

                            // unpack whole thing
                            std::pair<uint64_t, node_prog::triangle_params> tempPair;
                            message::unpack_message(*msg, message::NODE_PROG_RETURN, type, req_id, tempPair);

                            uint64_t oldval = p.second.num_edges;
                            p.second.num_edges += tempPair.second.num_edges;

                            // XXX temp make sure reference worked (AD: let's fix this)
                            assert(vts->outstanding_triangle_progs.at(req_id).second.num_edges - tempPair.second.num_edges == oldval); 

                            if (p.first == 0) { // all shards responded
                                // send back to client
                                vts->done_reqs[type].emplace(req_id, std::bitset<NUM_SHARDS>());
                                tempPair.second.num_edges = p.second.num_edges;
                                message::prepare_message(*msg, message::NODE_PROG_RETURN, type, req_id, tempPair);
                                vts->send(client_to_ret, msg->buf);
                                vts->outstanding_node_progs.erase(req_id);
                                mark_req_finished(req_id);
                            }
                        } else {*/ // just a normal node program
                            vts->done_reqs[type].emplace(req_id, std::bitset<NUM_SHARDS>());
                            vts->send(client_to_ret, msg->buf);
                            vts->outstanding_node_progs.erase(req_id);
                            mark_req_finished(req_id);
                        //}
                    } else {
                        WDEBUG << "node prog return for already completed ornever existed req id" << std::endl;
                    }
                    vts->mutex.unlock();
                    break;

                default:
                    std::cerr << "unexpected msg type " << mtype << std::endl;
            }
            //periodic_update();
        }
    }
}


int
main(int argc, char *argv[])
{
    std::thread *thr;
    signal(SIGINT, end_program);
    if (argc != 2) {
        WDEBUG << "Usage: " << argv[0] << " <vector_timestamper_id>" << std::endl;
        return -1;
    }
    vt_id = atoi(argv[1]);
    vts = new coordinator::timestamper(vt_id);
    std::cout << "Vector timestamper " << vt_id << std::endl;
    for (int i = 0; i < NUM_THREADS-1; i++) {
        thr = new std::thread(server_loop, i);
        thr->detach();
    }

    // send out nops to each shard
    //for (uint64_t shard = 0; shard < NUM_SHARDS; shard++) {
    //    nop(shard + SHARD_ID_INCR);
    //}
    //server_loop(NUM_THREADS-1);

    // call periodic thread function
    timer_function();
}
