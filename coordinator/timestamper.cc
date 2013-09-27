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
    DEBUG << "beginning tx " << std::endl;
    message::message msg;
    //vc::vclock clock;
    std::vector<transaction::pending_tx> tx_vec(NUM_SHARDS, transaction::pending_tx());
    vts->mutex.lock();
    for (std::shared_ptr<transaction::pending_update> upd: tx.writes) {
        //DEBUG << "updating qts for shard " << (upd->loc1-SHARD_ID_INCR) << std::endl;
        vts->qts.at(upd->loc1-SHARD_ID_INCR)++; // TODO what about edge create requests, loc2?
        upd->qts = vts->qts;
        tx_vec.at(upd->loc1-SHARD_ID_INCR).writes.emplace_back(upd);
    }
    vts->vclk.increment_clock();
    tx.timestamp = vts->vclk;
    tx.id = vts->generate_id();
    vts->tx_replies.emplace(tx.id, coordinator::tx_reply());
    vts->tx_replies.at(tx.id).client_id = tx.client_id;
    // send tx in per shard batches
    for (uint64_t i = 0; i < NUM_SHARDS; i++) {
        if (!tx_vec.at(i).writes.empty()) {
            tx_vec.at(i).timestamp = tx.timestamp;
            tx_vec.at(i).id = tx.id;
            message::prepare_message(msg, message::TX_INIT, vt_id, tx.timestamp, tx_vec.at(i).writes.at(0)->qts, tx.id, tx_vec.at(i).writes);
            vts->send(tx_vec.at(i).writes.at(0)->loc1, msg.buf);
            vts->tx_replies.at(tx.id).count++;
        }
    }
    DEBUG << "sent tx\n";
    vts->mutex.unlock(); // TODO: move sending out of critical section
}

// decrement reply count. if all replies have been received, ack to client
inline void
end_transaction(uint64_t tx_id)
{
    vts->mutex.lock();
    DEBUG << "Got end tx\n";
    if (--vts->tx_replies.at(tx_id).count == 0) {
        // done tx
        DEBUG << "Actually ending tx\n";
        uint64_t client_id = vts->tx_replies.at(tx_id).client_id;
        vts->tx_replies.erase(tx_id);
        vts->mutex.unlock();
        message::message msg;
        message::prepare_message(msg, message::CLIENT_TX_DONE);
        vts->send(client_id, msg.buf);
    } else {
        vts->mutex.unlock();
    }
}

// periodically send nops to shards to keep queues moving
// also send vector clock updates to other timestampers
inline void
periodic_update()
{
    vts->periodic_update_mutex.lock();
    vts->mutex.lock();
    uint64_t req_id;
    uint64_t cur_time_millis = wclock::get_time_elapsed_millis(vts->tspec);
    uint64_t first_diff_millis = cur_time_millis - vts->first_nop_time_millis;
    uint64_t cur_time_nanos = wclock::get_time_elapsed(vts->tspec);
    uint64_t diff_nanos = cur_time_nanos - vts->nop_time_nanos;
    if (diff_nanos > VT_NOP_TIMEOUT) {
        //std::cout << "cur time nano = " << cur_time_nanos << ", nop_time_nanos " << vts->nop_time_nanos << ", diff nanos " << diff_nanos << ", TIMEOUT " << VT_NOP_TIMEOUT << std::endl;
        // send nops to each shard
        vts->nop_time_millis = cur_time_millis;
        vts->nop_time_nanos = cur_time_nanos;
        std::vector<bool> to_nop = vts->to_nop;
        for (uint64_t i = 0; i < NUM_SHARDS; i++) {
            if (vts->to_nop[i]) {
                vts->qts[i]++;
                vts->to_nop[i] = false;
            }
        }
        vc::qtimestamp_t new_qts = vts->qts;
        //if (vts->nop_acks == NUM_SHARDS) {
        //    for (auto &qts: vts->qts) {
        //        qts++;
        //    }
        //}
        vts->vclk.increment_clock();
        vc::vclock vclk = vts->vclk;
        req_id = vts->generate_id();
        message::message msg;
        //if (vts->nop_acks == NUM_SHARDS) {
            std::vector<std::pair<uint64_t, node_prog::prog_type>> done_reqs;
            uint64_t max_done_id = vts->max_done_id;
            for (auto &x: vts->done_reqs) {
                node_prog::prog_type type = x.first;
                for (uint64_t id: x.second) {
                    done_reqs.emplace_back(std::make_pair(id, type));
                }
                x.second.clear();
            }
            vts->mutex.unlock();
            for (uint64_t i = 0; i < NUM_SHARDS; i++) {
                if (to_nop[i]) {
                    message::prepare_message(msg, message::VT_NOP, vt_id, vclk, new_qts, req_id, done_reqs, max_done_id);
                    vts->send(i + SHARD_ID_INCR, msg.buf);
                }
            }
            //vts->nop_acks = 0;
        //} else {
        //    vts->mutex.unlock();
        //}
        //if (vts->first_clock_update) {
        //    DEBUG << "clock update acks " << vts->clock_update_acks << std::endl;
        //    DEBUG << "diff " << diff << ", initial clock update delay " << VT_INITIAL_CLKUPDATE_DELAY << std::endl;
        //    DEBUG << "first diff " << first_diff << ", initial clock update delay " << VT_INITIAL_CLKUPDATE_DELAY << std::endl;
        //}
        // first check is an ugly hack to make sure all VTs are up before sending out clock updates
        // second check is to ensure all VTs acked previous update before sending out new update
        if (((vts->first_clock_update && first_diff_millis > VT_INITIAL_CLKUPDATE_DELAY) || !vts->first_clock_update)
        && (vts->clock_update_acks == (NUM_VTS-1))) {
            vts->first_clock_update = false;
            //DEBUG << "sending clock update now, clock update acks " << vts->clock_update_acks << std::endl;
            vts->clock_update_acks = 0;
            for (uint64_t i = 0; i < NUM_VTS; i++) {
                if (i == vt_id) {
                    continue;
                }
                message::prepare_message(msg, message::VT_CLOCK_UPDATE, vt_id, vclk.clock.at(vt_id));
                vts->send(i, msg.buf);
            }
        }
    } else {
        vts->mutex.unlock();
    }
    vts->periodic_update_mutex.unlock();
}

// node program stuff
template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, int thread_id)
{
    DEBUG << "starting node program on timestamper" << std::endl;
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
    DEBUG << "timestamper done looking up element mappings for node program" << std::endl;

    if (global_req) {
        // send copy of params to each shard
        for (int i = 0; i < NUM_SHARDS; i++) {
            initial_batches[i + SHARD_ID_INCR].emplace_back(std::make_tuple(initial_args[0].first, initial_args[0].second, db::element::remote_node()));
        }
    } else { // regular style node program
        for (std::pair<uint64_t, ParamsType> &node_params_pair : initial_args) { // TODO: change to params pointer so we can avoid potential copy?
            initial_batches[request_element_mappings[node_params_pair.first]].emplace_back(std::make_tuple(node_params_pair.first,
                        std::move(node_params_pair.second), db::element::remote_node())); // constructor
        }
    }
    vts->mutex.lock();
    vts->vclk.increment_clock();
    vc::vclock req_timestamp =  vts->vclk;
    assert(req_timestamp.clock.size() == NUM_VTS);
    uint64_t req_id = vts->generate_id();

    message::message msg_to_send;
    std::vector<uint64_t> empty_vector;
    std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> empty_tuple_vector;
    DEBUG << "starting node prog " << req_id << ", recd from client\t" << std::endl;
    uint64_t zero = 0;
    for (auto &batch_pair : initial_batches) {
        message::prepare_message(msg_to_send, message::NODE_PROG, pType, global_req, vt_id, req_timestamp, 
                req_id, batch_pair.second, /*empty_tuple_vector,*/ zero, zero);
        vts->send(batch_pair.first, msg_to_send.buf); // TODO: can we send out of critical section?
    }
    DEBUG << "sent to shards" << std::endl;
    if (global_req) {
        vts->outstanding_triangle_progs.emplace(std::make_pair(req_id, clientID));
    } else {
        vts->outstanding_node_progs.emplace(std::make_pair(req_id, clientID));
    }
    vts->outstanding_req_ids.emplace(req_id);
    vts->mutex.unlock();
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> ::
    unpack_and_run_db(std::unique_ptr<message::message>)
{ }

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
            DEBUG << "msg recv error: " << ret << std::endl;
            continue;
        } else if (ret == BUSYBEE_TIMEOUT) {
            //if (thread_id == 0) {
                periodic_update();
            //}
            continue;
        } else {
            // good to go, unpack msg
            msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
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
                    vts->periodic_update_mutex.unlock();
                    break;

                case message::VT_NOP_ACK:
                    message::unpack_message(*msg, message::VT_NOP_ACK, sender);
                    vts->periodic_update_mutex.lock();
                    vts->to_nop.at(sender-SHARD_ID_INCR) = true;
                    //vts->nop_acks++;
                    //assert(vts->nop_acks <= NUM_SHARDS);
                    vts->periodic_update_mutex.unlock();
                    break;

                // shard messages
                case message::TX_DONE:
                    message::unpack_message(*msg, message::TX_DONE, tx_id);
                    end_transaction(tx_id);
                    break;

                case message::START_MIGR:
                    message::prepare_message(*msg, message::MIGRATION_TOKEN);
                    vts->send(START_MIGR_ID, msg->buf); 
                    break; 

                case message::CLIENT_NODE_PROG_REQ:
                    message::unpack_message(*msg, message::CLIENT_NODE_PROG_REQ, pType);
                    node_prog::programs.at(pType)->unpack_and_start_coord(std::move(msg), sender, thread_id);
                    break;

                // response from a shard
                case message::NODE_PROG_RETURN:
                    uint64_t req_id;
                    node_prog::prog_type type;
                    message::unpack_message(*msg, message::NODE_PROG_RETURN, type, req_id); // don't unpack rest
                    vts->mutex.lock();
                    vts->done_reqs[type].emplace(req_id);
                    if (vts->outstanding_node_progs.find(req_id) != vts->outstanding_node_progs.end()) { // TODO: change to .count
                        uint64_t client_to_ret = vts->outstanding_node_progs.at(req_id);
                        vts->send(client_to_ret, msg->buf);
                        vts->outstanding_node_progs.erase(req_id);
                        mark_req_finished(req_id);
                    } else if (vts->outstanding_triangle_progs.count(req_id) > 0) {
                        std::pair<int, node_prog::triangle_params>& p =vts->outstanding_triangle_progs.at(req_id);
                        p.first--;
                        uint64_t ignore_req_id;
                        node_prog::prog_type ignore_type;
                        std::pair<uint64_t, node_prog::triangle_params> tempPair;
                        message::unpack_message(msg, message::NODE_PROG_RETURN, ignore_type, ignore_req_id, tempPair);
                        p.second.num_edges += temp.Pair.second.num_edges;
                        if (p.first == 0) { // all shards responded
                        // send back
                        mark_req_finished(req_id);
                        }
                    } else {
                        DEBUG << "node prog return for already completed ornever existed req id" << std::endl;
                    }
                    vts->mutex.unlock();
                    break;

                default:
                    std::cerr << "unexpected msg type " << mtype << std::endl;
            }
            //if (thread_id == 0) {
                periodic_update();
            //}
        }
    }
}

// TODO permanent deletion of deleted and migrated nodes and state for completed node progs
inline void
coord_daemon_initiate()
{ }

int
main(int argc, char *argv[])
{
    std::thread *thr;
    signal(SIGINT, end_program);
    if (argc != 2) {
        DEBUG << "Usage: " << argv[0] << " <vector_timestamper_id>" << std::endl;
        return -1;
    }
    vt_id = atoi(argv[1]);
    vts = new coordinator::timestamper(vt_id);
    std::cout << "Vector timestamper " << vt_id << std::endl;
    for (int i = 0; i < NUM_THREADS-1; i++) {
        thr = new std::thread(server_loop, i);
        thr->detach();
    }
    server_loop(NUM_THREADS-1);
}
