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

//#define __WEAVER_DEBUG__
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
        DEBUG << "updating qts for shard " << (upd->loc1-SHARD_ID_INCR) << std::endl;
        upd->qts = vts->qts;
        vts->qts.at(upd->loc1-SHARD_ID_INCR)++; // TODO what about edge create requests, loc2?
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
    vts->prev_write = true;
    vts->mutex.unlock(); // TODO: move sending out of critical section
}

// decrement reply count. if all replies have been received, ack to client
inline void
end_transaction(uint64_t tx_id)
{
    vts->mutex.lock();
    if (--vts->tx_replies.at(tx_id).count == 0) {
        // done tx
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
    uint64_t diff = cur_time_millis - vts->nop_time;
    uint64_t first_diff = cur_time_millis - vts->first_nop_time;
    if (diff > VT_NOP_TIMEOUT) {
        // send nops to each shard
        vts->nop_time = cur_time_millis;
        vc::qtimestamp_t new_qts = vts->qts;
        if (vts->nop_acks == NUM_SHARDS) {
            for (auto &qts: vts->qts) {
                qts++;
            }
        }
        vts->vclk.increment_clock();
        vc::vclock vclk = vts->vclk;
        req_id = vts->generate_id();
        vts->mutex.unlock();
        message::message msg;
        if (vts->nop_acks == NUM_SHARDS) {
            for (uint64_t i = 0; i < NUM_SHARDS; i++) {
                message::prepare_message(msg, message::VT_NOP, vt_id, vclk, new_qts, req_id);
                vts->send(i + SHARD_ID_INCR, msg.buf);
            }
            vts->nop_acks = 0;
        }
        if (vts->first_clock_update) {
            DEBUG << "clock update acks " << vts->clock_update_acks << std::endl;
            DEBUG << "diff " << diff << ", initial clock update delay " << VT_INITIAL_CLKUPDATE_DELAY << std::endl;
            DEBUG << "first diff " << first_diff << ", initial clock update delay " << VT_INITIAL_CLKUPDATE_DELAY << std::endl;
        }
        // first check is an ugly hack to make sure all VTs are up before sending out clock updates
        // second check is to ensure all VTs acked previous update before sending out new update
        if (((vts->first_clock_update && first_diff > VT_INITIAL_CLKUPDATE_DELAY) || !vts->first_clock_update)
        && (vts->clock_update_acks == (NUM_VTS-1))) {
            vts->first_clock_update = false;
            DEBUG << "sending clock update now, clock update acks " << vts->clock_update_acks << std::endl;
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
    unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID)
{
    DEBUG << "starting node program on timestamper" << std::endl;
    node_prog::prog_type pType;
    std::vector<std::pair<uint64_t, ParamsType>> initial_args;

    message::unpack_message(*msg, message::CLIENT_NODE_PROG_REQ, pType, initial_args);
    
    // map from locations to a list of start_node_params to send to that shard
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> initial_batches; 

    // lookup mappings
    std::unordered_map<uint64_t, uint64_t> request_element_mappings;
    std::unordered_set<uint64_t> mappings_to_get;
    for (auto &initial_arg : initial_args) {
        uint64_t c_id = initial_arg.first;
        mappings_to_get.insert(c_id);
    }
    if (!mappings_to_get.empty()) {
        auto results = vts->nmap_client.get_mappings(mappings_to_get, true);
        assert(results.size() == mappings_to_get.size());
        for (auto &toAdd : results) {
            request_element_mappings.emplace(toAdd);
        }
    }
    DEBUG << "timestamper done looking up element mappings for node program" << std::endl;

    for (std::pair<uint64_t, ParamsType> &node_params_pair : initial_args) { // TODO: change to params pointer so we can avoid potential copy?
        initial_batches[request_element_mappings[node_params_pair.first]].emplace_back(std::make_tuple(node_params_pair.first,
            std::move(node_params_pair.second), db::element::remote_node())); // constructor
    }
    vts->mutex.lock();
    if (vts->prev_write) {
        vts->vclk.increment_clock();
    }
    vc::vclock req_timestamp =  vts->vclk;
    assert(req_timestamp.clock.size() == NUM_VTS);
    uint64_t req_id = vts->generate_id();

    message::message msg_to_send;
    std::vector<uint64_t> empty_vector;
    std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> empty_tuple_vector;
    DEBUG << "starting node prog " << req_id << ", recd from client\t" << std::endl;
    for (auto &batch_pair : initial_batches) {
        message::prepare_message(msg_to_send, message::NODE_PROG, pType, vt_id, req_timestamp, 
                req_id, batch_pair.second, empty_tuple_vector);
        vts->send(batch_pair.first, msg_to_send.buf); // TODO: can we send out of critical section?
    }
    DEBUG << "sent to shards" << std::endl;
    vts->prev_write = false;
    vts->outstanding_node_progs.emplace(std::make_pair(req_id, clientID));
    vts->mutex.unlock();
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> ::
    unpack_and_run_db(std::unique_ptr<message::message>)
{ }

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
            if (thread_id == 0) {
                periodic_update();
            }
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
                    vts->unpack_tx(*msg, tx, sender);
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
                    vts->periodic_update_mutex.lock();
                    vts->nop_acks++;
                    assert(vts->nop_acks <= NUM_SHARDS);
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
                    //server->update_mutex.lock(); //TODO do we need to mutex here?
                    node_prog::programs.at(pType)->unpack_and_start_coord(std::move(msg), sender);
                    //server->update_mutex.unlock();
                    break;

                // response from a shard
                case message::NODE_PROG_RETURN:
                    uint64_t req_id;
                    message::unpack_message(*msg, message::NODE_PROG_RETURN, req_id); // don't unpack rest
                    vts->mutex.lock();
                    if (vts->outstanding_node_progs.find(req_id) != vts->outstanding_node_progs.end()) {
                        uint64_t client_to_ret = vts->outstanding_node_progs.at(req_id);
                        vts->send(client_to_ret, msg->buf);
                        vts->outstanding_node_progs.erase(req_id);
                    } else {
                        std::cerr << "node prog return for already completed ornever existed req id" << std::endl;
                    }
                    vts->mutex.unlock();
                    break;

                default:
                    std::cerr << "unexpected msg type " << mtype << std::endl;
            }
            if (thread_id == 0) {
                periodic_update();
            }
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
    DEBUG << "Vector timestamper " << vt_id << std::endl;
    for (int i = 0; i < NUM_THREADS-1; i++) {
        thr = new std::thread(server_loop, i);
        thr->detach();
    }
    server_loop(NUM_THREADS-1);
}
