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
    vc::vclock_t clock;
    std::vector<transaction::pending_tx> tx_vec(NUM_SHARDS, transaction::pending_tx());
    vts->mutex.lock();
    for (std::shared_ptr<transaction::pending_update> upd: tx.writes) {
        DEBUG << "updating qts for shard " << (upd->loc1-SHARD_ID_INCR) << std::endl;
        upd->qts = vts->qts;
        vts->qts.at(upd->loc1-SHARD_ID_INCR)++; // TODO what about edge create requests, loc2?
        tx_vec.at(upd->loc1-SHARD_ID_INCR).writes.emplace_back(upd);
    }
    vts->vclk.increment_clock();
    tx.timestamp = vts->vclk.get_clock();
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

// node program stuff
template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_start_coord(std::unique_ptr<message::message> msg)
{
    node_prog::prog_type ignore;
    std::vector<std::pair<uint64_t, ParamsType>> initial_args;

    message::unpack_message(*request->req_msg, message::CLIENT_NODE_PROG_REQ, ignore, initial_args);
    
    // map from locations to a list of start_node_params to send to that shard
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> initial_batches; 

    for (std::pair<uint64_t, ParamsType> &node_params_pair : initial_args) {
        if (check_elem(server, node_params_pair.first, true)) {
            std::cerr << "one of the arg nodes has been deleted, cannot perform request" << std::endl;
            /* TODO send back error msg */
            return;
        }
        common::meta_element *me = server->nodes.at(node_params_pair.first);
        initial_batches[me->get_loc()].emplace_back(std::make_tuple(node_params_pair.first,
            std::move(node_params_pair.second), db::element::remote_node())); // constructor
    }
    if (vts->prev_write) {
        vts->vclk.increment_clock();
    }
    vc::vclock_t req_timestamp =  vts->vclk.get_clock();
    uint64_t req_id = vts->generate_id();

    server->mutex.lock();
    server->pending.emplace(std::make_pair(request->req_id, request));
    server->mutex.

    message::message msg_to_send;
    std::vector<uint64_t> empty_vector;
    std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> empty_tuple_vector;
    DEBUG << "starting node prog " << request->req_id << ", recd from client\t";
    for (auto &batch_pair : initial_batches) {
        message::prepare_message(msg_to_send, message::NODE_PROG, request->pType, *request->vector_clock, 
                request->req_id, batch_pair.second, empty_vector, request->ignore_cache, empty_tuple_vector);
        server->send(batch_pair.first, msg_to_send.buf);
    }
    DEBUG << "sent to shards" << std::endl;
}

// caution: need to hold server->update_mutex throughout
void end_node_prog(std::shared_ptr<coordinator::pending_req> request)
{
    bool done = true;
    uint64_t req_id = request->req_id;
    request->out_count->cnt--;
    for (uint64_t cached_id: *request->cached_req_ids) {
        if (!server->is_deleted_cache_id(cached_id)) {
            server->add_good_cache_id(cached_id);
        } else {
            // request was served based on cache value that should be
            // invalidated; restarting request
            done = false;
            request->ignore_cache.emplace(cached_id);
            server->add_bad_cache_id(cached_id);
            request->del_request.reset();
            node_prog::programs.at(request->pType)->unpack_and_start_coord(request);
            break;
        }
    }
    server->pending.erase(req_id);
    if (done) {
        server->completed_requests->emplace_back(std::make_pair(req_id, request->pType));
        // send same message along to client
        DEBUG << "going to send msg to end node prog\t";
        server->send(request->client, request->reply_msg->buf);
        DEBUG << "ended node prog " << req_id << std::endl;
    }
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> ::
    unpack_and_run_db(message::message&)
{
}

void
server_loop()
{
    busybee_returncode ret;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> msg;
    uint64_t sender, tx_id;
    node_prog::prog_type pType;

    while (true)
    {
        msg.reset(new message::message());
        if ((ret = vts->bb->recv(&sender, &msg->buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
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

        case message::CLIENT_NODE_PROG_REQ:
            message::unpack_message(*msg, message::CLIENT_NODE_PROG_REQ, pType);
            //server->update_mutex.lock();
            node_prog::programs.at(pType)->unpack_and_start_coord(std::move(msg));
            //server->update_mutex.unlock();
            break;

            // other timestamper messages
            case message::VT_CLOCK_UPDATE: {
                uint64_t rec_vtid, rec_clock;
                message::unpack_message(*msg, message::VT_CLOCK_UPDATE, rec_vtid, rec_clock);
                vts->mutex.lock();
                vts->vclk.update_clock(rec_vtid, rec_clock);
                vts->mutex.unlock();
                break;
            }

            // shard messages
            case message::TX_DONE:
                message::unpack_message(*msg, message::TX_DONE, tx_id);
                end_transaction(tx_id);
                break;

                // response from a shard
            case message::NODE_PROG:
                cached_req_ids.reset(new std::vector<uint64_t>());
                message::unpack_message(*msg, message::NODE_PROG, pType, req_id, *cached_req_ids); // don't unpack rest
                server->update_mutex.lock();
                if (server->pending.count(req_id) == 0){
                    // XXX anything else we need to do?
                    server->update_mutex.unlock();
                    std::cerr << "got response for request " << req_id << ", which does not exist anymore" << std::endl;
                    return;
                }
                request = server->pending.at(req_id);
                request->cached_req_ids = std::move(cached_req_ids);
                request->reply_msg = std::move(msg);
                if (request->del_request) {
                    if (request->del_request->done) {
                        end_node_prog(request);
                    } else {
                        request->done = true;
                    }
                } else {
                    end_node_prog(request);
                }
                server->update_mutex.unlock();
                break;

            default:
                std::cerr << "unexpected msg type " << mtype << std::endl;
        }
    }
}

// periodically share local vector clock with other timestampers
// TODO permanent deletion of deleted and migrated nodes and state for completed node progs
inline void
coord_daemon_initiate()
{
    message::message msg;
    vc::vclock_t clock;
    vts->mutex.lock();
    clock = vts->vclk.get_clock();
    vts->mutex.unlock();
    message::prepare_message(msg, message::VT_CLOCK_UPDATE, vt_id, clock.at(vt_id));
    // broadcast updated vector clock
    for (uint64_t i = 0; i < NUM_VTS; i++) {
        if (i == vt_id) {
            continue;
        }
        vts->send(i, msg.buf);
    }
}

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
        thr = new std::thread(server_loop);
        thr->detach();
    }
    server_loop();
}
