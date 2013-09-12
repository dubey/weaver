/*
 * ===============================================================
 *    Description:  Core database functionality for a shard server
 *
 *        Created:  07/25/2013 04:02:37 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *                  Greg Hill, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <iostream>
#include <signal.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#define __WEAVER_DEBUG__
#include "common/weaver_constants.h"
// TODO #include "common/message_graph_elem.h"
#include "shard.h"

// global static variables
static uint64_t shard_id;
static db::shard *S;
db::shard *db::thread::pool::S = NULL; // reinitialized in graph constructor

inline void
create_node(vc::vclock_t &t_creat, uint64_t node_handle)
{
    S->create_node(node_handle, t_creat, false);
}

inline uint64_t
create_edge(vc::vclock_t &t_creat, uint64_t edge_handle, uint64_t n1, uint64_t n2, uint64_t loc2)
{
    return S->create_edge(edge_handle, n1, n2, loc2, t_creat);
}

inline uint64_t
delete_node(vc::vclock_t &t_del, uint64_t node_handle)
{
    return S->delete_node(node_handle, t_del);
}

inline uint64_t
delete_edge(vc::vclock_t &t_del, uint64_t edge_handle)
{
    return S->delete_edge(edge_handle, t_del);
}

inline uint64_t
create_reverse_edge(vc::vclock_t &vclk, uint64_t edge_handle, uint64_t local_node, uint64_t remote_node, uint64_t remote_loc)
{
    return S->create_reverse_edge(edge_handle, local_node, remote_node, remote_loc, vclk);
}

void
unpack_update_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;
    vc::vclock_t vclk, qts;
    uint64_t handle, elem1, elem2, loc2, ret;

    switch (request->type) {
        case message::REVERSE_EDGE_CREATE:
            DEBUG << "reverse edge create" << std::endl;
            message::unpack_message(*request->msg, request->type, vclk, handle, elem1, elem2, loc2);
            ret = create_reverse_edge(vclk, handle, elem1, elem2, loc2);
            break;

        default:
            DEBUG << "unknown type" << std::endl;
    }
    if (ret == 0) {
        // update successful
    } else {
        // node being migrated, tx needs to be forwarded
        // TODO
    }
    delete request;
}

void
unpack_tx_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;
    uint64_t vt_id, tx_id, ret;
    vc::vclock_t vclk, qts;
    transaction::pending_tx tx;
    bool ack = true;
    message::unpack_message(*request->msg, message::TX_INIT, vt_id, vclk, qts, tx_id, tx.writes);
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                DEBUG << "unpacked node create" << std::endl;
                create_node(vclk, upd->handle);
                S->record_completed_transaction(vt_id, tx_id);
                DEBUG << "done node create" << std::endl;
                ret = 0;
                break;

            case transaction::EDGE_CREATE_REQ:
                ret = create_edge(vclk, upd->handle, upd->elem1, upd->elem2, upd->loc2);
                S->record_completed_transaction(vt_id, tx_id);
                break;

            case transaction::NODE_DELETE_REQ:
                ret = delete_node(vclk, upd->elem1);
                S->record_completed_transaction(vt_id, tx_id);
                break;

            case transaction::EDGE_DELETE_REQ:
                ret = delete_edge(vclk, upd->elem1);
                S->record_completed_transaction(vt_id, tx_id);
                break;

            default:
                DEBUG << "unknown type" << std::endl;
        }
        if (ret == 0) {
            // tx subpart successful
        } else {
            // node being migrated, tx needs to be forwarded
            // TODO also need to maintain DS for tracking when to ack transaction
            ack = false;
        }
    }
    delete request;
    if (ack) {
        // send tx confirmation to coordinator
        message::message conf_msg;
        message::prepare_message(conf_msg, message::TX_DONE, tx_id);
        S->send(vt_id, conf_msg.buf);
    }
}

void
unpack_and_run_node_program(void *req) {

}

// server msg recv loop for the shard server
void
msgrecv_loop()
{
    busybee_returncode ret;
    uint64_t sender, vt_id, req_id;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg(new message::message());
    db::thread::unstarted_thread *thr;
    db::graph_request *request;
    vc::vclock_t vclk, qts;

    while (true) {
        if ((ret = S->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "msg recv error: " << ret << " at shard " << S->shard_id << std::endl;
            continue;
        }
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        rec_msg->change_type(mtype);
        sender -= ID_INCR;
        vclk.clear();
        qts.clear();

        switch (mtype)
        {
            case message::TX_INIT:
                DEBUG << "got tx_init" << std::endl;
                message::unpack_message(*rec_msg, message::TX_INIT, vt_id, vclk, qts);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(qts.at(shard_id-SHARD_ID_INCR), vclk, unpack_tx_request, request);
                //DEBUG << "going to add request to threadpool" << std::endl;
                S->add_write_request(vt_id, thr);
                //DEBUG << "added request to threadpool" << std::endl;
                rec_msg.reset(new message::message());
                break;

            case message::REVERSE_EDGE_CREATE:
                //message::unpack_message(*rec_msg, mtype, vclk, handle, elem1, elem2, 
                message::unpack_message(*rec_msg, mtype, vclk);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, vclk, unpack_update_request, request);
                S->add_write_request(0, thr);
                rec_msg.reset(new message::message());
                break;

            case message::NODE_PROG:
                DEBUG << "got node_prog" << std::endl;
                message::unpack_message(*rec_msg, message::NODE_PROG, vt_id, vclk, req_id);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_and_run_node_program, request);
                DEBUG << "going to add node prog to threadpool" << std::endl;
                S->add_read_request(vt_id, thr);
                DEBUG << "added node prog to threadpool" << std::endl;
                rec_msg.reset(new message::message());
                break;

            //case message::TRANSIT_NODE_DELETE_REQ:
            //case message::TRANSIT_EDGE_CREATE_REQ:
            //case message::TRANSIT_REVERSE_EDGE_CREATE:
            //case message::TRANSIT_EDGE_DELETE_REQ:
            //case message::TRANSIT_EDGE_ADD_PROP:
            //case message::TRANSIT_EDGE_DELETE_PROP:
            //    break;

            //case message::CLEAN_UP:
            //case message::MIGRATE_NODE_STEP1:
            //case message::MIGRATE_NODE_STEP2:
            //case message::COORD_NODE_MIGRATE_ACK:
            //case message::MIGRATE_NODE_STEP4:
            //case message::MIGRATE_NODE_STEP6a:
            //case message::MIGRATE_NODE_STEP6b:
            //case message::COORD_CLOCK_REPLY:
            //case message::MIGRATED_NBR_UPDATE:
            //case message::MIGRATED_NBR_ACK:
            //case message::PERMANENT_DELETE_EDGE:
            //case message::PERMANENT_DELETE_EDGE_ACK:
            //case message::REQUEST_COUNT:
            //case message::REQUEST_COUNT_ACK:
            //    request = new db::update_request(mtype, 0, std::move(rec_msg));
            //    thr = new db::thread::unstarted_thread(0, unpack_update_request, request);
            //    G->thread_pool.add_request(thr);
            //    rec_msg.reset(new message::message());
            //    break;

            //case message::NODE_PROG:
            //    vclocks.clear();
            //    message::unpack_message(*rec_msg, message::NODE_PROG, pType, vclocks);
            //    request = new db::update_request(mtype, 0, std::move(rec_msg));
            //    thr = new db::thread::unstarted_thread(vclocks[G->myid-1], unpack_and_run_node_program, request);
            //    G->thread_pool.add_request(thr);
            //    rec_msg.reset(new message::message());
            //    break;

            //case message::MIGRATION_TOKEN:
            //    DEBUG << "Now obtained migration token at shard " << G->myid << std::endl;
            //    G->migr_token_mutex.lock();
            //    G->migr_token = true;
            //    G->migrated = false;
            //    G->migr_token_mutex.unlock();
            //    DEBUG << "Ended obtaining token" << std::endl;
            //    break;

            case message::EXIT_WEAVER:
                exit(0);
                
            default:
                DEBUG << "unexpected msg type " << mtype << std::endl;
        }
    }
}

int
main(int argc, char* argv[])
{
    // TODO signal(SIGINT, end_program);
    if (argc != 2) {
        DEBUG << "Usage: " << argv[0] << " <myid>" << std::endl;
        return -1;
    }
    uint64_t id = atoi(argv[1]);
    shard_id = id;
    S = new db::shard(id);
    std::cout << "Weaver: shard instance " << S->shard_id << std::endl;

    // TODO migration methods init

    msgrecv_loop();

    return 0;
}
