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
    db::update_request *request = (db::update_request*)req;
    enum message::msg_type type = message::ERROR;
    uint64_t vt_id, sid;
    vc::vclock_t vclk, qts;
    uint64_t handle, elem1, elem2, loc1, loc2;
    uint32_t shift_off = 0;
    uint64_t ret;
    // TODO increment_qts if successful. What to do if not successful?

    switch (request->type) {
        case message::NODE_CREATE_REQ:
            message::unpack_message(*request->msg, type, sid, vt_id, vclk, qts, handle, loc1);
            create_node(vclk, handle);
            ret = 0;
            shift_off += sizeof(enum message::msg_type)
                       + message::size(vt_id)
                       + message::size(vclk)
                       + message::size(qts)
                       + message::size(handle)
                       + message::size(loc1);
            break;

        case message::EDGE_CREATE_REQ:
            message::unpack_message(*request->msg, type, sid, vt_id, vclk, qts, handle, elem1, elem2, loc1, loc2);
            ret = create_edge(vclk, handle, elem1, elem2, loc2);
            shift_off += sizeof(enum message::msg_type)
                       + message::size(vt_id)
                       + message::size(vclk)
                       + message::size(qts)
                       + message::size(handle)
                       + message::size(elem1)
                       + message::size(elem2)
                       + message::size(loc1)
                       + message::size(loc2);
            break;

        case message::NODE_DELETE_REQ:
            message::unpack_message(*request->msg, type, sid, vt_id, vclk, qts, elem1, loc1);
            ret = delete_node(vclk, elem1);
            shift_off += sizeof(enum message::msg_type)
                       + message::size(vt_id)
                       + message::size(vclk)
                       + message::size(qts)
                       + message::size(elem1)
                       + message::size(loc1);
            break;

        case message::EDGE_DELETE_REQ:
            message::unpack_message(*request->msg, type, sid, vt_id, vclk, qts, elem1, elem2, loc1, loc2);
            ret = delete_edge(vclk, elem1);
            shift_off += sizeof(enum message::msg_type)
                       + message::size(vt_id)
                       + message::size(vclk)
                       + message::size(qts)
                       + message::size(elem1)
                       + message::size(elem2)
                       + message::size(loc1)
                       + message::size(loc2);
            break;

        case message::REVERSE_EDGE_CREATE:
            message::unpack_message(*request->msg, type, vclk, handle, elem1, elem2, loc2);
            ret = create_reverse_edge(vclk, handle, elem1, elem2, loc2);
            break;

        default:
            DEBUG << "unknown type" << std::endl;
    }
    if (shift_off > 0) {
        if (ret == 0) {
            // tx subpart successful
            message::shift_buffer(*request->msg, shift_off);
            if (!message::empty_buffer(*request->msg)) {
                // propagate tx to next shard
                message::unpack_message(*request->msg, type, sid); // unpack next location
                S->send(sid, request->msg->buf);
            } else {
                // send tx confirmation to coordinator
                message::message conf_msg;
                message::prepare_message(conf_msg, message::TX_DONE, vclk);
                S->send(vt_id, conf_msg.buf);
            }
        } else {
            // node being migrated, tx needs to be forwarded
            // TODO
        }
    } else {
        if (ret != 0) {
            // create rev edge not completed as node being migrated, needs to be fwd
            // TODO
        }
    }
    delete request;
}

// server msg recv loop for the shard server
void
msgrecv_loop()
{
    busybee_returncode ret;
    uint64_t sender, vt_id;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg(new message::message());
    db::thread::unstarted_thread *thr;
    db::update_request *request;
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

        switch (mtype)
        {
            case message::NODE_CREATE_REQ:
            case message::NODE_DELETE_REQ:
            case message::EDGE_CREATE_REQ:
            case message::EDGE_DELETE_REQ:
                message::unpack_message(*rec_msg, mtype, vt_id, vclk, qts);
                request = new db::update_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(qts.at(shard_id), vclk, unpack_update_request, request);
                S->add_request(vt_id, thr);
                rec_msg.reset(new message::message());
                break;

            case message::REVERSE_EDGE_CREATE:
                //message::unpack_message(*rec_msg, mtype, vclk, handle, elem1, elem2, 
                message::unpack_message(*rec_msg, mtype, vclk);
                request = new db::update_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, vclk, unpack_update_request, request);
                S->add_request(0, thr);
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
