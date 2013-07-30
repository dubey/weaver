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
#include "common/message_graph_elem.h"
#include "graph.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
#include "node_prog/dijkstra_program.h"
#include "node_prog/reach_program.h"
#include "node_prog/clustering_program.h"

// global static variables
static uint64_t shard_id;
static db::shard *G;
db::shard *db::thread::pool::G = NULL; // reinitialized in graph constructor

// server msg recv loop for the shard server
void
msgrecv_loop()
{
    busybee_returncode ret;
    uint64_t sender;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg(new message::message());
    db::thread::unstarted_thread *thr;
    db::update_request *request;
    std::vector<uint64_t> clk;

    while (true) {
        if ((ret = G->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "msg recv error: " << ret << " at shard " << G->myid << std::endl;
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
            case message::REVERSE_EDGE_CREATE:
            case message::EDGE_DELETE_REQ:
            case message::EDGE_ADD_PROP:
            case message::EDGE_DELETE_PROP:
                message::unpack_message(*rec_msg, mtype, clk);
                //request = new db::update_request(mtype, clk, std::move(rec_msg)); TODO continue here
                request = new db::update_request(mtype, start_time - 1, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(start_time - 1, unpack_update_request, request);
                G->thread_pool.add_request(thr);
                rec_msg.reset(new message::message());
                break;

            //case message::TRANSIT_NODE_DELETE_REQ:
            //case message::TRANSIT_EDGE_CREATE_REQ:
            //case message::TRANSIT_REVERSE_EDGE_CREATE:
            //case message::TRANSIT_EDGE_DELETE_REQ:
            //case message::TRANSIT_EDGE_ADD_PROP:
            //case message::TRANSIT_EDGE_DELETE_PROP:
            //    rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(mtype)) >> update_count >> start_time;
            //    request = new db::update_request(mtype, update_count, std::move(rec_msg));
            //    G->migration_mutex.lock();
            //    G->pending_updates.emplace(request);
            //    G->migration_mutex.unlock();
            //    request = new db::update_request(mtype, 0);
            //    thr = new db::thread::unstarted_thread(0, process_pending_updates, request);
            //    G->thread_pool.add_request(thr);
            //    rec_msg.reset(new message::message());
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
