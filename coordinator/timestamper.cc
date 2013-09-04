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

#include "common/vclock.h"
#include "timestamper.h"
#include "transaction.h"

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
begin_transaction(coordinator::pending_tx tx)
{
    message::message msg;
    vc::vclock_t clock;
    vts->mutex.lock();
    for (std::shared_ptr<coordinator::pending_update> upd: tx.writes) {
        switch (upd->type) {
            case message::NODE_CREATE_REQ:
            case message::NODE_DELETE_REQ:
                vts->qts.increment_clock(upd->loc1 - 1);
                break;

            case message::EDGE_CREATE_REQ:
            case message::EDGE_DELETE_REQ:
                vts->qts.increment_clock(upd->loc1 - 1);
                vts->qts.increment_clock(upd->loc2 - 1);
                break;

            default:
                DEBUG << "bad update type";
        }
        vts->vclk.increment_clock();
        upd->qts = vts->qts;
    }
    tx.timestamp = vts->vclk.get_clock();
    vts->mutex.unlock();
    message::prepare_tx_message(msg, vt_id, tx);
    vts->send(tx.writes.at(0).loc1, msg);
}

// create a node
inline void
create_node(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    uint64_t loc, node_handle;
    vc::vclock_t clock;
    // local state update
    vts->mutex.lock();
    vts->port_ctr = (vts->port_ctr + 1) % NUM_SHARDS;
    loc = vts->port_ctr + 1; // node will be placed on this shard
    vts->vc.increment_clock(loc-1);
    node_handle = vts->generate_handle();
    clock = vts->vclk.get_clock();
    vts->mutex.unlock();
    // send update for new entry to state manager
    // XXX hyperdex transaction -- is there a callback for an async call?
    // send update to shard
    message::prepare_message(msg, message::NODE_CREATE_REQ, clock, node_handle);
    vts->send(loc, msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY, node_handle);
    vts->send(creq->sender, msg.buf);
}

// create an edge
inline void
create_edge(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    uint64_t edge_handle;
    common::meta_element *me1, *me2;
    vc::vclock_t clock;
    // get nodes from state manager
    // XXX hyperdex lookup for me1, me2
    // XXX if lookup successful then do the following:
    // local state update
    vts->mutex.lock();
    vts->vc.increment_clock(me1->get_loc()-1);
    vts->vc.increment_clock(me2->get_loc()-1);
    edge_handle = vts->generate_handle();
    clock = vts->vc.get_clock();
    vts->mutex.unlock();
    // send update for new entry to state manager
    // XXX hyperdex transaction
    // send update to shards
    message::prepare_message(msg, message::EDGE_CREATE_REQ,
        clock, edge_handle, creq->elem1, creq->elem2, me2->get_loc());
    vts->send(me1->get_loc(), msg.buf);
    message::prepare_message(msg, message::REVERSE_EDGE_CREATE,
        clock, edge_handle, creq->elem2, creq->elem1, me1->get_loc());
    vts->send(me2->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY, edge_handle);
    vts->send(creq->sender, msg.buf);
}

// add a property, i.e. a key-value pair to an edge
inline void
add_edge_property(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    common::meta_element *me;
    vclock::nvc clock;
    // get edge from state manager
    // XXX hyperdex lookup
    // XXX if lookup successful then do the following:
    // local state update
    vts->mutex.lock();
    vts->vc.increment_clock(me->get_loc()-1);
    clock = vts->vc.get_entire_clock();
    vts->mutex.unlock();
    // send update to shard
    common::property prop(creq->key, creq->value, vts->vt_id, me->get_loc(), clock.at(me->get_loc()-1));
    message::prepare_message(msg, message::EDGE_ADD_PROP, clock, creq->elem1, prop);
    vts->send(me->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY);
    vts->send(creq->sender, msg.buf);
}

// delete a node
inline void
delete_node_initiate(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    common::meta_element *me;
    vclock::nvc clock;
    // get node from state manager
    // XXX hyperdex transaction to mark node as deleted
    // XXX if lookup successful then do the following:
    // local state update
    vts->mutex.lock();
    vts->vc.increment_clock(me->get_loc()-1);
    clock = vts->vc.get_entire_clock();
    vts->mutex.unlock();
    // send update to shard
    message::prepare_message(msg, message::NODE_DELETE_REQ, clock, creq->elem1);
    vts->send(me->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY);
    vts->send(creq->sender, msg.buf);
}

// delete an edge
inline void
delete_edge_initiate(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    common::meta_element *me;
    vclock::nvc clock;
    // get edge from state manager
    // XXX hyperdex lookup
    // XXX if lookup successful then do the following:
    // local state update
    vts->mutex.lock();
    vts->vc.increment_clock(me->get_loc()-1);
    clock = vts->vc.get_entire_clock();
    vts->mutex.unlock();
    // send update to shard
    message::prepare_message(msg, message::EDGE_DELETE_REQ, clock, creq->elem1);
    vts->send(me->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY);
    vts->send(creq->sender, msg.buf);
}

// delete edge property corresponding to a particular key
inline void
delete_edge_property_initiate(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    common::meta_element *me;
    vclock::nvc clock;
    // get edge from state manager
    // XXX hyperdex lookup
    // XXX if lookup successful then do the following:
    // local state update
    vts->mutex.lock();
    vts->vc.increment_clock(me->get_loc()-1);
    clock = vts->vc.get_entire_clock();
    vts->mutex.unlock();
    // send update to shard
    message::prepare_message(msg, message::EDGE_DELETE_REQ, clock, creq->elem1, creq->key);
    vts->send(me->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY);
    vts->send(creq->sender, msg.buf);
}

void
server_loop()
{
    busybee_returncode ret;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> msg;
    uint64_t sender;

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
        case message::CLIENT_NODE_CREATE_REQ: {
            auto crequest = std::make_shared<coordinator::pending_update>();
            crequest->sender = sender;
            create_node(crequest);
            break;
        }

        case message::CLIENT_EDGE_CREATE_REQ: {
            auto crequest = std::make_shared<coordinator::pending_update>();
            crequest->sender = sender;
            message::unpack_message(*msg, message::CLIENT_EDGE_CREATE_REQ,
                crequest->elem1, crequest->elem2);
            create_edge(crequest);
            break;
        }

        case message::CLIENT_NODE_DELETE_REQ: {
            auto crequest = std::make_shared<coordinator::pending_update>();
            crequest->sender = sender;
            message::unpack_message(*msg, message::CLIENT_NODE_DELETE_REQ, crequest->elem1);
            delete_node_initiate(crequest);
            break;
        }

        case message::CLIENT_EDGE_DELETE_REQ: {
            auto crequest = std::make_shared<coordinator::pending_update>();
            crequest->sender = sender;
            message::unpack_message(*msg, message::CLIENT_EDGE_DELETE_REQ, crequest->elem1);
            delete_edge_initiate(crequest);
            break;
        }

        case message::CLIENT_ADD_EDGE_PROP: {
            auto crequest = std::make_shared<coordinator::pending_update>();
            crequest->sender = sender;
            message::unpack_message(*msg, message::CLIENT_ADD_EDGE_PROP,
                crequest->elem1, crequest->key, crequest->value);
            add_edge_property(crequest);
            break;
        }

        case message::CLIENT_DEL_EDGE_PROP: {
            auto crequest = std::make_shared<coordinator::pending_update>();
            crequest->sender = sender;
            message::unpack_message(*msg, message::CLIENT_DEL_EDGE_PROP,
                crequest->elem1, crequest->key);
            delete_edge_property_initiate(crequest);
            break;
        }

        case message::VT_CLOCK_UPDATE: {
            uint64_t rec_vtid;
            std::vector<uint64_t> rec_clock;
            message::unpack_message(*msg, message::VT_CLOCK_UPDATE,
                rec_vtid, rec_clock);
            vts->mutex.lock();
            vts->vc.update_clock(rec_vtid, rec_clock);
            vts->mutex.unlock();
            break;
        }

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
    vclock::nvc clock;
    vts->mutex.lock();
    clock = vtc->vc.get_clock();
    vts->mutex.unlock();
    message::prepare_message(msg, message::VT_CLOCK_UPDATE, vt_id, clock);
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
    vts = new coordinator::timestamper(id);
    for (int i = 0; i < NUM_THREADS-1; i++) {
        thr = new std::thread(server_loop);
        thr->detach();
    }
    server_loop();
}
