/*
 * ===============================================================
 *    Description:  Request handler server loop and request
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

#include "reqhandler.h"

static coordinator::rhandler *handler;

// SIGINT handler
void
end_program(int param)
{
    std::cerr << "Ending program, param = " << param << std::endl;
    exit(0);
}

// create a node
void
create_node(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    uint64_t loc, node_handle;
    std::vector<uint64_t> clock;
    // local state update
    handler->mutex.lock();
    handler->port_ctr = (handler->port_ctr + 1) % NUM_SHARDS;
    loc = handler->port_ctr + 1; // node will be placed on this shard
    handler->vc.increment_clock(loc-1);
    node_handle = handler->generate_handle();
    clock = handler->vc.get_clock();
    handler->mutex.unlock();
    // send update for new entry to state manager
    // XXX hyperdex transaction -- is there a callback for an async call?
    // send update to shard
    message::prepare_message(msg, message::NODE_CREATE_REQ, clock, node_handle);
    handler->send(loc, msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY, node_handle);
    handler->send(creq->sender, msg.buf);
}

// create an edge
void
create_edge(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    uint64_t edge_handle;
    common::meta_element *me1, *me2;
    std::vector<uint64_t> clock;
    // get nodes from state manager
    // XXX hyperdex lookup for me1, me2
    // XXX if lookup successful then do the following:
    // local state update
    handler->mutex.lock();
    handler->vc.increment_clock(me1->get_loc()-1);
    handler->vc.increment_clock(me2->get_loc()-1);
    edge_handle = handler->generate_handle();
    clock = handler->vc.get_clock();
    handler->mutex.unlock();
    // send update for new entry to state manager
    // XXX hyperdex transaction
    // send update to shards
    message::prepare_message(msg, message::EDGE_CREATE_REQ,
        clock, edge_handle, creq->elem1, creq->elem2, me2->get_loc());
    handler->send(me1->get_loc(), msg.buf);
    message::prepare_message(msg, message::REVERSE_EDGE_CREATE,
        clock, edge_handle, creq->elem2, creq->elem1, me1->get_loc());
    handler->send(me2->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY, edge_handle);
    handler->send(creq->sender, msg.buf);
}

// add a property, i.e. a key-value pair to an edge
void
add_edge_property(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    common::meta_element *me;
    std::vector<uint64_t> clock;
    // get edge from state manager
    // XXX hyperdex lookup
    // XXX if lookup successful then do the following:
    // local state update
    handler->mutex.lock();
    handler->vc.increment_clock(me->get_loc()-1);
    clock = handler->vc.get_clock();
    handler->mutex.unlock();
    // send update to shard
    common::property prop(creq->key, creq->value, handler->rh_id, me->get_loc(), clock.at(me->get_loc()-1));
    message::prepare_message(msg, message::EDGE_ADD_PROP, clock, creq->elem1, prop);
    handler->send(me->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY);
    handler->send(creq->sender, msg.buf);
}

// delete a node
void
delete_node_initiate(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    common::meta_element *me;
    std::vector<uint64_t> clock;
    // get node from state manager
    // XXX hyperdex transaction to mark node as deleted
    // XXX if lookup successful then do the following:
    // local state update
    handler->mutex.lock();
    handler->vc.increment_clock(me->get_loc()-1);
    clock = handler->vc.get_clock();
    handler->mutex.unlock();
    // send update to shard
    message::prepare_message(msg, message::NODE_DELETE_REQ, clock, creq->elem1);
    handler->send(me->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY);
    handler->send(creq->sender, msg.buf);
}

// delete an edge
void
delete_edge_initiate(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    common::meta_element *me;
    std::vector<uint64_t> clock;
    // get edge from state manager
    // XXX hyperdex lookup
    // XXX if lookup successful then do the following:
    // local state update
    handler->mutex.lock();
    handler->vc.increment_clock(me->get_loc()-1);
    clock = handler->vc.get_clock();
    handler->mutex.unlock();
    // send update to shard
    message::prepare_message(msg, message::EDGE_DELETE_REQ, clock, creq->elem1);
    handler->send(me->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY);
    handler->send(creq->sender, msg.buf);
}

// delete edge property corresponding to a particular key
void
delete_edge_property_initiate(std::shared_ptr<coordinator::pending_update> creq)
{
    message::message msg;
    common::meta_element *me;
    std::vector<uint64_t> clock;
    // get edge from state manager
    // XXX hyperdex lookup
    // XXX if lookup successful then do the following:
    // local state update
    handler->mutex.lock();
    handler->vc.increment_clock(me->get_loc()-1);
    clock = handler->vc.get_clock();
    handler->mutex.unlock();
    // send update to shard
    message::prepare_message(msg, message::EDGE_DELETE_REQ, clock, creq->elem1, creq->key);
    handler->send(me->get_loc(), msg.buf);
    // send response to client
    message::prepare_message(msg, message::CLIENT_REPLY);
    handler->send(creq->sender, msg.buf);
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
        if ((ret = handler->bb->recv(&sender, &msg->buf)) != BUSYBEE_SUCCESS) {
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
            message::unpack_message(*msg, message::CLIENT_EDGE_CREATE_REQ, crequest->elem1, crequest->elem2);
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

        default:
            std::cerr << "unexpected msg type " << mtype << std::endl;
        }
    }
}

int
main(int argc, char *argv[])
{
    std::thread *thr;
    signal(SIGINT, end_program);
    if (argc != 2) {
        DEBUG << "Usage: " << argv[0] << " <req_handler_id>" << std::endl;
        return -1;
    }
    uint64_t id = atoi(argv[1]);
    handler = new coordinator::rhandler(id);
    for (int i = 0; i < NUM_THREADS-1; i++) {
        thr = new std::thread(server_loop);
        thr->detach();
    }
    server_loop();
}
