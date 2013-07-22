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

#include "reqhandler.h"

static coordinator::rhandler *handler;

void
create_node(coordinator::pending_update creq)
{
    message::message msg;
    uint64_t loc, node_handle;
    std::vector<uint64_t> clock;
    // local state update
    handler->mutex.lock();
    handler->port_ctr = (handler->port_ctr + 1) % NUM_SHARDS;
    loc = handler->port_ctr;
    handler->vc.increment_clock(loc);
    node_handle = handler->generate_handle();
    clock = handler->vc.get_clock();
    handler->mutex.unlock();
    // send update for new entry to state manager
}

void
create_edge(coordinator::pending_update creq)
{
}

void
add_edge_property(coordinator::pending_update creq)
{
}

void
delete_node_initiate(coordinator::pending_update creq)
{
}

void
delete_edge_initiate(coordinator::pending_update creq)
{
}

void
delete_edge_property_initiate(coordinator::pending_update creq)
{
}

void
server_loop()
{
    busybee_returncode ret;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    uint64_t sender;

    while (true)
    {
        rec_msg.reset(new message::message());
        if ((ret = handler->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        sender -= ID_INCR;

        switch (mtype) {
        // shard messages   
        case message::NODE_DELETE_ACK:
        case message::EDGE_DELETE_ACK:
        case message::EDGE_DELETE_PROP_ACK:
            break;
        
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
            message::unpack_message(*msg, message::CLIENT_EDGE_DELETE_REQ, crequest->elem1, crequest->elem2);
            delete_edge_initiate(crequest);
            break;
        }

        case message::CLIENT_ADD_EDGE_PROP: {
            auto crequest = std::make_shared<coordinator::pending_update>();
            crequest->sender = sender;
            message::unpack_message(*msg, message::CLIENT_ADD_EDGE_PROP,
                crequest->elem1, crequest->elem2, crequest->key, crequest->value);
            add_edge_property(crequest);
            break;
        }

        case message::CLIENT_DEL_EDGE_PROP: {
            auto crequest = std::make_shared<coordinator::pending_update>();
            crequest->sender = sender;
            message::unpack_message(*msg, message::CLIENT_DEL_EDGE_PROP,
                crequest->elem1, crequest->elem2, crequest->key);
            delete_edge_property_initiate(crequest);
            break;
        }

        case message::EXIT_WEAVER:
            exit_weaver();
            break;

        default:
            std::cerr << "unexpected msg type " << m_type << std::endl;
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
