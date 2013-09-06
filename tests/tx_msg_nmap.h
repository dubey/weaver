/*
 * ===============================================================
 *    Description:  Test for tx msg packing, unpacking, and mapper
 *                  lookup/insertion.
 *
 *        Created:  09/05/2013 03:01:35 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/transaction.h"
#include "coordinator/transaction.h"
#include "common/message.h"
#include "common/message_tx_client.h"
#include "common/message_tx_coord.h"
#include "coordinator/timestamper.h"

void
pack_tx_a(message::message &m)
{
    client::tx_list_t tx;
    for (int i = 0; i < 10; i++) {
        auto node1 = std::make_shared<client::pending_update>();
        node1->type = message::CLIENT_NODE_CREATE_REQ;
        node1->handle = 42+i;
        tx.emplace_back(node1);
        auto node2 = std::make_shared<client::pending_update>();
        node2->type = message::CLIENT_NODE_CREATE_REQ;
        node2->handle = 84+i;
        tx.emplace_back(node2);
        auto edge = std::make_shared<client::pending_update>();
        edge->type = message::CLIENT_EDGE_CREATE_REQ;
        edge->handle = 12345+i;
        edge->elem1 = 42+i;
        edge->elem2 = 84+i;
        tx.emplace_back(edge);
    }
    message::prepare_tx_message_client(m, tx);
}

void
unpack_nmap_tx_a(message::message &m)
{
    coordinator::timestamper vts(0);
    coordinator::pending_tx tx;
    vts.unpack_tx(m, tx);
    uint64_t loc = 0;
    for (uint64_t i = 0; i < 10; i++) {
        auto node1 = tx.writes.at(3*i + 0);
        auto node2 = tx.writes.at(3*i + 1);
        auto edge = tx.writes.at(3*i + 2);
        assert(node1->handle == 42+i);
        loc = (loc + 1) % NUM_SHARDS;
        assert(node1->loc1 ==  (loc + SHARD_ID_INCR));
        assert(edge->loc1 == (loc + SHARD_ID_INCR));
        assert(node2->handle == 84+i);
        loc = (loc + 1) % NUM_SHARDS;
        assert(node2->loc1 ==  (loc + SHARD_ID_INCR));
        assert(edge->loc2 == (loc + SHARD_ID_INCR));
        assert(edge->handle == 12345+i);
    }
}

void
pack_tx_b(message::message &m)
{
    client::tx_list_t tx;
    for (int i = 0; i < 10; i+=2) {
        auto edge = std::make_shared<client::pending_update>();
        edge->type = message::CLIENT_EDGE_DELETE_REQ;
        edge->elem1 = 12345+i;
        tx.emplace_back(edge);
        auto node = std::make_shared<client::pending_update>();
        node->type = message::CLIENT_NODE_DELETE_REQ;
        node->elem1 = 42+i;
        tx.emplace_back(node);
    }
    message::prepare_tx_message_client(m, tx);
}

void unpack_nmap_tx_b(message::message &m)
{
    coordinator::timestamper vts(1);
    coordinator::pending_tx tx;
    vts.unpack_tx(m, tx);
    uint64_t j = 0, loc = 0;
    loc = (loc + 1) % NUM_SHARDS;
    for (uint64_t i = 0; i < 10; i+=2) {
        auto node = tx.writes.at(2*j + 1);
        auto edge = tx.writes.at(2*j + 0);
        assert(node->type == message::NODE_DELETE_REQ);
        assert(node->elem1 == 42+i);
        assert(node->loc1 = (loc + SHARD_ID_INCR));
        assert(edge->type == message::EDGE_DELETE_REQ);
        assert(edge->elem1 == 12345+i);
        assert(edge->loc1 = (loc + SHARD_ID_INCR));
        j++;
        loc = (loc + 2) % NUM_SHARDS;
    }
    vts.clean_nmap_space();
}

void
tx_msg_nmap_test()
{
    message::message msg;
    pack_tx_a(msg);
    unpack_nmap_tx_a(msg);
    pack_tx_b(msg);
    unpack_nmap_tx_b(msg);
}
