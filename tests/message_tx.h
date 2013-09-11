/*
 * ===============================================================
 *    Description:  Packing and unpacking of tx messages, at
 *                  client and coordinator side respectively.
 *
 *        Created:  09/05/2013 11:50:00 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/transaction.h"
#include "common/transaction.h"
#include "common/message.h"
#include "common/message_tx_client.h"

void
pack_tx(message::message &m)
{
    client::tx_list_t tx;
    for (int i = 0; i < 20; i++) {
        auto upd = std::make_shared<client::pending_update>();
        tx.emplace_back(upd);
        switch (i % 4) {
            case 0:
                upd->type = message::CLIENT_NODE_CREATE_REQ;
                upd->handle = 42;
                break;

            case 1:
                upd->type = message::CLIENT_EDGE_CREATE_REQ;
                upd->handle = 1;
                upd->elem1 = 20;
                upd->elem2 = 239872;
                break;

            case 2:
                upd->type = message::CLIENT_NODE_DELETE_REQ;
                upd->elem1 = 98765;
                break;

            case 3:
                upd->type = message::CLIENT_EDGE_DELETE_REQ;
                upd->elem1 = 0xdeadbeef;
                break;
        }
    }
    message::prepare_tx_message_client(m, tx);
}

void
unpack_tx(message::message &m)
{
    transaction::pending_tx tx;
    message::unpack_client_tx(m, tx);
    int i = 0;
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                assert((i % 4) == 0);
                assert(upd->handle == 42);
                break;

            case transaction::EDGE_CREATE_REQ:
                assert((i % 4) == 1);
                assert(upd->handle == 1);
                assert(upd->elem1 == 20);
                assert(upd->elem2 == 239872);
                break;

            case transaction::NODE_DELETE_REQ:
                assert((i % 4) == 2);
                assert(upd->elem1 == 98765);
                break;

            case transaction::EDGE_DELETE_REQ:
                assert((i % 4) == 3);
                assert(upd->elem1 == 0xdeadbeef);
                break;

            default:
                DEBUG << "bad type" << std::endl;
        }
        i++;
    }
}

void
message_tx_test()
{
    message::message msg;
    pack_tx(msg);
    unpack_tx(msg);
}
