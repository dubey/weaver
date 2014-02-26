/*
 * ===============================================================
 *    Description:  Message packing for client transactions.
 *
 *        Created:  09/04/2013 09:28:34 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_message_tx_client_h_
#define weaver_common_message_tx_client_h_

#include "message.h"
#include "client/transaction.h"

namespace message
{
    inline void
    prepare_tx_message_client(message &m, const client::tx_list_t &tx)
    {
        uint64_t num_writes = tx.size();
        uint64_t bytes_to_pack = sizeof(enum msg_type) * (1 + tx.size())
            + size_wrapper(num_writes);
        for (auto &upd: tx) {
            switch (upd->type) {
                case CLIENT_NODE_CREATE_REQ:
                    bytes_to_pack += size_wrapper(upd->handle);
                    break;

                case CLIENT_EDGE_CREATE_REQ:
                    bytes_to_pack += size_wrapper(upd->handle, upd->elem1, upd->elem2);
                    break;

                case CLIENT_NODE_DELETE_REQ:
                    bytes_to_pack += size_wrapper(upd->elem1);
                    break;

                case CLIENT_EDGE_DELETE_REQ:
                    bytes_to_pack += size_wrapper(upd->elem1, upd->elem2);
                    break;

                case CLIENT_NODE_SET_PROP:
                    bytes_to_pack += size_wrapper(upd->elem1, upd->key, upd->value);
                    break;

                case CLIENT_EDGE_SET_PROP:
                    bytes_to_pack += size_wrapper(upd->elem1, upd->elem2, upd->key, upd->value);
                    break;

                default:
                    WDEBUG << "bad msg type" << std::endl;
            }
        }
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = m.buf->pack_at(BUSYBEE_HEADER_SIZE);

        packer = packer << CLIENT_TX_INIT;
        pack_buffer_wrapper(packer, num_writes);
        for (auto &upd: tx) {
            packer = packer << upd->type;
            switch (upd->type) {
                case CLIENT_NODE_CREATE_REQ:
                    pack_buffer_wrapper(packer, upd->handle);
                    break;

                case CLIENT_EDGE_CREATE_REQ:
                    pack_buffer_wrapper(packer, upd->handle, upd->elem1, upd->elem2);
                    break;

                case CLIENT_NODE_DELETE_REQ:
                    pack_buffer_wrapper(packer, upd->elem1);
                    break;

                case CLIENT_EDGE_DELETE_REQ:
                    pack_buffer_wrapper(packer, upd->elem1, upd->elem2);
                    break;

                case CLIENT_NODE_SET_PROP:
                    pack_buffer_wrapper(packer, upd->elem1, upd->key, upd->value);
                    break;

                case CLIENT_EDGE_SET_PROP:
                    pack_buffer_wrapper(packer, upd->elem1, upd->elem2, upd->key, upd->value);
                    break;

                default:
                    WDEBUG << "bad msg type" << std::endl;
            }
        }
    }
}

#endif
