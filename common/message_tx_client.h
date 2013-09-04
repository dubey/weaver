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

#include "message.h"
#include "client/transaction.h"

namespace message
{
    inline void
    prepare_tx_message_client(message &m, const client::tx_list_t &tx)
    {
        uint64_t bytes_to_pack = sizeof(enum msg_type) * tx.size();
        for (auto &upd: tx) {
            switch (upd->type) {
                case message::CLIENT_NODE_CREATE_REQ:
                    bytes_to_pack += size(upd->temp_handle);
                    break;

                case message::CLIENT_EDGE_CREATE_REQ:
                    bytes_to_pack += size(upd->temp_handle, upd->elem1, upd->elem2);
                    break;

                case message::CLIENT_NODE_DELETE_REQ:
                case message::CLIENT_EDGE_DELETE_REQ:
                    bytes_to_pack += size(upd->elem1);
                    break;

                default:
                    DEBUG << "bad msg type" << std::endl;
            }
        }
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = m.buf->pack_at(BUSYBEE_HEADER_SIZE);

        for (auto &upd: tx) {
            packer = packer << upd->type;
            switch (upd->type) {
                case message::CLIENT_NODE_CREATE_REQ:
                    pack_buffer(packer, upd->temp_handle);
                    break;

                case message::CLIENT_EDGE_CREATE_REQ:
                    pack_buffer(packer, upd->temp_handle, upd->elem1, upd->elem2);
                    break;

                case message::CLIENT_NODE_DELETE_REQ:
                case message::CLIENT_EDGE_DELETE_REQ:
                    pack_buffer(packer, upd->elem1);
                    break;

                default:
                    DEBUG << "bad msg type" << std::endl;
            }
        }
    }
}
