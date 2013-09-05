/*
 * ===============================================================
 *    Description:  Message packing for coordinator transactions.
 *
 *        Created:  09/03/2013 04:26:15 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __MSG_TX_COORD__
#define __MSG_TX_COORD__

#include "message.h"
#include "coordinator/transaction.h"

namespace message
{
    inline void
    prepare_tx_message(message &m, const uint64_t vt_id, const coordinator::pending_tx &tx)
    {
        uint64_t bytes_to_pack = sizeof(enum msg_type) * tx.writes.size()
                               + size(vt_id) * tx.writes.size()
                               + size(tx.timestamp) * tx.writes.size();
        for (auto &upd: tx.writes) {
            bytes_to_pack += size(upd->qts);
            switch (upd->type) {
                case NODE_CREATE_REQ:
                    bytes_to_pack += size(upd->handle, upd->loc1);
                    break;
                
                case EDGE_CREATE_REQ:
                    bytes_to_pack += size(upd->handle, upd->elem1, upd->elem2, upd->loc1, upd->loc2);
                    break;
                
                case NODE_DELETE_REQ:
                    bytes_to_pack += size(upd->elem1, upd->loc1);
                    break;
                
                case EDGE_DELETE_REQ:
                    bytes_to_pack += size(upd->elem1, upd->loc1);
                    break;
                
                default:
                    DEBUG << "bad msg type" << std::endl; 
            }
        }
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = m.buf->pack_at(BUSYBEE_HEADER_SIZE);

        for (auto &upd: tx.writes) {
            packer = packer << upd->type;
            pack_buffer(packer, vt_id, tx.timestamp, upd->qts);
            switch (upd->type) {
                case NODE_CREATE_REQ:
                    pack_buffer(packer, upd->handle, upd->loc1);
                    break;
                
                case EDGE_CREATE_REQ:
                    pack_buffer(packer, upd->handle, upd->elem1, upd->elem2, upd->loc1, upd->loc2);
                    break;
                
                case NODE_DELETE_REQ:
                    pack_buffer(packer, upd->elem1, upd->loc1);
                    break;
                
                case EDGE_DELETE_REQ:
                    pack_buffer(packer, upd->elem1, upd->loc1);
                    break;
                
                default:
                    DEBUG << "bad msg type" << std::endl; 
            }
        }
    }

    inline void
    unpack_client_tx(message &m, coordinator::pending_tx &tx)
    {
        uint64_t num_tx;
        uint32_t type;
        enum msg_type mtype;
        e::unpacker unpacker = m.buf->unpack_from(BUSYBEE_HEADER_SIZE);
        unpacker = unpacker >> type;
        mtype = (enum msg_type)type;
        assert(mtype == CLIENT_TX_INIT);
        unpack_buffer(unpacker, num_tx);
        while (num_tx-- > 0) {
            auto upd = std::make_shared<coordinator::pending_update>();
            tx.writes.emplace_back(upd);
            unpacker = unpacker >> type;
            mtype = (enum msg_type)type;
            switch (type) {
                case CLIENT_NODE_CREATE_REQ:
                    upd->type = NODE_CREATE_REQ;
                    unpack_buffer(unpacker, upd->handle); 
                    break;

                case CLIENT_EDGE_CREATE_REQ:
                    upd->type = EDGE_CREATE_REQ;
                    unpack_buffer(unpacker, upd->handle, upd->elem1, upd->elem2);
                    break;

                case CLIENT_NODE_DELETE_REQ:
                    upd->type = NODE_DELETE_REQ;
                    unpack_buffer(unpacker, upd->elem1);
                    break;

                case CLIENT_EDGE_DELETE_REQ:
                    upd->type = EDGE_DELETE_REQ;
                    unpack_buffer(unpacker, upd->elem1);
                    break;

                default:
                    DEBUG << "bad msg type" << std::endl;
            }
        }
    }

}

#endif
