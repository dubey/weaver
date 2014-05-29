/*
 * ===============================================================
 *    Description:  Client-side transaction data structures.
 *
 *        Created:  09/04/2013 08:57:13 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_client_transaction_h_
#define weaver_client_transaction_h_

#include <vector>

#include "common/message.h"

namespace client
{
    struct pending_update
    {
        message::msg_type type;
        uint64_t elem1, elem2, handle;
        std::string key, value;
    };

    typedef std::vector<std::shared_ptr<pending_update>> tx_list_t;
}

namespace message
{
    inline void
    prepare_tx_message_client(message &m, const client::tx_list_t &tx)
    {
        assert(tx.size() <= UINT32_MAX);
        uint32_t num_writes = tx.size();
        enum msg_type mtype;
        uint64_t bytes_to_pack = size(mtype) * (1 + tx.size()) + size(num_writes);

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

        pack_buffer_wrapper(packer, CLIENT_TX_INIT);
        pack_buffer(packer, num_writes);

        for (auto &upd: tx) {
            pack_buffer_wrapper(packer, upd->type);
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
