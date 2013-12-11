/*
 * ===============================================================
 *    Description:  context packers, unpackers.
 *
 *        Created:  05/20/2013 02:40:32 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __MESSAGE_CACHE_CONTEXT__
#define __MESSAGE_CACHE_CONTEXT__

#include "message.h"
#include "message_graph_elem.h"

namespace message
{
    // size methods
    inline uint64_t
    size(const db::caching::node_cache_context &t)
    {
        uint64_t toRet = size(t.node_deleted)
            + size(t.edges_added)
            + size(t.edges_deleted);
        return toRet;
    }


    // packing methods
    inline void
    pack_buffer(e::buffer::packer &packer, const db::caching::node_cache_context &t)
    {
        pack_buffer(packer, t.node_deleted);
        pack_buffer(packer, t.edges_added);
        pack_buffer(packer, t.edges_deleted);
    }

    // unpacking methods
    inline void
    unpack_buffer(e::unpacker &unpacker, db::caching::node_cache_context &t)
    {
        unpack_buffer(unpacker, t.node_deleted);
        unpack_buffer(unpacker, t.edges_added);
        unpack_buffer(unpacker, t.edges_deleted);
    }
}

#endif
