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
#include "db/cache/prog_cache.h"

/*
    struct edge_cache_context
    {
        uint64_t edge_handle;
        db::element::remote_node nbr;

        edge_cache_context(uint64_t handle, db::element::remote_node &nbr) : edge_handle(handle), nbr(nbr) {};

        std::vector<node_prog::property> props_added;
        std::vector<node_prog::property> props_deleted;
    };

    struct node_cache_context
    {
        db::element::remote_node node;

        bool node_deleted;
        std::vector<node_prog::property> props_added;
        std::vector<node_prog::property> props_deleted;

        std::vector<edge_cache_context> edges_added;
        std::vector<edge_cache_context> edges_modified;
        std::vector<edge_cache_context> edges_deleted;
    };
    XXX make stuff in message.h for node_prog::property
    */

namespace message
{
    // size methods
    inline uint64_t
    size(const node_prog::edge_cache_context &t)
    {
        uint64_t toRet = size(t.edge_handle)
            + size(t.nbr)
            + size(t.props_added)
            + size(t.props_deleted);
        return toRet;
    }

    inline uint64_t
    size(const node_prog::node_cache_context &t)
    {
        uint64_t toRet = size(t.node_deleted_internal)
            + size(t.edges_added_internal)
            + size(t.edges_deleted_internal);
        return toRet;
    }


    // packing methods
    inline void
    pack_buffer(e::buffer::packer &packer, const node_prog::node_cache_context &t)
    {
        pack_buffer(packer, t.node_deleted_internal);
        pack_buffer(packer, t.edges_added_internal);
        pack_buffer(packer, t.edges_deleted_internal);
    }

    // unpacking methods
    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::node_cache_context &t)
    {
        unpack_buffer(unpacker, t.node_deleted_internal);
        unpack_buffer(unpacker, t.edges_added_internal);
        unpack_buffer(unpacker, t.edges_deleted_internal);
    }
}

#endif
