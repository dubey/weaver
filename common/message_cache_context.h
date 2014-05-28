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

#ifndef weaver_common_message_cache_context_h_
#define weaver_common_message_cache_context_h_

#include "message.h"
#include "message_graph_elem.h"
#include "node_prog/cache_response.h"


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
        uint64_t toRet = size(t.node) +
            size(t.node_deleted) +
            size(t.props_added) +
            size(t.props_deleted) +
            size(t.edges_added) +
            size(t.edges_modified) +
            size(t.edges_deleted);
        return toRet;
    }


    // packing methods
    inline void
    pack_buffer(e::buffer::packer &packer, const node_prog::edge_cache_context &t)
    {
        pack_buffer(packer, t.edge_handle);
        pack_buffer(packer, t.nbr);
        pack_buffer(packer, t.props_added);
        pack_buffer(packer, t.props_deleted);
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const node_prog::node_cache_context &t)
    {
        pack_buffer(packer, t.node);
        pack_buffer(packer, t.node_deleted);
        pack_buffer(packer, t.props_added);
        pack_buffer(packer, t.props_deleted);
        pack_buffer(packer, t.edges_added);
        pack_buffer(packer, t.edges_modified);
        pack_buffer(packer, t.edges_deleted);
    }

    // unpacking methods
    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::edge_cache_context &t)
    {
        unpack_buffer(unpacker, t.edge_handle);
        unpack_buffer(unpacker, t.nbr);
        unpack_buffer(unpacker, t.props_added);
        unpack_buffer(unpacker, t.props_deleted);
    }
    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::node_cache_context &t)
    {
        unpack_buffer(unpacker, t.node);
        unpack_buffer(unpacker, t.node_deleted);
        unpack_buffer(unpacker, t.props_added);
        unpack_buffer(unpacker, t.props_deleted);
        unpack_buffer(unpacker, t.edges_added);
        unpack_buffer(unpacker, t.edges_modified);
        unpack_buffer(unpacker, t.edges_deleted);
    }
}

#endif
