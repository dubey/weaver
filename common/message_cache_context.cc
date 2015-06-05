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

#include "common/stl_serialization.h"
#include "node_prog/cache_response.h"

// size methods
uint64_t
message :: size(const node_prog::edge_cache_context &t)
{
    uint64_t toRet = size(t.edge_handle)
        + size(t.nbr)
        + size(t.props_added)
        + size(t.props_deleted);
    return toRet;
}

uint64_t
message :: size(const node_prog::node_cache_context &t)
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
void
message :: pack_buffer(e::packer &packer, const node_prog::edge_cache_context &t)
{
    pack_buffer(packer, t.edge_handle);
    pack_buffer(packer, t.nbr);
    pack_buffer(packer, t.props_added);
    pack_buffer(packer, t.props_deleted);
}

void
message :: pack_buffer(e::packer &packer, const node_prog::node_cache_context &t)
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
void
message :: unpack_buffer(e::unpacker &unpacker, node_prog::edge_cache_context &t)
{
    unpack_buffer(unpacker, t.edge_handle);
    unpack_buffer(unpacker, t.nbr);
    unpack_buffer(unpacker, t.props_added);
    unpack_buffer(unpacker, t.props_deleted);
}
void
message :: unpack_buffer(e::unpacker &unpacker, node_prog::node_cache_context &t)
{
    unpack_buffer(unpacker, t.node);
    unpack_buffer(unpacker, t.node_deleted);
    unpack_buffer(unpacker, t.props_added);
    unpack_buffer(unpacker, t.props_deleted);
    unpack_buffer(unpacker, t.edges_added);
    unpack_buffer(unpacker, t.edges_modified);
    unpack_buffer(unpacker, t.edges_deleted);
}
