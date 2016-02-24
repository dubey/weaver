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
message :: size(void *aux_args, const node_prog::edge_cache_context &t)
{
    uint64_t toRet = size(aux_args, t.edge_handle)
        + size(aux_args, t.nbr)
        + size(aux_args, t.props_added)
        + size(aux_args, t.props_deleted);
    return toRet;
}

uint64_t
message :: size(void *aux_args, const node_prog::node_cache_context &t)
{
    uint64_t toRet = size(aux_args, t.node) +
        size(aux_args, t.node_deleted) +
        size(aux_args, t.props_added) +
        size(aux_args, t.props_deleted) +
        size(aux_args, t.edges_added) +
        size(aux_args, t.edges_modified) +
        size(aux_args, t.edges_deleted);
    return toRet;
}


// packing methods
void
message :: pack_buffer(e::packer &packer, void *aux_args, const node_prog::edge_cache_context &t)
{
    pack_buffer(packer, aux_args, t.edge_handle);
    pack_buffer(packer, aux_args, t.nbr);
    pack_buffer(packer, aux_args, t.props_added);
    pack_buffer(packer, aux_args, t.props_deleted);
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const node_prog::node_cache_context &t)
{
    pack_buffer(packer, aux_args, t.node);
    pack_buffer(packer, aux_args, t.node_deleted);
    pack_buffer(packer, aux_args, t.props_added);
    pack_buffer(packer, aux_args, t.props_deleted);
    pack_buffer(packer, aux_args, t.edges_added);
    pack_buffer(packer, aux_args, t.edges_modified);
    pack_buffer(packer, aux_args, t.edges_deleted);
}

// unpacking methods
void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, node_prog::edge_cache_context &t)
{
    unpack_buffer(unpacker, aux_args, t.edge_handle);
    unpack_buffer(unpacker, aux_args, t.nbr);
    unpack_buffer(unpacker, aux_args, t.props_added);
    unpack_buffer(unpacker, aux_args, t.props_deleted);
}
void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, node_prog::node_cache_context &t)
{
    unpack_buffer(unpacker, aux_args, t.node);
    unpack_buffer(unpacker, aux_args, t.node_deleted);
    unpack_buffer(unpacker, aux_args, t.props_added);
    unpack_buffer(unpacker, aux_args, t.props_deleted);
    unpack_buffer(unpacker, aux_args, t.edges_added);
    unpack_buffer(unpacker, aux_args, t.edges_modified);
    unpack_buffer(unpacker, aux_args, t.edges_deleted);
}
