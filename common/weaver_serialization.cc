/*
 * ===============================================================
 *    Description:  Weaver data structures serialization.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/weaver_serialization.h"
#include "common/stl_serialization.h"
#include "common/enum_serialization.h"
#include "common/vclock.h"
#include "common/passert.h"
#include "node_prog/base_classes.h"
#include "node_prog/property.h"
#include "db/remote_node.h"
#include "db/property.h"
#include "client/datastructures.h"

// size functions

uint64_t
message :: size(const node_prog::Node_Parameters_Base &t)
{
    return t.size();
}

uint64_t
message :: size(const node_prog::Node_State_Base &t)
{
    return t.size();
}

uint64_t
message :: size(const node_prog::Cache_Value_Base &t)
{
    return t.size();
}

uint64_t
message :: size(const vc::vclock &t)
{
    return size(t.vt_id)
        + size(t.clock);
}

uint64_t
message :: size(const node_prog::property &t)
{
    return size(t.key)
        + size(t.value);
}

uint64_t
message :: size(const db::property &t)
{
    return size(t.key)
        + size(t.value)
        + size(t.get_creat_time())
        + size(t.get_del_time());
}

uint64_t
message :: size(const db::remote_node &t)
{
    return size(t.loc) + size(t.handle);
}

uint64_t
message :: size(const std::shared_ptr<transaction::pending_update> &t)
{
    uint64_t sz = size(t->type)
         + size(t->handle)
         + size(t->handle1)
         + size(t->handle2)
         + size(t->alias1)
         + size(t->alias2)
         + size(t->loc1)
         + size(t->loc2);
    if (t->type == transaction::NODE_SET_PROPERTY
     || t->type == transaction::EDGE_SET_PROPERTY) {
        sz += size(*t->key)
         + size(*t->value);
    }
    return sz;
}

uint64_t
message :: size(const std::shared_ptr<transaction::nop_data> &t)
{
    return size(t->max_done_clk)
         + size(t->outstanding_progs)
         + size(t->shard_node_count)
         + size(t->done_txs);
}

uint64_t
message :: size(const transaction::pending_tx &t)
{
    uint64_t sz = size(t.type)
        + size(t.id)
        + size(t.timestamp)
        + size(t.vt_seq)
        + size(t.qts)
        + size(t.shard_write);
    if (t.type == transaction::UPDATE) {
        sz = sz + size(t.writes)
            + size(t.sender);
    } else {
        sz = sz + size(t.nop);
    }
    return sz;
}

uint64_t
message :: size(const cl::node &t)
{
    return size(t.handle)
         + size(t.properties)
         + size(t.out_edges)
         + size(t.aliases);
}

uint64_t
message :: size(const cl::edge &t)
{
    return size(t.handle)
         + size(t.start_node)
         + size(t.end_node)
         + size(t.properties);
}

uint64_t
message::size(const predicate::prop_predicate &t)
{
    return size(t.key)
         + size(t.value)
         + size(t.rel);
}


// packing functions

void
message :: pack_buffer(e::packer &packer, const node_prog::Node_Parameters_Base &t)
{
    t.pack(packer);
}

void
message :: pack_buffer(e::packer &packer, const node_prog::Node_State_Base &t)
{
    t.pack(packer);
}

void
message :: pack_buffer(e::packer &packer, const node_prog::Cache_Value_Base *&t)
{
    t->pack(packer);
}

void
message :: pack_buffer(e::packer &packer, const vc::vclock &t)
{
    pack_buffer(packer, t.vt_id);
    pack_buffer(packer, t.clock);
}

void 
message :: pack_buffer(e::packer &packer, const node_prog::property &t)
{
    pack_buffer(packer, t.key);
    pack_buffer(packer, t.value);
}

void 
message :: pack_buffer(e::packer &packer, const db::property &t)
{
    pack_buffer(packer, t.key);
    pack_buffer(packer, t.value);
    pack_buffer(packer, t.get_creat_time());
    pack_buffer(packer, t.get_del_time());
}

void 
message :: pack_buffer(e::packer &packer, const db::remote_node &t)
{
    pack_buffer(packer, t.loc);
    pack_buffer(packer, t.handle);
}

void
message :: pack_buffer(e::packer &packer, const std::shared_ptr<transaction::pending_update> &t)
{
    pack_buffer(packer, t->type);
    pack_buffer(packer, t->handle);
    pack_buffer(packer, t->handle1);
    pack_buffer(packer, t->handle2);
    pack_buffer(packer, t->alias1);
    pack_buffer(packer, t->alias2);
    pack_buffer(packer, t->loc1);
    pack_buffer(packer, t->loc2);
    if (t->type == transaction::NODE_SET_PROPERTY
     || t->type == transaction::EDGE_SET_PROPERTY) {
        pack_buffer(packer, *t->key);
        pack_buffer(packer, *t->value);
    }
}

void
message :: pack_buffer(e::packer &packer, const std::shared_ptr<transaction::nop_data> &t)
{
    pack_buffer(packer, t->max_done_clk);
    pack_buffer(packer, t->outstanding_progs);
    pack_buffer(packer, t->shard_node_count);
    pack_buffer(packer, t->done_txs);
}

void
message :: pack_buffer(e::packer &packer, const transaction::pending_tx &t)
{
    pack_buffer(packer, t.type);
    pack_buffer(packer, t.id);
    pack_buffer(packer, t.timestamp);
    pack_buffer(packer, t.vt_seq);
    pack_buffer(packer, t.qts);
    pack_buffer(packer, t.shard_write);
    if (t.type == transaction::UPDATE) {
        pack_buffer(packer, t.writes);
        pack_buffer(packer, t.sender);
    } else {
        pack_buffer(packer, t.nop);
    }
}

void
message :: pack_buffer(e::packer &packer, const cl::node &t)
{
    pack_buffer(packer, t.handle);
    pack_buffer(packer, t.properties);
    pack_buffer(packer, t.out_edges);
    pack_buffer(packer, t.aliases);
}

void
message :: pack_buffer(e::packer &packer, const cl::edge &t)
{
    pack_buffer(packer, t.handle);
    pack_buffer(packer, t.start_node);
    pack_buffer(packer, t.end_node);
    pack_buffer(packer, t.properties);
}

void
message :: pack_buffer(e::packer &packer, const predicate::prop_predicate &t)
{
    pack_buffer(packer, t.key);
    pack_buffer(packer, t.value);
    pack_buffer(packer, t.rel);
}


// unpacking functions

void
message :: unpack_buffer(e::unpacker &unpacker, node_prog::Node_Parameters_Base &t)
{
    t.unpack(unpacker);
}

void
message :: unpack_buffer(e::unpacker &unpacker, node_prog::Node_State_Base &t)
{
    t.unpack(unpacker);
}

void
message :: unpack_buffer(e::unpacker &unpacker, node_prog::Cache_Value_Base &t)
{
    t.unpack(unpacker);
}

void
message :: unpack_buffer(e::unpacker &unpacker, vc::vclock &t)
{
    unpack_buffer(unpacker, t.vt_id);
    unpack_buffer(unpacker, t.clock);
}

void 
message :: unpack_buffer(e::unpacker &unpacker, node_prog::property &t)
{
    unpack_buffer(unpacker, t.key);
    unpack_buffer(unpacker, t.value);
}
void 
message :: unpack_buffer(e::unpacker &unpacker, db::property &t)
{
    unpack_buffer(unpacker, t.key);
    unpack_buffer(unpacker, t.value);

    vclock_ptr_t tcreat, tdel;

    unpack_buffer(unpacker, tcreat);
    unpack_buffer(unpacker, tdel);
    t.update_creat_time(tcreat);
    t.update_del_time(tdel);
}

void 
message :: unpack_buffer(e::unpacker &unpacker, db::remote_node& t)
{
    unpack_buffer(unpacker, t.loc);
    unpack_buffer(unpacker, t.handle);
}

void
message :: unpack_buffer(e::unpacker &unpacker, std::shared_ptr<transaction::pending_update> &t)
{
    t = std::make_shared<transaction::pending_update>();
    unpack_buffer(unpacker, t->type);
    unpack_buffer(unpacker, t->handle);
    unpack_buffer(unpacker, t->handle1);
    unpack_buffer(unpacker, t->handle2);
    unpack_buffer(unpacker, t->alias1);
    unpack_buffer(unpacker, t->alias2);
    unpack_buffer(unpacker, t->loc1);
    unpack_buffer(unpacker, t->loc2);
    if (t->type == transaction::NODE_SET_PROPERTY
     || t->type == transaction::EDGE_SET_PROPERTY) {
        t->key.reset(new std::string());
        t->value.reset(new std::string());
        unpack_buffer(unpacker, *t->key);
        unpack_buffer(unpacker, *t->value);
    }
}

void
message :: unpack_buffer(e::unpacker &unpacker, std::shared_ptr<transaction::nop_data> &t)
{
    t = std::make_shared<transaction::nop_data>();
    unpack_buffer(unpacker, t->max_done_clk);
    unpack_buffer(unpacker, t->outstanding_progs);
    unpack_buffer(unpacker, t->shard_node_count);
    unpack_buffer(unpacker, t->done_txs);
}

void
message :: unpack_buffer(e::unpacker &unpacker, transaction::pending_tx &t)
{
    unpack_buffer(unpacker, t.type);
    unpack_buffer(unpacker, t.id);
    unpack_buffer(unpacker, t.timestamp);
    unpack_buffer(unpacker, t.vt_seq);
    unpack_buffer(unpacker, t.qts);
    unpack_buffer(unpacker, t.shard_write);
    if (t.type == transaction::UPDATE) {
        unpack_buffer(unpacker, t.writes);
        unpack_buffer(unpacker, t.sender);
    } else {
        unpack_buffer(unpacker, t.nop);
    }
}

void
message :: unpack_buffer(e::unpacker &unpacker, cl::node &t)
{
    unpack_buffer(unpacker, t.handle);
    unpack_buffer(unpacker, t.properties);
    unpack_buffer(unpacker, t.out_edges);
    unpack_buffer(unpacker, t.aliases);
}

void
message :: unpack_buffer(e::unpacker &unpacker, cl::edge &t)
{
    unpack_buffer(unpacker, t.handle);
    unpack_buffer(unpacker, t.start_node);
    unpack_buffer(unpacker, t.end_node);
    unpack_buffer(unpacker, t.properties);
}

void
message :: unpack_buffer(e::unpacker &unpacker, predicate::prop_predicate &t)
{
    unpack_buffer(unpacker, t.key);
    unpack_buffer(unpacker, t.value);
    unpack_buffer(unpacker, t.rel);
}
