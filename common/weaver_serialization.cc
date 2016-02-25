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

#include <dlfcn.h>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/weaver_serialization.h"
#include "common/stl_serialization.h"
#include "common/enum_serialization.h"
#include "common/vclock.h"
#include "node_prog/base_classes.h"
#include "node_prog/property.h"
#include "db/remote_node.h"
#include "db/property.h"
#include "client/datastructures.h"

using node_prog::Node_Parameters_Base;
using node_prog::Node_State_Base;
using node_prog::np_param_ptr_t;
using node_prog::np_state_ptr_t;

typedef np_param_ptr_t (*param_ctor_func_t)();
typedef uint64_t (*param_size_func_t)(const Node_Parameters_Base&, void*);
typedef void (*param_pack_func_t)(const Node_Parameters_Base&, e::packer&, void*);
typedef void (*param_unpack_func_t)(Node_Parameters_Base&, e::unpacker&, void*);

typedef np_state_ptr_t (*state_ctor_func_t)();
typedef uint64_t (*state_size_func_t)(const Node_State_Base&, void*);
typedef void (*state_pack_func_t)(const Node_State_Base&, e::packer&, void*);
typedef void (*state_unpack_func_t)(Node_State_Base&, e::unpacker&, void*);

// size functions

uint64_t
message :: size(void *prog_handle, const np_param_ptr_t &t)
{
    assert(prog_handle != nullptr);
    param_size_func_t size_f = (param_size_func_t)dlsym(prog_handle, "param_size");
    return size_f(*t, prog_handle);
}

uint64_t
message :: size(void *prog_handle, const np_state_ptr_t &t)
{
    assert(prog_handle != nullptr);
    state_size_func_t size_f = (state_size_func_t)dlsym(prog_handle, "state_size");
    return size_f(*t, prog_handle);
}

uint64_t
message :: size(void *aux_args, const node_prog::Cache_Value_Base &t)
{
    return t.size(aux_args);
}

uint64_t
message :: size(void *aux_args, const vc::vclock &t)
{
    return size(aux_args, t.vt_id)
        + size(aux_args, t.clock);
}

uint64_t
message :: size(void *aux_args, const node_prog::property &t)
{
    return size(aux_args, t.key)
        + size(aux_args, t.value);
}

uint64_t
message :: size(void *aux_args, const db::property &t)
{
    return size(aux_args, t.key)
        + size(aux_args, t.value)
        + size(aux_args, t.get_creat_time())
        + size(aux_args, t.get_del_time());
}

uint64_t
message :: size(void *aux_args, const db::remote_node &t)
{
    return size(aux_args, t.loc) + size(aux_args, t.handle);
}

uint64_t
message :: size(void *aux_args, const std::shared_ptr<transaction::pending_update> &t)
{
    uint64_t sz = size(aux_args, t->type)
         + size(aux_args, t->handle)
         + size(aux_args, t->handle1)
         + size(aux_args, t->handle2)
         + size(aux_args, t->alias1)
         + size(aux_args, t->alias2)
         + size(aux_args, t->loc1)
         + size(aux_args, t->loc2);
    if (t->type == transaction::NODE_SET_PROPERTY
     || t->type == transaction::EDGE_SET_PROPERTY) {
        sz += size(aux_args, *t->key)
         + size(aux_args, *t->value);
    }
    return sz;
}

uint64_t
message :: size(void *aux_args, const std::shared_ptr<transaction::nop_data> &t)
{
    return size(aux_args, t->max_done_clk)
         + size(aux_args, t->outstanding_progs)
         + size(aux_args, t->shard_node_count)
         + size(aux_args, t->done_txs);
}

uint64_t
message :: size(void *aux_args, const transaction::pending_tx &t)
{
    uint64_t sz = size(aux_args, t.type)
        + size(aux_args, t.id)
        + size(aux_args, t.timestamp)
        + size(aux_args, t.vt_seq)
        + size(aux_args, t.qts)
        + size(aux_args, t.shard_write);
    if (t.type == transaction::UPDATE) {
        sz = sz + size(aux_args, t.writes)
            + size(aux_args, t.sender);
    } else {
        sz = sz + size(aux_args, t.nop);
    }
    return sz;
}

uint64_t
message :: size(void *aux_args, const cl::node &t)
{
    return size(aux_args, t.handle)
         + size(aux_args, t.properties)
         + size(aux_args, t.out_edges)
         + size(aux_args, t.aliases);
}

uint64_t
message :: size(void *aux_args, const cl::edge &t)
{
    return size(aux_args, t.handle)
         + size(aux_args, t.start_node)
         + size(aux_args, t.end_node)
         + size(aux_args, t.properties);
}

uint64_t
message::size(void *aux_args, const predicate::prop_predicate &t)
{
    return size(aux_args, t.key)
         + size(aux_args, t.value)
         + size(aux_args, t.rel);
}


// packing functions

void
message :: pack_buffer(e::packer &packer, void *prog_handle, const np_param_ptr_t &t)
{
    assert(prog_handle != nullptr);
    param_pack_func_t pack_f = (param_pack_func_t)dlsym(prog_handle, "param_pack");
    pack_f(*t, packer, prog_handle);
}

void
message :: pack_buffer(e::packer &packer, void *prog_handle, const np_state_ptr_t &t)
{
    assert(prog_handle != nullptr);
    state_pack_func_t pack_f = (state_pack_func_t)dlsym(prog_handle, "state_pack");
    pack_f(*t, packer, prog_handle);
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const node_prog::Cache_Value_Base *&t)
{
    t->pack(packer, aux_args);
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const vc::vclock &t)
{
    pack_buffer(packer, aux_args, t.vt_id);
    pack_buffer(packer, aux_args, t.clock);
}

void 
message :: pack_buffer(e::packer &packer, void *aux_args, const node_prog::property &t)
{
    pack_buffer(packer, aux_args, t.key);
    pack_buffer(packer, aux_args, t.value);
}

void 
message :: pack_buffer(e::packer &packer, void *aux_args, const db::property &t)
{
    pack_buffer(packer, aux_args, t.key);
    pack_buffer(packer, aux_args, t.value);
    pack_buffer(packer, aux_args, t.get_creat_time());
    pack_buffer(packer, aux_args, t.get_del_time());
}

void 
message :: pack_buffer(e::packer &packer, void *aux_args, const db::remote_node &t)
{
    pack_buffer(packer, aux_args, t.loc);
    pack_buffer(packer, aux_args, t.handle);
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const std::shared_ptr<transaction::pending_update> &t)
{
    pack_buffer(packer, aux_args, t->type);
    pack_buffer(packer, aux_args, t->handle);
    pack_buffer(packer, aux_args, t->handle1);
    pack_buffer(packer, aux_args, t->handle2);
    pack_buffer(packer, aux_args, t->alias1);
    pack_buffer(packer, aux_args, t->alias2);
    pack_buffer(packer, aux_args, t->loc1);
    pack_buffer(packer, aux_args, t->loc2);
    if (t->type == transaction::NODE_SET_PROPERTY
     || t->type == transaction::EDGE_SET_PROPERTY) {
        pack_buffer(packer, aux_args, *t->key);
        pack_buffer(packer, aux_args, *t->value);
    }
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const std::shared_ptr<transaction::nop_data> &t)
{
    pack_buffer(packer, aux_args, t->max_done_clk);
    pack_buffer(packer, aux_args, t->outstanding_progs);
    pack_buffer(packer, aux_args, t->shard_node_count);
    pack_buffer(packer, aux_args, t->done_txs);
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const transaction::pending_tx &t)
{
    pack_buffer(packer, aux_args, t.type);
    pack_buffer(packer, aux_args, t.id);
    pack_buffer(packer, aux_args, t.timestamp);
    pack_buffer(packer, aux_args, t.vt_seq);
    pack_buffer(packer, aux_args, t.qts);
    pack_buffer(packer, aux_args, t.shard_write);
    if (t.type == transaction::UPDATE) {
        pack_buffer(packer, aux_args, t.writes);
        pack_buffer(packer, aux_args, t.sender);
    } else {
        pack_buffer(packer, aux_args, t.nop);
    }
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const cl::node &t)
{
    pack_buffer(packer, aux_args, t.handle);
    pack_buffer(packer, aux_args, t.properties);
    pack_buffer(packer, aux_args, t.out_edges);
    pack_buffer(packer, aux_args, t.aliases);
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const cl::edge &t)
{
    pack_buffer(packer, aux_args, t.handle);
    pack_buffer(packer, aux_args, t.start_node);
    pack_buffer(packer, aux_args, t.end_node);
    pack_buffer(packer, aux_args, t.properties);
}

void
message :: pack_buffer(e::packer &packer, void *aux_args, const predicate::prop_predicate &t)
{
    pack_buffer(packer, aux_args, t.key);
    pack_buffer(packer, aux_args, t.value);
    pack_buffer(packer, aux_args, t.rel);
}


// unpacking functions

void
message :: unpack_buffer(e::unpacker &unpacker, void *prog_handle, np_param_ptr_t &t)
{
    assert(prog_handle != nullptr);
    param_ctor_func_t ctor_f = (param_ctor_func_t)dlsym(prog_handle, "ctor_prog_param");
    t = ctor_f();
    param_unpack_func_t unpack_f = (param_unpack_func_t)dlsym(prog_handle, "param_unpack");
    unpack_f(*t, unpacker, prog_handle);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *prog_handle, np_state_ptr_t &t)
{
    assert(prog_handle != nullptr);
    state_ctor_func_t ctor_f = (state_ctor_func_t)dlsym(prog_handle, "ctor_prog_state");
    t = ctor_f();
    state_unpack_func_t unpack_f = (state_unpack_func_t)dlsym(prog_handle, "state_unpack");
    unpack_f(*t, unpacker, prog_handle);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, node_prog::Cache_Value_Base &t)
{
    t.unpack(unpacker, aux_args);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, vc::vclock &t)
{
    unpack_buffer(unpacker, aux_args, t.vt_id);
    unpack_buffer(unpacker, aux_args, t.clock);
}

void 
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, node_prog::property &t)
{
    unpack_buffer(unpacker, aux_args, t.key);
    unpack_buffer(unpacker, aux_args, t.value);
}
void 
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, db::property &t)
{
    unpack_buffer(unpacker, aux_args, t.key);
    unpack_buffer(unpacker, aux_args, t.value);

    vclock_ptr_t tcreat, tdel;

    unpack_buffer(unpacker, aux_args, tcreat);
    unpack_buffer(unpacker, aux_args, tdel);
    t.update_creat_time(tcreat);
    t.update_del_time(tdel);
}

void 
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, db::remote_node& t)
{
    unpack_buffer(unpacker, aux_args, t.loc);
    unpack_buffer(unpacker, aux_args, t.handle);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, std::shared_ptr<transaction::pending_update> &t)
{
    t = std::make_shared<transaction::pending_update>();
    unpack_buffer(unpacker, aux_args, t->type);
    unpack_buffer(unpacker, aux_args, t->handle);
    unpack_buffer(unpacker, aux_args, t->handle1);
    unpack_buffer(unpacker, aux_args, t->handle2);
    unpack_buffer(unpacker, aux_args, t->alias1);
    unpack_buffer(unpacker, aux_args, t->alias2);
    unpack_buffer(unpacker, aux_args, t->loc1);
    unpack_buffer(unpacker, aux_args, t->loc2);
    if (t->type == transaction::NODE_SET_PROPERTY
     || t->type == transaction::EDGE_SET_PROPERTY) {
        t->key.reset(new std::string());
        t->value.reset(new std::string());
        unpack_buffer(unpacker, aux_args, *t->key);
        unpack_buffer(unpacker, aux_args, *t->value);
    }
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, std::shared_ptr<transaction::nop_data> &t)
{
    t = std::make_shared<transaction::nop_data>();
    unpack_buffer(unpacker, aux_args, t->max_done_clk);
    unpack_buffer(unpacker, aux_args, t->outstanding_progs);
    unpack_buffer(unpacker, aux_args, t->shard_node_count);
    unpack_buffer(unpacker, aux_args, t->done_txs);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, transaction::pending_tx &t)
{
    unpack_buffer(unpacker, aux_args, t.type);
    unpack_buffer(unpacker, aux_args, t.id);
    unpack_buffer(unpacker, aux_args, t.timestamp);
    unpack_buffer(unpacker, aux_args, t.vt_seq);
    unpack_buffer(unpacker, aux_args, t.qts);
    unpack_buffer(unpacker, aux_args, t.shard_write);
    if (t.type == transaction::UPDATE) {
        unpack_buffer(unpacker, aux_args, t.writes);
        unpack_buffer(unpacker, aux_args, t.sender);
    } else {
        unpack_buffer(unpacker, aux_args, t.nop);
    }
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, cl::node &t)
{
    unpack_buffer(unpacker, aux_args, t.handle);
    unpack_buffer(unpacker, aux_args, t.properties);
    unpack_buffer(unpacker, aux_args, t.out_edges);
    unpack_buffer(unpacker, aux_args, t.aliases);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, cl::edge &t)
{
    unpack_buffer(unpacker, aux_args, t.handle);
    unpack_buffer(unpacker, aux_args, t.start_node);
    unpack_buffer(unpacker, aux_args, t.end_node);
    unpack_buffer(unpacker, aux_args, t.properties);
}

void
message :: unpack_buffer(e::unpacker &unpacker, void *aux_args, predicate::prop_predicate &t)
{
    unpack_buffer(unpacker, aux_args, t.key);
    unpack_buffer(unpacker, aux_args, t.value);
    unpack_buffer(unpacker, aux_args, t.rel);
}
