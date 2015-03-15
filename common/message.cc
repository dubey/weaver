/*
 * ===============================================================
 *    Description:  Implementation of basic message packing
 *                  functions.
 *
 *        Created:  2014-05-29 14:57:46
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/message.h"
#include "client/datastructures.h"

const char*
message :: to_string(const msg_type &t)
{
    switch (t) {
        case CLIENT_TX_INIT:
            return "CLIENT_TX_INIT";
        case CLIENT_TX_SUCCESS:
            return "CLIENT_TX_SUCCESS";
        case CLIENT_TX_ABORT:
            return "CLIENT_TX_ABORT";
        case CLIENT_NODE_PROG_REQ:
            return "CLIENT_NODE_PROG_REQ";
        case CLIENT_NODE_PROG_REPLY:
            return "CLIENT_NODE_PROG_REPLY";
        case START_MIGR:
            return "START_MIGR";
        case ONE_STREAM_MIGR:
            return "ONE_STREAM_MIGR";
        case EXIT_WEAVER:
            return "EXIT_WEAVER";
        case TX_INIT:
            return "TX_INIT";
        case TX_DONE:
            return "TX_DONE";
        case NODE_PROG:
            return "NODE_PROG";
        case NODE_PROG_RETURN:
            return "NODE_PROG_RETURN";
        case NODE_PROG_RETRY:
            return "NODE_PROG_RETRY";
        case NODE_PROG_NOTFOUND:
            return "NODE_PROG_NOTFOUND";
        case NODE_CONTEXT_FETCH:
            return "NODE_CONTEXT_FETCH";
        case NODE_CONTEXT_REPLY:
            return "NODE_CONTEXT_REPLY";
        case CACHE_UPDATE:
            return "CACHE_UPDATE";
        case CACHE_UPDATE_ACK:
            return "CACHE_UPDATE_ACK";
        case MIGRATE_SEND_NODE:
            return "MIGRATE_SEND_NODE";
        case MIGRATED_NBR_UPDATE:
            return "MIGRATED_NBR_UPDATE";
        case MIGRATED_NBR_ACK:
            return "MIGRATED_NBR_ACK";
        case MIGRATION_TOKEN:
            return "MIGRATION_TOKEN";
        case CLIENT_NODE_COUNT:
            return "CLIENT_NODE_COUNT";
        case NODE_COUNT_REPLY:
            return "NODE_COUNT_REPLY";
        case RESTORE_DONE:
            return "RESTORE_DONE";
        case LOADED_GRAPH:
            return "LOADED_GRAPH";
        case VT_CLOCK_UPDATE:
            return "VT_CLOCK_UPDATE";
        case VT_CLOCK_UPDATE_ACK:
            return "VT_CLOCK_UPDATE_ACK";
        case VT_NOP:
            return "VT_NOP";
        case VT_NOP_ACK:
            return "VT_NOP_ACK";
        case DONE_MIGR:
            return "DONE_MIGR";
        case ERROR:
            return "ERROR";
    }

    return "";
}

// size functions

uint64_t
message :: size(const enum msg_type &)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const enum node_prog::prog_type&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const enum transaction::update_type&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const enum transaction::tx_type&)
{
    return sizeof(uint8_t);
}

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
message :: size(const bool&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const char&)
{
    return sizeof(uint8_t);
}

uint64_t
message :: size(const uint16_t&)
{
    return sizeof(uint16_t);
}

uint64_t
message :: size(const uint32_t&)
{
    return sizeof(uint32_t);
}

uint64_t
message :: size(const uint64_t&)
{
    return sizeof(uint64_t);
}

uint64_t
message :: size(const int64_t&)
{
    return sizeof(int64_t);
}

uint64_t
message :: size(const int&)
{
    return sizeof(int);
}

uint64_t
message :: size(const double&)
{
    return sizeof(uint64_t);
}

uint64_t
message :: size(const std::string &t)
{
    return t.size() + sizeof(uint32_t);
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
    return size(t->max_done_id)
         + size(t->max_done_clk)
         + size(t->outstanding_progs)
         + size(t->done_reqs)
         + size(t->shard_node_count);
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
message :: size(const std::vector<bool> &t)
{
    return sizeof(uint32_t) + t.size()*sizeof(uint8_t);
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


// packing functions

void
message :: pack_buffer(e::buffer::packer &packer, const node_prog::Node_Parameters_Base &t)
{
    t.pack(packer);
}

void
message :: pack_buffer(e::buffer::packer &packer, const node_prog::Node_State_Base &t)
{
    t.pack(packer);
}

void
message :: pack_buffer(e::buffer::packer &packer, const node_prog::Cache_Value_Base *&t)
{
    t->pack(packer);
}

void
message :: pack_buffer(e::buffer::packer &packer, const enum msg_type &t)
{
    assert(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: pack_buffer(e::buffer::packer &packer, const enum node_prog::prog_type &t)
{
    assert(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: pack_buffer(e::buffer::packer &packer, const enum transaction::update_type &t)
{
    assert(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: pack_buffer(e::buffer::packer &packer, const enum transaction::tx_type &t)
{
    assert(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: pack_buffer(e::buffer::packer &packer, const bool &t)
{
    uint8_t to_pack = 0;
    if (t) {
        to_pack = 1;
    }
    packer = packer << to_pack;
}

void 
message :: pack_buffer(e::buffer::packer &packer, const uint8_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::buffer::packer &packer, const uint16_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::buffer::packer &packer, const uint32_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::buffer::packer &packer, const uint64_t &t)
{
    packer = packer << t;
}

void
message :: pack_buffer(e::buffer::packer &packer, const int64_t &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::buffer::packer &packer, const int &t)
{
    packer = packer << t;
}

void 
message :: pack_buffer(e::buffer::packer &packer, const double &t)
{
    uint64_t dbl;
    memcpy(&dbl, &t, sizeof(double)); //to avoid casting issues, probably could avoid copy
    packer = packer << dbl;
}

void
message :: pack_string(e::buffer::packer &packer, const std::string &t, const uint32_t sz)
{
    uint32_t words = sz / 8;
    uint32_t leftover_chars = sz % 8;

    const char *rawchars = t.data();
    const uint64_t *rawwords = (const uint64_t*) rawchars;

    for (uint32_t i = 0; i < words; i++) {
        pack_buffer(packer, rawwords[i]);
    }

    for (uint32_t i = 0; i < leftover_chars; i++) {
        pack_buffer(packer, (uint8_t) rawchars[words*8+i]);
    }
}

void 
message :: pack_buffer(e::buffer::packer &packer, const std::string &t)
{
    assert(t.size() <= UINT32_MAX);
    uint32_t strlen = t.size();
    packer = packer << strlen;

    pack_string(packer, t, strlen);
}

void
message :: pack_buffer(e::buffer::packer &packer, const vc::vclock &t)
{
    pack_buffer(packer, t.vt_id);
    pack_buffer(packer, t.clock);
}

void 
message :: pack_buffer(e::buffer::packer &packer, const node_prog::property &t)
{
    pack_buffer(packer, t.key);
    pack_buffer(packer, t.value);
}

void 
message :: pack_buffer(e::buffer::packer &packer, const db::property &t)
{
    pack_buffer(packer, t.key);
    pack_buffer(packer, t.value);
    pack_buffer(packer, t.get_creat_time());
    pack_buffer(packer, t.get_del_time());
}

void 
message :: pack_buffer(e::buffer::packer &packer, const db::remote_node &t)
{
    pack_buffer(packer, t.loc);
    pack_buffer(packer, t.handle);
}

void
message :: pack_buffer(e::buffer::packer &packer, const std::shared_ptr<transaction::pending_update> &t)
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
message :: pack_buffer(e::buffer::packer &packer, const std::shared_ptr<transaction::nop_data> &t)
{
    pack_buffer(packer, t->max_done_id);
    pack_buffer(packer, t->max_done_clk);
    pack_buffer(packer, t->outstanding_progs);
    pack_buffer(packer, t->done_reqs);
    pack_buffer(packer, t->shard_node_count);
}

void
message :: pack_buffer(e::buffer::packer &packer, const transaction::pending_tx &t)
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
message :: pack_buffer(e::buffer::packer &packer, const std::vector<bool> &t)
{
    uint32_t sz = t.size();
    pack_buffer(packer, sz);
    for (bool b: t) {
        pack_buffer(packer, b);
    }
}

void
message :: pack_buffer(e::buffer::packer &packer, const cl::node &t)
{
    pack_buffer(packer, t.handle);
    pack_buffer(packer, t.properties);
    pack_buffer(packer, t.out_edges);
    pack_buffer(packer, t.aliases);
}

void
message :: pack_buffer(e::buffer::packer &packer, const cl::edge &t)
{
    pack_buffer(packer, t.handle);
    pack_buffer(packer, t.start_node);
    pack_buffer(packer, t.end_node);
    pack_buffer(packer, t.properties);
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
message :: unpack_buffer(e::unpacker &unpacker, enum msg_type &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum msg_type)_type;
}

void
message :: unpack_buffer(e::unpacker &unpacker, enum node_prog::prog_type &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum node_prog::prog_type)_type;
}

void
message :: unpack_buffer(e::unpacker &unpacker, enum transaction::update_type &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum transaction::update_type)_type;
}

void
message :: unpack_buffer(e::unpacker &unpacker, enum transaction::tx_type &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum transaction::tx_type)_type;
}

void
message :: unpack_buffer(e::unpacker &unpacker, bool &t)
{
    uint8_t temp;
    unpacker = unpacker >> temp;
    t = (temp != 0);
}

void
message :: unpack_buffer(e::unpacker &unpacker, uint8_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, uint16_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, uint32_t &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, uint64_t &t)
{
    unpacker = unpacker >> t;
}

void
message :: unpack_buffer(e::unpacker &unpacker, int64_t &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, int &t)
{
    unpacker = unpacker >> t;
}

void 
message :: unpack_buffer(e::unpacker &unpacker, double &t)
{
    uint64_t dbl;
    unpacker = unpacker >> dbl;
    memcpy(&t, &dbl, sizeof(double)); //to avoid casting issues, probably could avoid copy
}

void
message :: unpack_string(e::unpacker &unpacker, std::string &t, const uint32_t sz)
{
    t.resize(sz);

    uint32_t words = sz / 8;
    uint32_t leftover_chars = sz % 8;

    const char* rawchars = t.data();
    uint8_t* rawuint8s = (uint8_t*) rawchars;
    uint64_t* rawwords = (uint64_t*) rawchars;

    for (uint32_t i = 0; i < words; i++) {
        unpack_buffer(unpacker, rawwords[i]);
    }

    for (uint32_t i = 0; i < leftover_chars; i++) {
        unpack_buffer(unpacker, rawuint8s[words*8+i]);
    }
}

void 
message :: unpack_buffer(e::unpacker &unpacker, std::string &t)
{
    uint32_t strlen;
    unpack_buffer(unpacker, strlen);

    unpack_string(unpacker, t, strlen);
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

    vc::vclock tcreat;
    std::unique_ptr<vc::vclock> tdel;

    unpack_buffer(unpacker, tcreat);
    t.update_creat_time(tcreat);

    unpack_buffer(unpacker, tdel);
    if (tdel) {
        t.update_del_time(*tdel);
    }
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
    unpack_buffer(unpacker, t->max_done_id);
    unpack_buffer(unpacker, t->max_done_clk);
    unpack_buffer(unpacker, t->outstanding_progs);
    unpack_buffer(unpacker, t->done_reqs);
    unpack_buffer(unpacker, t->shard_node_count);
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
message :: unpack_buffer(e::unpacker &unpacker, std::vector<bool> &t)
{
    uint32_t sz;
    unpack_buffer(unpacker, sz);
    t.reserve(sz);
    bool b;
    for (uint32_t i = 0; i < sz; i++) {
        unpack_buffer(unpacker, b);
        t.push_back(b);
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


// message class methods

enum message::msg_type
message :: message :: unpack_message_type()
{
    enum msg_type mtype;
    auto unpacker = buf->unpack_from(BUSYBEE_HEADER_SIZE);
    unpack_buffer(unpacker, mtype);
    return mtype;
}
