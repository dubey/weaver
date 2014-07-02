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

#include "common/weaver_constants.h"
#include "common/message.h"

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
message :: size(const db::element::property &t)
{
    return size(t.key)
        + size(t.value)
        + size(t.creat_time)
        + size(t.del_time);
}

uint64_t
message :: size(const db::element::remote_node &t)
{
    return size(t.loc) + size(t.id);
}

uint64_t
message :: size(const std::shared_ptr<transaction::pending_update> &ptr_t)
{
    transaction::pending_update &t = *ptr_t;
    uint64_t sz = size(t.type)
         + size(t.qts)
         + size(t.handle)
         + size(t.handle1)
         + size(t.handle2)
         + size(t.id)
         + size(t.elem1)
         + size(t.elem2)
         + size(t.loc1)
         + size(t.loc2);
    if (t.type == transaction::NODE_SET_PROPERTY
     || t.type == transaction::EDGE_SET_PROPERTY) {
        sz += size(*t.key)
         + size(*t.value);
    }
    return sz;
}

uint64_t
message :: size(const transaction::pending_tx &t)
{
    return size(t.id)
         + size(t.client_id)
         + size(t.writes)
         + size(t.timestamp);
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
message :: pack_buffer(e::buffer::packer &packer, const enum transaction::update_type&t)
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
message :: pack_buffer(e::buffer::packer &packer, const std::string &t)
{
    assert(t.size() <= UINT32_MAX);
    uint32_t strlen = t.size();
    packer = packer << strlen;

    uint32_t words = strlen / 8;
    uint32_t leftover_chars = strlen % 8;

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
message :: pack_buffer(e::buffer::packer &packer, const db::element::property &t)
{
    pack_buffer(packer, t.key);
    pack_buffer(packer, t.value);
    pack_buffer(packer, t.creat_time);
    pack_buffer(packer, t.del_time);
}

void 
message :: pack_buffer(e::buffer::packer &packer, const db::element::remote_node &t)
{
    pack_buffer(packer, t.loc);
    pack_buffer(packer, t.id);
}

void
message :: pack_buffer(e::buffer::packer &packer, const std::shared_ptr<transaction::pending_update> &ptr_t)
{
    transaction::pending_update &t = *ptr_t;
    pack_buffer(packer, t.type);
    pack_buffer(packer, t.qts);
    pack_buffer(packer, t.handle);
    pack_buffer(packer, t.handle1);
    pack_buffer(packer, t.handle2);
    pack_buffer(packer, t.id);
    pack_buffer(packer, t.elem1);
    pack_buffer(packer, t.elem2);
    pack_buffer(packer, t.loc1);
    pack_buffer(packer, t.loc2);
    if (t.type == transaction::NODE_SET_PROPERTY
     || t.type == transaction::EDGE_SET_PROPERTY) {
        pack_buffer(packer, *t.key);
        pack_buffer(packer, *t.value);
    }
}

void
message :: pack_buffer(e::buffer::packer &packer, const transaction::pending_tx &t)
{
    pack_buffer(packer, t.id);
    pack_buffer(packer, t.client_id);
    pack_buffer(packer, t.writes);
    pack_buffer(packer, t.timestamp);
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
message :: unpack_buffer(e::unpacker &unpacker, std::string &t)
{
    uint32_t strlen;
    unpack_buffer(unpacker, strlen);
    t.resize(strlen);

    uint32_t words = strlen / 8;
    uint32_t leftover_chars = strlen % 8;

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
message :: unpack_buffer(e::unpacker &unpacker, db::element::property &t)
{
    unpack_buffer(unpacker, t.key);
    unpack_buffer(unpacker, t.value);
    t.creat_time.clock.clear();
    t.del_time.clock.clear();
    unpack_buffer(unpacker, t.creat_time);
    unpack_buffer(unpacker, t.del_time);
}

void 
message :: unpack_buffer(e::unpacker &unpacker, db::element::remote_node& t)
{
    unpack_buffer(unpacker, t.loc);
    unpack_buffer(unpacker, t.id);
}

void
message :: unpack_buffer(e::unpacker &unpacker, std::shared_ptr<transaction::pending_update> &ptr_t)
{
    ptr_t.reset(new transaction::pending_update());
    transaction::pending_update &t = *ptr_t;
    unpack_buffer(unpacker, t.type);
    unpack_buffer(unpacker, t.qts);
    unpack_buffer(unpacker, t.handle);
    unpack_buffer(unpacker, t.handle1);
    unpack_buffer(unpacker, t.handle2);
    unpack_buffer(unpacker, t.id);
    unpack_buffer(unpacker, t.elem1);
    unpack_buffer(unpacker, t.elem2);
    unpack_buffer(unpacker, t.loc1);
    unpack_buffer(unpacker, t.loc2);
    if (t.type == transaction::NODE_SET_PROPERTY
     || t.type == transaction::EDGE_SET_PROPERTY) {
        t.key.reset(new std::string());
        t.value.reset(new std::string());
        unpack_buffer(unpacker, *t.key);
        unpack_buffer(unpacker, *t.value);
    }
}

void
message :: unpack_buffer(e::unpacker &unpacker, transaction::pending_tx &t)
{
    unpack_buffer(unpacker, t.id);
    unpack_buffer(unpacker, t.client_id);
    unpack_buffer(unpacker, t.writes);
    unpack_buffer(unpacker, t.timestamp);
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

void
message :: message :: unpack_client_tx(transaction::pending_tx &tx)
{
    uint32_t num_tx;
    enum msg_type mtype;
    e::unpacker unpacker = buf->unpack_from(BUSYBEE_HEADER_SIZE);
    unpack_buffer(unpacker, mtype);
    assert(mtype == CLIENT_TX_INIT);
    unpack_buffer(unpacker, num_tx);

    while (num_tx-- > 0) {
        auto upd = *(tx.writes.emplace(tx.writes.end(), std::make_shared<transaction::pending_update>()));
        unpack_buffer(unpacker, mtype);
        switch (mtype) {
            case CLIENT_NODE_CREATE_REQ:
                upd->type = transaction::NODE_CREATE_REQ;
                unpack_buffer(unpacker, upd->id); 
                break;

            case CLIENT_EDGE_CREATE_REQ:
                upd->type = transaction::EDGE_CREATE_REQ;
                unpack_buffer(unpacker, upd->id);
                unpack_buffer(unpacker, upd->handle1);
                unpack_buffer(unpacker, upd->handle2);
                break;

            case CLIENT_NODE_DELETE_REQ:
                upd->type = transaction::NODE_DELETE_REQ;
                unpack_buffer(unpacker, upd->handle1);
                break;

            case CLIENT_EDGE_DELETE_REQ:
                upd->type = transaction::EDGE_DELETE_REQ;
                unpack_buffer(unpacker, upd->handle1);
                unpack_buffer(unpacker, upd->handle2);
                break;

            case CLIENT_NODE_SET_PROP:
                upd->type = transaction::NODE_SET_PROPERTY;
                upd->key.reset(new std::string());
                upd->value.reset(new std::string());
                unpack_buffer(unpacker, upd->handle1);
                unpack_buffer(unpacker, *upd->key);
                unpack_buffer(unpacker, *upd->value);
                break;

            case CLIENT_EDGE_SET_PROP:
                upd->type = transaction::EDGE_SET_PROPERTY;
                upd->key.reset(new std::string());
                upd->value.reset(new std::string());
                unpack_buffer(unpacker, upd->handle1);
                unpack_buffer(unpacker, upd->handle2);
                unpack_buffer(unpacker, *upd->key);
                unpack_buffer(unpacker, *upd->value);
                break;

            default:
                WDEBUG << "bad msg type: " << mtype << std::endl;
        }
    }
}
