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
        case NODE_PROG_BADPROGTYPE:
            return "NODE_PROG_BADPROGTYPE";
        case NODE_PROG_BENCHMARK:
            return "NODE_PROG_BENCHMARK";
        case NODE_CONTEXT_FETCH:
            return "NODE_CONTEXT_FETCH";
        case NODE_CONTEXT_REPLY:
            return "NODE_CONTEXT_REPLY";
        case CACHE_UPDATE:
            return "CACHE_UPDATE";
        case CACHE_UPDATE_ACK:
            return "CACHE_UPDATE_ACK";
        case REGISTER_NODE_PROG:
            return "REGISTER_NODE_PROG";
        case REGISTER_NODE_PROG_SUCCESSFUL:
            return "REGISTER_NODE_PROG_SUCCESSFUL";
        case REGISTER_NODE_PROG_FAILED:
            return "REGISTER_NODE_PROG_FAILED";
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

uint64_t
message :: size(const enum msg_type &)
{
    return sizeof(uint8_t);
}

void
message :: pack_buffer(e::packer &packer, const enum msg_type &t)
{
    assert(t <= UINT8_MAX);
    uint8_t temp = (uint8_t) t;
    packer = packer << temp;
}

void
message :: unpack_buffer(e::unpacker &unpacker, enum msg_type &t)
{
    uint8_t _type;
    unpacker = unpacker >> _type;
    t = (enum msg_type)_type;
}


// message class methods

// prepare message with only message_type and no additional payload
void
message :: message :: prepare_message(const enum msg_type given_type)
{
    uint64_t bytes_to_pack = size(given_type);
    type = given_type;
    buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
    e::packer packer = buf->pack_at(BUSYBEE_HEADER_SIZE); 

    pack_buffer(packer, given_type);
}

enum message::msg_type
message :: message :: unpack_message_type()
{
    enum msg_type mtype;
    auto unpacker = buf->unpack_from(BUSYBEE_HEADER_SIZE);
    unpack_buffer(unpacker, mtype);
    return mtype;
}
