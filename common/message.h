/*
 * ================================================================
 *    Description:  Inter-server message packing and unpacking
 *
 *        Created:  11/07/2012 01:40:52 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_message_h_
#define weaver_common_message_h_

#include <busybee_constants.h>
#include <e/buffer.h>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/stl_serialization.h"
#include "common/weaver_serialization.h"
#include "common/enum_serialization.h"

namespace message
{
    enum msg_type
    {
        // client messages
        CLIENT_TX_INIT = 0,
        CLIENT_TX_SUCCESS,
        CLIENT_TX_ABORT,
        CLIENT_NODE_PROG_REQ,
        CLIENT_NODE_PROG_REPLY,
        CLIENT_REGISTER_NODE_PROG,
        START_MIGR,
        ONE_STREAM_MIGR,
        EXIT_WEAVER,
        // graph update messages
        TX_INIT,
        TX_DONE,
        // node program messages
        NODE_PROG,
        NODE_PROG_RETURN,
        NODE_PROG_RETRY,
        NODE_PROG_NOTFOUND,
        NODE_PROG_BADPROGTYPE,
        NODE_PROG_BENCHMARK,
        NODE_CONTEXT_FETCH,
        NODE_CONTEXT_REPLY,
        CACHE_UPDATE,
        CACHE_UPDATE_ACK,
        REGISTER_NODE_PROG,
        REGISTER_NODE_PROG_SUCCESSFUL,
        REGISTER_NODE_PROG_FAILED,
        // migration messages
        MIGRATE_SEND_NODE,
        MIGRATED_NBR_UPDATE,
        MIGRATED_NBR_ACK,
        MIGRATION_TOKEN,
        CLIENT_NODE_COUNT,
        NODE_COUNT_REPLY,
        // ft messages
        RESTORE_DONE,
        // initial graph loading
        LOADED_GRAPH,
        // coordinator group
        VT_CLOCK_UPDATE,
        VT_CLOCK_UPDATE_ACK,
        VT_NOP,
        VT_NOP_ACK,
        DONE_MIGR,

        ERROR
    };

    const char* to_string(const msg_type &t);

    class message
    {
        public:
            enum msg_type type;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
            std::auto_ptr<e::buffer> buf;
#pragma GCC diagnostic pop

            message()
                : type(ERROR)
                , buf(nullptr)
            { }
            message(enum msg_type t)
                : type(t)
                , buf(nullptr)
            { }
            message(message &copy)
                : type(copy.type)
            {
                buf.reset(copy.buf->copy());
            }

            void change_type(enum msg_type t) { type = t; }

            void prepare_message(const enum msg_type given_type);
            template <typename... Args> void prepare_message(const enum msg_type given_type, void *aux_args, const Args&... args);
            template <typename... Args> void unpack_partial_message(const enum msg_type expected_type, Args&... args);
            template <typename... Args> void unpack_message(const enum msg_type expected_type, void *aux_args, Args&... args);
            enum msg_type unpack_message_type();

        private:
            template <typename... Args> void unpack_message_internal(bool check_empty, const enum msg_type expected_type, void *aux_args, Args&... args);
    };

    uint64_t size(const enum msg_type &);
    void pack_buffer(e::packer &packer, const enum msg_type &t);    
    void unpack_buffer(e::unpacker &unpacker, enum msg_type &t);

    // base case for recursive size_wrapper()
    template <typename T>
    inline uint64_t size_wrapper(void *aux_args, const T &t)
    {
        return size(aux_args, t);
    }

    // recursive wrapper around variadic templated size()
    template <typename T, typename... Args>
    inline uint64_t size_wrapper(void *aux_args, const T &t, const Args&... args)
    {
        return size_wrapper(aux_args, t) + size_wrapper(aux_args, args...);
    }


    // base case for recursive pack_buffer_wrapper()
    template <typename T>
    inline void 
    pack_buffer_wrapper(e::packer &packer, void *aux_args, const T &t)
    {
        pack_buffer(packer, aux_args, t);
    }

    // recursive wrapper around variadic templated pack_buffer()
    // also performs buffer size sanity checks
    template <typename T, typename... Args>
    inline void 
    pack_buffer_wrapper(e::packer &packer, void *aux_args, const T &t, const Args&... args)
    {
        pack_buffer_wrapper(packer, aux_args, t);
        pack_buffer_wrapper(packer, aux_args, args...);
    }

    template <typename... Args>
    inline void
    message :: prepare_message(const enum msg_type given_type, void *aux_args, const Args&... args)
    {
        uint64_t bytes_to_pack = size_wrapper(aux_args, args...) + size(given_type);
        type = given_type;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::packer packer = buf->pack_at(BUSYBEE_HEADER_SIZE); 

        pack_buffer(packer, given_type);
        pack_buffer_wrapper(packer, aux_args, args...);
    }


    // base case for recursive unpack_buffer_wrapper()
    template <typename T>
    inline void
    unpack_buffer_wrapper(e::unpacker &unpacker, void *aux_args, T &t)
    {
        unpack_buffer(unpacker, aux_args, t);
    }

    // recursive weapper around variadic templated pack_buffer()
    template <typename T, typename... Args>
    inline void 
    unpack_buffer_wrapper(e::unpacker &unpacker, void *aux_args, T &t, Args&... args)
    {
        unpack_buffer_wrapper(unpacker, aux_args, t);
        unpack_buffer_wrapper(unpacker, aux_args, args...);
    }

    template <typename... Args>
    inline void
    message :: unpack_message_internal(bool check_empty,
                                       const enum msg_type expected_type,
                                       void *aux_args,
                                       Args&... args)
    {
        enum msg_type received_type;
        e::unpacker unpacker = buf->unpack_from(BUSYBEE_HEADER_SIZE);
        assert(!unpacker.error());

        unpack_buffer(unpacker, received_type);
#ifdef weaver_benchmark_
        if (received_type != expected_type) {
            WDEBUG << "recv type = " << to_string(received_type) << ", expected type " << to_string(expected_type) << std::endl;
        }
#endif
        assert(received_type == expected_type);
        UNUSED(expected_type);

        unpack_buffer_wrapper(unpacker, aux_args, args...);
        assert(!unpacker.error());
        if (check_empty) {
            assert(unpacker.remain() == 0); // assert whole message was unpacked
        }
    }

    template <typename... Args>
    inline void
    message :: unpack_partial_message(const enum msg_type expected_type, Args&... args)
    {
        unpack_message_internal(false, expected_type, nullptr, args...);
    }

    template <typename... Args>
    inline void
    message :: unpack_message(const enum msg_type expected_type, void *aux_args, Args&... args)
    {
        unpack_message_internal(true, expected_type, aux_args, args...);
    }

}

#endif
