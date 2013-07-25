/*
 * ===============================================================
 *    Description:  Coordinator request handler class definition.
 *                  Request handlers receive client requests and
 *                  foward them to the appropriate shard.
 *
 *        Created:  07/22/2013 12:23:33 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __COORD_REQHANDLE__
#define __COORD_REQHANDLE__

#include <vector>
#include <unordered_map>

#include "common/busybee_infra.h"
#include "common/vclock.h"
#include "common/message.h"

namespace coordinator
{
    // store state for update received from client but not yet completed
    struct pending_update
    {
        message::msg_type type;
        uint64_t elem1, elem2, sender;
        uint32_t key;
        uint64_t value;
    };

    class rhandler
    {
        public:
            uint64_t rh_id; // this request handler's id
            // messaging
            std::shared_ptr<po6::net::location> myloc;
            busybee_mta *bb;
            // req handler state
            uint64_t port_ctr, handle_gen;
            vclock::vector vc;
            std::unordered_map<uint64_t, pending_update> pending_updates;
            // big mutex
            po6::threads::mutex mutex;
            // caching
            // permanent deletion
            // daemon

        public:
            rhandler(uint64_t id);
            uint64_t generate_handle();
            busybee_returncode send(uint64_t shard_id, std::auto_ptr<e::buffer> buf);
    };

    inline
    rhandler :: rhandler(uint64_t id)
        : rh_id(id)
        , port_ctr(0)
        , handle_gen(0)
        , vc(id)
    {
        // initialize array of server locations
        initialize_busybee(bb, rh_id, myloc, NUM_THREADS);
    }

    // to generate 64 bit graph element handles
    // assuming no more than 2^(RHID_BITS) request handlers
    // assuming no more than 2^(64-RHID_BITS) graph nodes and edges created at this handler
    // assuming caller holds mutex
    inline uint64_t
    rhandler :: generate_handle()
    {
        uint64_t new_handle = (++handle_gen) << RHID_BITS;
        new_handle |= rh_id;
        return new_handle;
    }

    inline busybee_returncode
    rhandler :: send(uint64_t shard_id, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        if ((ret = bb->send(shard_id, buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "message sending error: " << ret << std::endl;
        }
        return ret;
    }
 }

#endif
