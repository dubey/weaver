/*
 * ===============================================================
 *    Description:  Coordinator vector timestamper class
 *                  definition. Vector timestampers receive client
 *                  requests, attach ordering related metadata,
 *                  and forward them to appropriate shards.
 *
 *        Created:  07/22/2013 12:23:33 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __COORD_VT__
#define __COORD_VT__

#include <vector>
#include <unordered_map>

#include "common/busybee_infra.h"
#include "common/vclock.h"
#include "common/message.h"
#include "transaction.h"

namespace coordinator
{
    class timestamper
    {
        public:
            uint64_t vt_id; // this vector timestamper's id
            // messaging
            std::shared_ptr<po6::net::location> myloc;
            busybee_mta *bb;
            // timestamper state
            uint64_t port_ctr, handle_gen;
            vc::vclock_t vclk; // vector clock
            vc::vclock_t qts; // queue timestamp
            std::unordered_map<uint64_t, pending_update> pending_updates;
            // big mutex
            po6::threads::mutex mutex;
            // migration
            // permanent deletion
            // daemon

        public:
            timestamper(uint64_t id);
            uint64_t generate_handle();
            busybee_returncode send(uint64_t shard_id, std::auto_ptr<e::buffer> buf);
    };

    inline
    timestamper :: timestamper(uint64_t id)
        : vt_id(id)
        , port_ctr(0)
        , handle_gen(0)
        , vclk(id)
    {
        // initialize array of server locations
        initialize_busybee(bb, vt_id, myloc, NUM_THREADS);
    }

    // to generate 64 bit graph element handles
    // assuming no more than 2^(VTID_BITS) request handlers
    // assuming no more than 2^(64-VTID_BITS) graph nodes and edges created at this handler
    // assuming caller holds mutex
    inline uint64_t
    timestamper :: generate_handle()
    {
        uint64_t new_handle = (++handle_gen) << VTID_BITS;
        new_handle |= vt_id;
        return new_handle;
    }

    inline busybee_returncode
    timestamper :: send(uint64_t shard_id, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        if ((ret = bb->send(shard_id, buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "message sending error: " << ret << std::endl;
        }
        return ret;
    }
 }

#endif
