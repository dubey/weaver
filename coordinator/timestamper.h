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
#include "nmap_stub.h"

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
            vc::vclock vclk; // vector clock
            vc::qtimestamp_t qts; // queue timestamp
            std::unordered_map<uint64_t, pending_update> pending_updates;
            // node map client
            coordinator::nmap_stub;
            // big mutex
            po6::threads::mutex mutex;
            // migration
            // permanent deletion
            // daemon

        public:
            timestamper(uint64_t id);
            busybee_returncode send(uint64_t shard_id, std::auto_ptr<e::buffer> buf);
    };

    inline
    timestamper :: timestamper(uint64_t id)
        : vt_id(id)
        , port_ctr(0)
        , handle_gen(0)
        , vclk(id)
        , qts(NUM_SHARDS, 0)
    {
        // initialize array of server locations
        initialize_busybee(bb, vt_id, myloc, NUM_THREADS);
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
