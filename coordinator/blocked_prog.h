/*
 * ===============================================================
 *    Description:  DS for storing node prog state while shard is
 *                  restoring graph data.
 *
 *        Created:  2014-11-11 09:35:38
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_coordinator_blocked_prog_h_
#define weaver_coordinator_blocked_prog_h_

namespace coordinator
{
    struct blocked_prog
    {
        uint64_t client;
        std::unique_ptr<message::message> msg;

        blocked_prog(uint64_t c, std::unique_ptr<message::message> m)
            : client(c)
            , msg(std::move(m))
        { }
    };
}
#endif
