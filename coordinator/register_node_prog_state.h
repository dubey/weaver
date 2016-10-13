/*
 * ===============================================================
 *    Description:  State saved for registering node prog
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_coordinator_register_node_prog_state_h_
#define weaver_coordinator_register_node_prog_state_h_

namespace coordinator
{
    struct register_node_prog_state
    {
        uint64_t client;
        std::unordered_set<uint64_t> servers;
        bool success;

        register_node_prog_state() : success(true) { }
    };
}
#endif
