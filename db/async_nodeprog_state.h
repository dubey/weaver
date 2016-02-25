/*
 * ===============================================================
 *    Description:  Node prog state when an async get node call
 *                  delays execution of the node program
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_async_nodeprog_state_h_
#define weaver_db_async_nodeprog_state_h_

#include "node_prog/node_prog_type.h"
#include "common/vclock.h"
#include "db/node.h"

namespace db
{
    struct async_nodeprog_state
    {
        uint64_t type;
        vc::vclock clk;
        std::shared_ptr<void> state;
        db::node *n;
    };
}

#endif
