/*
 * ===============================================================
 *    Description:  DS to keep track of transaction replies from
 *                  shards.
 *
 *        Created:  2014-02-24 12:20:10
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_coordinator_current_tx_h_
#define weaver_coordinator_current_tx_h_

#include <vector>

#include "common/transaction.h"

namespace coordinator
{
    struct current_tx
    {
        std::vector<bool> shard_ack;
        transaction::pending_tx *tx;

        current_tx() : tx(nullptr) { }

        current_tx(transaction::pending_tx *t)
            : tx(t)
        { }
    };
}

#endif
