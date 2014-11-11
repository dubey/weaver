/*
 * ===============================================================
 *    Description:  Implementation of transaction:: methods
 *
 *        Created:  2014-09-18 15:28:18
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/transaction.h"

using transaction::pending_tx;

pending_tx :: pending_tx(tx_type t)
    : type(t)
    , vt_seq(42)
    , nop(nullptr)
{ }

pending_tx :: ~pending_tx()
{ }

std::shared_ptr<pending_tx>
pending_tx :: copy_fail_transaction()
{
    std::shared_ptr<pending_tx> fail_tx = std::make_shared<pending_tx>(FAIL);
    fail_tx->id = id;
    fail_tx->timestamp = timestamp;
    fail_tx->vt_seq = vt_seq;
    return fail_tx;
}
