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
{
    if (type == NOP) {
        delete nop;
    } else if (type == UPDATE) {
        for (pending_update *upd: writes) {
            delete upd;
        }
        writes.clear();
    }
}

pending_tx*
pending_tx :: copy_fail_transaction()
{
    pending_tx *fail_tx = new pending_tx(FAIL);
    fail_tx->id = id;
    fail_tx->timestamp = timestamp;
    fail_tx->vt_seq = vt_seq;
    return fail_tx;
}
