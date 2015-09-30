/*
 * ===============================================================
 *    Description:  Implementation of vclock
 *
 *        Created:  2014-06-19 13:14:44
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/vclock.h"
#include "common/config_constants.h"
#include "common/passert.h"

using vc::vclock;

vclock :: vclock(uint64_t vtid, uint64_t clk_init)
    : vt_id(vtid)
    , clock(std::vector<uint64_t>(ClkSz, clk_init))
{
    PASSERT(vt_id < NumVts || vt_id == UINT64_MAX);
}

vclock :: vclock(uint64_t vtid, vclock_t &vclk)
    : vt_id(vtid)
    , clock(vclk)
{
    PASSERT(vt_id < NumVts || vt_id == UINT64_MAX);
}

void
vclock :: new_epoch(uint64_t new_epoch)
{
    PASSERT(clock[0] < new_epoch);
    clock = vclock_t(ClkSz, 0);
    clock[0] = new_epoch;
}

void
vclock :: update_clock(vc::vclock &other)
{
    uint64_t vtid = other.vt_id;
    PASSERT(vtid < NumVts);
    if (clock[0] == other.clock[0] && clock[vtid+1] < other.clock[vtid+1]) {
        clock[vtid+1] = other.clock[vtid+1];
    }
}

bool
vclock :: operator==(const vclock &other) const
{
    return vt_id == other.vt_id && clock == other.clock;
}

bool
vclock :: operator!=(const vclock &other) const
{
    return !(*this==other);
}
