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

using vc::vclock;

vclock :: vclock(uint64_t vtid, uint64_t clk_init)
    : vt_id(vtid)
    , clock(std::vector<uint64_t>(NUM_VTS, clk_init))
{
    assert(vt_id < NUM_VTS || vt_id == UINT64_MAX);
}

vclock :: vclock(uint64_t vtid, vclock_t &vclk)
    : vt_id(vtid)
    , clock(vclk)
{
    assert(vt_id < NUM_VTS || vt_id == UINT64_MAX);
}

void
vclock :: increment_counter(uint64_t index)
{
    assert(index < NUM_VTS);
    clock[index]++;
}

void
vclock :: update_clock(uint64_t vtid, uint64_t new_clock)
{
    assert(vtid < NUM_VTS);
    if (clock[vtid] < new_clock) {
        clock[vtid] = new_clock;
    }
}
