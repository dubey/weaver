/*
 * ===============================================================
 *    Description:  Atomic vector clock implementation
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2016, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/atomic_vclock.h"
#include "common/config_constants.h"

using vc::vclock;
using vc::atomic_vclock;

atomic_vclock :: vclock()
    : m_vt_id(UINT64_MAX)
{ }

atomic_vclock :: vclock(uint64_t vt_id, uint64_t clk_init)
    : m_vt_id(vt_id)
{
    assert(vt_id < NumVts || vt_id == UINT64_MAX);
    for (uint64_t i = 0; i < ClkSz; i++) {
        m_clock.emplace_back(clk_init);
    }
}

uint64_t
atomic_vclock :: get_clock() const
{
    return m_clock[m_vt_id+1].load();
}

uint64_t
atomic_vclock :: get_epoch() const
{
    return m_clock[0].load();
}

uint64_t
atomic_vclock :: get_vt_id() const
{
    return m_vt_id;
}

// copy out clock into non-atomic vclock
// not atomic
vclock
atomic_vclock :: copy_clock() const
{
    vclock copy_clk;
    copy_clk.vt_id = get_vt_id();
    for (uint64_t i = 0; i < ClkSz; i++) {
        uint64_t c = m_clock[i].load();
        copy_clk.clock.emplace_back(c);
    }

    return copy_clk;
}

void
atomic_vclock :: update_vt_id(uint64_t vt_id)
{
    m_vt_id = vt_id;
}

// set clock to new epoch
// set all other entries to 0
void
atomic_clock :: reset_clock(uint64_t new_epoch)
{
    m_clock[0].store(new_epoch);
    for (uint64_t i = 1; i < ClkSz; i++) {
        m_clock[i].store(0);
    }
}

// check if we are at expected epoch
// if yes: CAS into expected_epoch+1, set rest of the clock, return true
// if no : return false indicating failure
bool
atomic_vclock :: increment_epoch(uint64_t new_epoch)
{
    uint64_t expected_epoch = new_epoch-1;
    bool cmpxchg_success = m_clock[0].compare_exchange_strong(expected_epoch, new_epoch);
    if (cmpxchg_success) {
        for (uint64_t i = 1; i < ClkSz; i++) {
            m_clock[i].store(0);
        }
    }
    return cmpxchg_success;
}

// atomically increment this process's clock
void
atomic_vclock :: increment_clock()
{
    m_clock[vt_id+1].fetch_add(1);
}

// atomically load current clock value for other_vt
// calculate difference between our current value and new clock
// if difference is positive AND epochs match, CAS in the new clock value
void
atomic_vclock :: update_clock(uint64_t other_vt, uint64_t other_epoch, uint64_t new_clock)
{
    uint64_t old_clock = m_clock[other_vt+1].load();
    uint64_t diff      = new_clock - old_clock;
    uint64_t my_epoch  = m_clock[0].load();

    if (my_epoch == other_epoch && diff > 0) {
        m_clock[other_vt+1].compare_exchange_strong(old_clock, new_clock);
    }
}
