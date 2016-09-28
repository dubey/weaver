/*
 * ===============================================================
 *    Description:  Vector clock that supports atomic operations.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2016, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_atomic_vclock_h_
#define weaver_common_atomic_vclock_h_

#include <memory>
#include <vector>
#include <atomic>

#include "common/vclock.h"

namespace vc
{
using atomic_vclock_t     = std::vector<std::atomic_uint64_t>;
using atomic_vclock_ptr_t = std::shared_ptr<atomic_vclock>;

class atomic_vclock
{
    private:
    uint64_t m_vt_id;
    atomic_vclock_t m_clock;

    public:
    atomic_vclock();
    atomic_vclock(uint64_t vt_id, uint64_t clk_init);

    uint64_t get_clock() const;
    uint64_t get_epoch() const;
    uint64_t get_vt_id() const;

    vclock copy_clock() const;

    void update_vt_id(uint64_t vt_id);
    void reset_clock(uint64_t new_epoch);
    void increment_epoch(uint64_t new_epoch);
    void increment_clock();
    void update_clock(uint64_t other_vt, uint64_t other_epoch, uint64_t new_clock);
};
}

#endif
