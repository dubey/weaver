/*
 * ===============================================================
 *    Description:  Vector of event counters, one for each vector
 *                  timestamper. Basically a wrapper class around
 *                  a vector of ints.
 *
 *        Created:  01/15/2013 06:23:23 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __VCLOCK__
#define __VCLOCK__

#include <vector>
#include <algorithm>
#include <assert.h>

#include "common/weaver_constants.h"

namespace vc
{
    typedef std::vector<uint64_t> vclock_t;
    typedef std::vector<uint64_t> qtimestamp_t;
    
    class vclock
    {
        public:
            uint64_t vt_id;
            vclock_t clock;

            vclock();
            vclock(uint64_t vt_id);
            vclock(uint64_t vt_id, vclock_t &vclk);
            vclock_t get_clock();
            void increment_clock();
            void increment_counter(uint64_t index);
            void update_clock(uint64_t vt_id, uint64_t new_clock);
    };

    inline
    vclock :: vclock()
        : vt_id(MAX_UINT64)
        , clock(NUM_VTS, MAX_UINT64)
    { }

    inline
    vclock :: vclock(uint64_t vtid)
        : vt_id(vtid)
        , clock(std::vector<uint64_t>(NUM_VTS, 0))
    { }

    inline
    vclock :: vclock(uint64_t vtid, vclock_t &vclk)
        : vt_id(vtid)
        , clock(vclk)
    { }

    inline vclock_t
    vclock :: get_clock()
    {
        return clock;
    }

    inline void
    vclock :: increment_clock()
    {
        clock.at(vt_id)++;
    }

    inline void
    vclock :: increment_counter(uint64_t index)
    {
        clock.at(index)++;
    }

    inline void
    vclock :: update_clock(uint64_t vtid, uint64_t new_clock)
    {
        assert(clock.at(vtid) <= new_clock);
        clock.at(vtid) = new_clock;
    }
}

#endif
