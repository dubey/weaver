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

#ifndef weaver_common_vclock_h_
#define weaver_common_vclock_h_

#include <vector>
#include <algorithm>
#include <assert.h>

#include "common/utils.h"

namespace vc
{
    typedef std::vector<uint64_t> vclock_t;
    typedef std::vector<uint64_t> qtimestamp_t;
    
    class vclock
    {
        public:
            uint64_t vt_id;
            vclock_t clock;

            vclock() : vt_id(UINT64_MAX) { }
            vclock(uint64_t vt_id, uint64_t clk_init);
            vclock(uint64_t vt_id, vclock_t &vclk);
            void new_epoch(uint64_t epoch_num);
            uint64_t get_clock() const { return clock[vt_id+1]; }
            uint64_t get_epoch() const { return clock[0]; }
            void increment_clock() { clock[vt_id+1]++; }
            void update_clock(vc::vclock &other);

            bool operator==(const vclock &rhs) const;
            bool operator!=(const vclock &rhs) const;
    };
}

#endif
