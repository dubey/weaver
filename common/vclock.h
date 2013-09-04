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
        uint64_t vt_id;
        vclock_t clock;

        public:
            vclock(uint64_t vt_id);
            vclock_t get_clock();
            void increment_clock();
            void increment_counter(uint64_t index);
            void update_clock(uint64_t vt_id, uint64_t new_clock);
    };

    inline
    vclock :: vclock(uint64_t vtid)
        : vt_id(vtid)
        , clock(std::vector<uint64_t>(NUM_VTS, 0))
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

    // return the smaller of the two clocks
    // return -1 if clocks cannot be compared
    // return 2 if clocks are identical
    static inline int
    compare_two_clocks(const vclock_t &clk1, const vclock_t &clk2)
    {
        int ret = 2;
        for (uint64_t i = 0; i < NUM_VTS; i++) {
            if ((clk1.at(i) < clk2.at(i)) && (ret != 0)) {
                if (ret == 2) {
                    ret = 0;
                } else {
                    ret = -1;
                    break;
                }
            } else if ((clk1.at(i) > clk2.at(i)) && (ret != 1)) {
                if (ret == 2) {
                    ret = 1;
                } else {
                    ret = -1;
                    break;
                }
            }
        }
        return ret;
    }

    // static method which only compares vector clocks
    // returns bool vector, with i'th entry as true if that clock is definitely larger than some other clock
    static inline std::vector<bool>
    compare_vector_clocks(const std::vector<vclock_t> &clocks)
    {
        std::vector<bool> large(NUM_VTS, false); // keep track of clocks which are definitely not smallest
        uint64_t num_large = 0; // number of true values in previous vector
        for (uint64_t i = 0; i < (NUM_VTS-1); i++) {
            for (uint64_t j = (i+1); j < NUM_VTS; j++) {
                int cmp = compare_two_clocks(clocks.at(i), clocks.at(j));
                if ((cmp == 0) && !large.at(j)) {
                    large.at(j) = true;
                    num_large++;
                } else if ((cmp == 1) && !large.at(i)) {
                    large.at(i) = true;
                    num_large++;
                }
            }
            if (num_large == (NUM_VTS-1)) {
                return large;
            }
        }
        assert(num_large < (NUM_VTS-1));
        return large;
    }

    // static vector clock comparison method
    // will call Kronos daemon if clocks are incomparable
    // returns index of earliest clock
    static inline int64_t
    compare_vts(const std::vector<vclock_t> &clocks)
    {
        uint64_t min_pos;
        std::vector<bool> large = compare_vector_clocks(clocks);
        uint64_t num_large = std::count(large.begin(), large.end(), true);
        if (num_large == (NUM_VTS-1)) {
            for (min_pos = 0; min_pos < NUM_VTS; min_pos++) {
                if (!large.at(min_pos)) {
                    break;
                }
            }
            assert(min_pos < NUM_VTS);
            return min_pos;
        } else {
            // need to call Kronos
            // TODO
            return 0;
        }
    }

    // compare two vector clocks
    // return 0 if first is smaller, 1 if second is smaller, 2 if identical
    static inline int64_t
    compare_two_vts(const vclock_t &clk1, const vclock_t &clk2)
    {
        int cmp = compare_two_clocks(clk1, clk2);
        if (cmp == -1) {
            std::vector<vclock_t> compare_vclks;
            compare_vclks.push_back(clk1);
            compare_vclks.push_back(clk2);
            cmp = compare_vts(compare_vclks);
        }
        return cmp;
    }
}

#endif
