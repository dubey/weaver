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
            vclock_t get_clock() { return clock; }
            void increment_clock() { clock[vt_id]++; }
            void increment_counter(uint64_t index);
            void update_clock(uint64_t vt_id, uint64_t new_clock);

            bool operator==(const vclock& rhs) {
                return vt_id == rhs.vt_id && clock == rhs.clock;
            }
    };
}

namespace std
{
    template <>
    struct hash<vc::vclock_t> 
    {
        public:
            size_t operator()(const vc::vclock_t &vc) const throw() 
            {
                size_t hash = std::hash<uint64_t>()(vc[0]);
                for (size_t i = 1; i < vc.size(); i++) {
                    hash ^= std::hash<uint64_t>()(vc[i]) + 0x9e3779b9 + (hash<<6) + (hash>>2);
                }
                return hash;
            }
    };
}

#endif
