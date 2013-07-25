/*
 * ===============================================================
 *    Description:  Nested vector clock for update timestamps of
 *                  shard servers, for each request handler.
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

#include "common/weaver_constants.h"

namespace vclock
{
    struct timestamp
    {
        uint64_t rh_id, shard_id, clock;

        timestamp() : rh_id(0), shard_id(0), clock(MAX_TIME) { }

        timestamp(uint64_t rid, uint64_t sid, uint64_t clk)
            : rh_id(rid)
            , shard_id(sid)
            , clock(clk)
        { }

        inline bool operator<=(timestamp const &ts) const
        {
            return ((rh_id == ts.rh_id) && (shard_id == ts.shard_id) && (clock <= ts.clock));
        }
    };
    
    typedef std::vector<std::vector<uint64_t>> nvc; // nested vector clock

    class vector
    {
        uint64_t rhandle_id;
        nvc clocks;

        public:
            vector(uint64_t rh_id);
            std::vector<uint64_t> get_clock();
            std::vector<uint64_t> get_clock(uint64_t rh_id);
            void increment_clock(uint64_t shard_id);
            void update_clock(uint64_t rh_id, std::vector<uint64_t> &new_clock);
            nvc get_entire_clock();
    };

    inline
    vector :: vector(uint64_t rh_id)
        : rhandle_id(rh_id)
        , clocks(std::vector<std::vector<uint64_t>>(NUM_RHANDLERS, std::vector<uint64_t>(NUM_SHARDS, 0)))
    {
    }
    
    inline std::vector<uint64_t>
    vector :: get_clock()
    {
        return get_clock(rhandle_id);
    }

    inline std::vector<uint64_t>
    vector :: get_clock(uint64_t rh_id)
    {
        return clocks.at(rh_id);
    }

    inline void
    vector :: increment_clock(uint64_t shard_id)
    {
        clocks.at(rhandle_id).at(shard_id)++;
    }

    inline void
    vector :: update_clock(uint64_t rh_id, std::vector<uint64_t> &new_clock)
    {
        for (uint64_t sid = 0; sid < NUM_SHARDS; sid++) {
            clocks.at(rh_id).at(sid) = new_clock.at(sid);
        }
    }

    inline nvc
    vector :: get_entire_clock()
    {
        return clocks;
    }
}
#endif
