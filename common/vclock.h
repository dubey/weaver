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
    class vector
    {
        uint64_t rhandle_id;
        std::vector<std::vector<uint64_t>> clocks;

        public:
            vector(uint64_t rh_id);
            std::vector<uint64_t> get_clock(uint64_t rh_id);
            void increment_clock(uint64_t shard_id);
            void update_clock(uint64_t rh_id, std::vector<uint64_t> &new_clock);
            std::vector<std::vector<uint64_t>> get_entire_clock();
    };

    inline
    vector :: vector(uint64_t rh_id)
        : rhandle_id(rh_id)
        , clocks(std::vector(NUM_RHANDLERS, std::vector(NUM_SHARDS, 0)))
    {
    }

    std::vector<uint64_t>
    vector :: get_clock(uint64_t rh_id=rhandle_id)
    {
        return clocks(rh_id);
    }

    void
    vector :: increment_clock(uint64_t shard_id)
    {
        clocks.at(rhandle_id).at(shard_id)++;
    }

    void
    vector :: update_clock(uint64_t rh_id, std::vector<uint64_t> &new_clock)
    {
        for (uint64_t sid = 0; sid < NUM_SHARDS; sid++) {
            clocks.at(rh_id).at(sid) = new_clock.at(sid);
        }
    }

    std::vector<std::vector<uint64_t>>
    vector :: get_entire_clock()
    {
        return clock;
    }
}
#endif
