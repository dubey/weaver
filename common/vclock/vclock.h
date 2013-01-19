/*
 * ===============================================================
 *    Description:  Vector clock for update timestamps of shard
 *                  servers
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

#include "../weaver_constants.h"

namespace vclock
{
    class vector
    {
        public:
            vector();

        public:
            std::shared_ptr<std::vector<uint64_t>> clocks;
    };

    inline
    vector :: vector()
        : clocks(new std::vector<uint64_t>(NUM_SHARDS, 0))
    {
    }
}
#endif
