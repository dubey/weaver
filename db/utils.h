/*
 * ===============================================================
 *    Description:  Commonly used db functionality
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_utils_h
#define weaver_db_utils_h

#include "common/types.h"
#include "common/node_hash_utils.h"
#include "db/shard_constants.h"

inline uint64_t
get_map_idx(const node_handle_t &nh)
{
    return (weaver_util::murmur_hasher<size_t>()(hash_node_handle(nh)) % NUM_NODE_MAPS);
}

#endif
