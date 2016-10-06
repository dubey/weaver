/*
 * ===============================================================
 *    Description:  Node hash and location related util functions
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2016, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_node_hash_utils_h_
#define weaver_common_node_hash_utils_h_

#include "common/types.h"
#include "common/config_constants.h"

inline uint64_t
hash_node_handle(const node_handle_t &nh)
{
    return weaver_util::murmur_hasher<std::string>()(nh);
}

inline uint64_t
hash_node_location(const node_handle_t &nh, uint64_t num_shards)
{
    return ((hash_node_handle(nh) % num_shards) + ShardIdIncr); 
}

#endif
