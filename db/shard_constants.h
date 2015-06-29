/*
 * ===============================================================
 *    Description:  Constants for shards.
 *
 *        Created:  2014-06-19 13:01:18
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_shard_constants_h_
#define weaver_db_shard_constants_h_

//#define NUM_SHARD_THREADS (sysconf(_SC_NPROCESSORS_ONLN ))
#define NUM_SHARD_THREADS 32
//#define NUM_SHARD_THREADS 128

#define NUM_NODE_MAPS 64
#define SHARD_MSGRECV_TIMEOUT -1 // busybee recv timeout (ms) for shard worker threads

#define BATCH_MSG_SIZE 1 // 1 == no batching

// migration
//#define WEAVER_CLDG // defined if communication-based LDG, undef otherwise
//#define WEAVER_NEW_CLDG // defined if communication-based LDG, undef otherwise

#endif
