/*
 * ===============================================================
 *    Description:  Constant values used across the project
 *
 *        Created:  01/15/2013 03:01:04 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

// debugging
#ifndef DEBUG
#ifdef __WEAVER_DEBUG__
#define DEBUG std::cerr << __FILE__ << ":" << __LINE__ << " "
#else
#define DEBUG if (0) std::cerr << __FILE__ << ":" << __LINE__ << " "
#endif
#endif

#ifndef __CONSTANTS__
#define __CONSTANTS__

// unused expression for no warnings
#define UNUSED(exp) do { (void)(exp); } while (0)

#include "stdint.h"

#define MAX_TIME UINT64_MAX
// messaging constants
#define ID_INCR (1ULL << 32ULL)
#define COORD_ID (0ULL)
#define CLIENT_ID (11ULL)
#define SHARDS_DESC_FILE "../common/shards"
// weaver setup
#define NUM_SHARDS 2
#define NUM_THREADS 8
#define GRAPH_FILE "graph.rec"
#define DAEMON_PERIOD 1 // frequency in seconds for coordinator daemon to run
#define MIGR_FREQ 3 // seconds delay between consecutive migrations
#define MSG_BATCHING true // whether to batch messages or not
#define BATCH_MSG_SIZE 100
#define MIGRATION true // whether to enable migration or not
#define MAX_CACHE_PER_NODE 10 // max num of cache entries per node per request type

#endif
