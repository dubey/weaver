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

#include <stdint.h>

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
#define MAX_UINT64 UINT64_MAX
// messaging constants
#define ID_INCR (1ULL << 32ULL)
#define COORD_ID (0ULL)
#define COORD_SM_ID (0ULL)
#define CLIENT_ID (100ULL)
#define SHARDS_DESC_FILE "../common/shards"
// weaver setup
#define NUM_SHARDS 2
#define NUM_VTS 1
#define SHARD_ID_INCR NUM_VTS
#define NUM_THREADS 8
#define ID_BITS 8
#define TOP_MASK (0x0fffffffffffffffULL)
#define GRAPH_FILE "graph.rec"
#define DAEMON_PERIOD 2 // frequency in seconds for coordinator daemon to run
#define MIGR_FREQ 10 // seconds delay between consecutive migrations
#define INITIAL_MIGR_DELAY 15 // seconds delay for initial migration
#define START_MIGR_ID SHARD_ID_INCR
#define MAX_NODES 500
#define MSG_BATCHING true // whether to batch messages or not
#define BATCH_MSG_SIZE 100
#define MIGRATION false // whether to enable migration at the beginning of the program or not
#define MAX_CACHE_PER_NODE 10 // max num of cache entries per node per request type
#define VT_NOP_TIMEOUT 1 // number of milliseconds between successive nops
#define VT_INITIAL_CLKUPDATE_DELAY 2000 // number of millis delay to ensure all timestampers are running
// hyperdex constants
#define HYPERDEX_COORD_IPADDR "127.0.0.1"
//#define HYPERDEX_COORD_IPADDR "128.84.227.115"
#define HYPERDEX_COORD_PORT 1982
// kronos constants
#define KRONOS_IPADDR "127.0.0.1"
//#define KRONOS_IPADDR "128.84.227.114"
#define KRONOS_PORT 1992

#endif
