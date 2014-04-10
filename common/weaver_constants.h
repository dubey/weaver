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
#ifndef WDEBUG
#ifdef weaver_debug_
#define WDEBUG std::cerr << __FILE__ << ":" << __LINE__ << " "
#else
#define WDEBUG if (0) std::cerr << __FILE__ << ":" << __LINE__ << " "
#endif
#endif

#ifndef weaver_common_constants_h_
#define weaver_common_constants_h_

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
#define SHARDS_FILE "common/shards"
#define CLIENT_SHARDS_FILE "shards"

// fault tolerance
#define NUM_BACKUPS 0
//#define SERVER_MANAGER_IPADDR "127.0.0.1"
#define SERVER_MANAGER_IPADDR "128.84.167.101"
#define SERVER_MANAGER_PORT 2002

// weaver setup
#define NUM_SHARDS 2
#define NUM_VTS 1
#define NUM_SERVERS ((NUM_VTS + NUM_SHARDS) * (1 + NUM_BACKUPS))
#define SHARD_ID_INCR NUM_VTS
#define NUM_THREADS (sysconf(_SC_NPROCESSORS_ONLN ))
#define ID_BITS 8
#define TOP_MASK (0x0fffffffffffffffULL)
#define GRAPH_FILE "graph.rec"
#define NANO (1000000000ULL)
#define INITIAL_TIMEOUT_NANO 5000000000 // number of nanoseconds initial delay
#define SHARD_MSGRECV_TIMEOUT 1 // busybee recv timeout (ms) for shard worker threads
                                // worker threads should constantly check for and execute queued_requests
#define CLIENT_MSGRECV_TIMEOUT 1000 // busybee recv timeout (ms) for clients
                                    // assuming if request takes > 1 sec then vt is dead
// node programs
#define BATCH_MSG_SIZE 1 // 1 == no batching
#define MAX_CACHE_ENTRIES 0 // 0 to turn off caching

// migration
#define START_MIGR_ID SHARD_ID_INCR // first shard to get migration token
#define SHARD_CAP (100000ULL/NUM_SHARDS)
//#define WEAVER_CLDG // defined if communication-based LDG, false otherwise
//#define WEAVER_MSG_COUNT // defined if client msg count call will take place

// coordinator
#define VT_TIMEOUT_NANO 10000 // number of nanoseconds between successive nops

// hyperdex
//#define HYPERDEX_COORD_IPADDR "127.0.0.1"
//#define HYPERDEX_COORD_PORT 1982
#define HYPERDEX_COORD_IPADDR "128.84.167.101"
#define HYPERDEX_COORD_PORT 7982

// kronos
//#define KRONOS_IPADDR "127.0.0.1"
#define KRONOS_IPADDR "128.84.167.101"
#define KRONOS_PORT 1992

#endif
