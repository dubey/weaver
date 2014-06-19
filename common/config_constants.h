/*
 * ===============================================================
 *    Description:  Constants pertaining to deployment
 *                  configuration.
 *
 *        Created:  2014-06-19 12:57:54
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_config_constants_h_
#define weaver_common_config_constants_h_

#define NUM_BACKUPS 1
#define NUM_SHARDS 1
#define NUM_VTS 1
#define NUM_SERVERS ((NUM_VTS + NUM_SHARDS) * (1 + NUM_BACKUPS))
#define SHARD_ID_INCR NUM_VTS
#define NUM_THREADS (sysconf(_SC_NPROCESSORS_ONLN ))
#define ID_BITS 16 // max client id
#define TOP_MASK (0x0000ffffffffffffULL)

// hyperdex
#define HYPERDEX_COORD_IPADDR "127.0.0.1"
//#define HYPERDEX_COORD_IPADDR "128.84.167.101"
#define HYPERDEX_COORD_PORT 7982

// kronos
#define KRONOS_IPADDR "127.0.0.1"
//#define KRONOS_IPADDR "128.84.167.101"
#define KRONOS_PORT 1992

// server manager
#define SERVER_MANAGER_IPADDR "127.0.0.1"
//#define SERVER_MANAGER_IPADDR "128.84.167.101"
#define SERVER_MANAGER_PORT 2002

#endif
