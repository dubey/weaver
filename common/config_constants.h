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

extern uint64_t NumVts;
extern uint64_t NumShards;
extern uint64_t NumBackups;
extern uint64_t NumEffectiveServers;
extern uint64_t NumActualServers;
extern uint64_t ShardIdIncr;

inline void
init_config_constants()
{
    NumEffectiveServers = NumVts + NumShards;
    NumActualServers = NumEffectiveServers * (1+NumBackups);
    ShardIdIncr = NumVts;
}

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
