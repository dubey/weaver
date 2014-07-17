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

#include <stdint.h>
#include <vector>

extern uint64_t NumVts;
extern uint64_t NumShards;
extern uint64_t NumBackups;
extern uint64_t NumEffectiveServers;
extern uint64_t NumActualServers;
extern uint64_t ShardIdIncr;

extern char *HyperdexCoordIpaddr;
extern uint16_t HyperdexCoordPort;
extern std::vector<std::pair<char*, uint16_t>> HyperdexDaemons;

extern char *KronosIpaddr;
extern uint16_t KronosPort;

extern char *ServerManagerIpaddr;
extern uint16_t ServerManagerPort;

void init_config_constants(const char *config_file_name);

#define NUM_THREADS (sysconf(_SC_NPROCESSORS_ONLN ))
#define ID_BITS 16 // max 2^16 timestampers
#define TOP_MASK (0x0000ffffffffffffULL)

#endif
