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
#include <po6/threads/rwlock.h>

extern uint64_t NumVts;
extern uint64_t ClkSz;
extern uint64_t NumShards;
extern po6::threads::rwlock NumShardsLock;
extern uint64_t MaxNumServers;
extern uint64_t ShardIdIncr;

extern char *HyperdexCoordIpaddr;
extern uint16_t HyperdexCoordPort;
extern std::vector<std::pair<char*, uint16_t>> HyperdexCoord;
extern std::vector<std::pair<char*, uint16_t>> HyperdexDaemons;

extern char *KronosIpaddr;
extern uint16_t KronosPort;
extern std::vector<std::pair<char*, uint16_t>> KronosLocs;

extern char *ServerManagerIpaddr;
extern uint16_t ServerManagerPort;
extern std::vector<std::pair<char*, uint16_t>> ServerManagerLocs;

extern bool AuxIndex;
extern char BulkLoadPropertyValueDelimiter;
extern std::string BulkLoadNodeAliasKey;

bool init_config_constants(const char *config_file_name=NULL);
void update_config_constants(uint64_t num_shards);
uint64_t get_num_shards();

#define ID_BITS 16 // max 2^16 timestampers
#define TOP_MASK (0x0000ffffffffffffULL)

#define DECLARE_CONFIG_CONSTANTS \
    uint64_t NumVts; \
    uint64_t ClkSz; \
    uint64_t NumShards; \
    po6::threads::rwlock NumShardsLock; \
    uint64_t MaxNumServers; \
    uint64_t ShardIdIncr; \
    char *HyperdexCoordIpaddr; \
    uint16_t HyperdexCoordPort; \
    std::vector<std::pair<char*, uint16_t>> HyperdexCoord; \
    std::vector<std::pair<char*, uint16_t>> HyperdexDaemons; \
    char *KronosIpaddr; \
    uint16_t KronosPort; \
    std::vector<std::pair<char*, uint16_t>> KronosLocs; \
    char *ServerManagerIpaddr; \
    uint16_t ServerManagerPort; \
    std::vector<std::pair<char*, uint16_t>> ServerManagerLocs; \
    bool AuxIndex; \
    char BulkLoadPropertyValueDelimiter; \
    std::string BulkLoadNodeAliasKey; \
    uint16_t MaxCacheEntries;


#endif
