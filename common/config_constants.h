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

#include <string>
#include <stdint.h>
#include <vector>
#include <po6/threads/rwlock.h>

extern uint64_t NumVts;
extern uint64_t ClkSz;
extern uint64_t NumShards;
extern po6::threads::rwlock NumShardsLock;
extern uint64_t MaxNumServers;
extern uint64_t ShardIdIncr;

extern std::string HyperdexCoordIpaddr;
extern uint16_t HyperdexCoordPort;
extern std::vector<std::pair<std::string, uint16_t>> HyperdexCoord;
extern std::vector<std::pair<std::string, uint16_t>> HyperdexDaemons;

extern std::string KronosIpaddr;
extern uint16_t KronosPort;
extern std::vector<std::pair<std::string, uint16_t>> KronosLocs;

extern std::string ServerManagerIpaddr;
extern uint16_t ServerManagerPort;
extern std::vector<std::pair<std::string, uint16_t>> ServerManagerLocs;

extern bool AuxIndex;
extern char BulkLoadPropertyValueDelimiter;
extern std::string BulkLoadNodeAliasKey;
extern std::string BulkLoadEdgeIndexKey;
extern std::string BulkLoadEdgeHandlePrefix;

extern uint64_t NodesPerMap;
extern float MaxMemory;
extern uint64_t RdNopPeriod;
extern uint64_t WrNopPeriod;
extern uint64_t ClkGossipPeriod;

bool init_config_constants(const char *config_file_name=nullptr);
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
    std::string HyperdexCoordIpaddr; \
    uint16_t HyperdexCoordPort; \
    std::vector<std::pair<std::string, uint16_t>> HyperdexCoord; \
    std::vector<std::pair<std::string, uint16_t>> HyperdexDaemons; \
    std::string KronosIpaddr; \
    uint16_t KronosPort; \
    std::vector<std::pair<std::string, uint16_t>> KronosLocs; \
    std::string ServerManagerIpaddr; \
    uint16_t ServerManagerPort; \
    std::vector<std::pair<std::string, uint16_t>> ServerManagerLocs; \
    bool AuxIndex; \
    char BulkLoadPropertyValueDelimiter; \
    std::string BulkLoadNodeAliasKey; \
    std::string BulkLoadEdgeIndexKey; \
    std::string BulkLoadEdgeHandlePrefix; \
    uint64_t NodesPerMap; \
    float MaxMemory; \
    uint64_t RdNopPeriod; \
    uint64_t WrNopPeriod; \
    uint64_t ClkGossipPeriod; \
    uint16_t MaxCacheEntries;


#endif
