/*
 * ===============================================================
 *    Description:  Parse config yaml file.
 *
 *        Created:  2014-06-25 16:34:00
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include <assert.h>
#include <yaml.h>
#include "common/weaver_constants.h"
#include "common/cache_constants.h"
#include "common/config_constants.h"

bool
init_config_constants(const char *config_file_name)
{
    std::string NumVts_str = "num_vts";
    NumVts = UINT64_MAX;
    ClkSz = UINT64_MAX;
    NumShards = UINT64_MAX;
    std::string MaxCacheEntries_str = "max_cache_entries";
    MaxCacheEntries = UINT16_MAX;
    std::string HyperdexCoord_str = "hyperdex_coord";
    HyperdexCoordIpaddr = std::string();
    HyperdexCoordPort = UINT16_MAX;
    std::string HyperdexDaemons_str = "hyperdex_daemons";
    std::string Kronos_str = "kronos";
    KronosIpaddr = std::string();
    KronosPort = UINT16_MAX;
    std::string WeaverCoord_str = "weaver_coord";
    ServerManagerIpaddr = std::string();
    ServerManagerPort = UINT16_MAX;
    std::string NoDynamicPartitioning_str = "no_dynamic_partitioning";
    NoDynamicPartitioning = true;
    std::string AuxIndex_str = "aux_index";
    AuxIndex = false;
    std::string BulkLoadPropertyValueDelimiter_str = "bulk_load_property_value_delimiter";
    BulkLoadPropertyValueDelimiter = (char)0;
    std::string BulkLoadNodeAliasKey_str = "bulk_load_node_alias_key";
    BulkLoadNodeAliasKey = "";
    std::string BulkLoadEdgeIndexKey_str = "bulk_load_edge_index_key";
    BulkLoadEdgeIndexKey = "";
    std::string BulkLoadEdgeHandlePrefix_str = "bulk_load_edge_handle_prefix";
    BulkLoadEdgeHandlePrefix = "e";
    std::string NodesPerMap_str = "nodes_per_map";
    NodesPerMap = 2;
    std::string MaxMemory_str = "max_memory";
    MaxMemory = 0.5;
    std::string RdNopPeriod_str = "rd_nop_period";
    RdNopPeriod     = 100000;
    std::string WrNopPeriod_str = "wr_nop_period";
    WrNopPeriod     = 100*RdNopPeriod;
    std::string ClkGossipPeriod_str = "clk_gossip_period";
    ClkGossipPeriod = 10000;

    FILE *config_file = nullptr;
    if (config_file_name != nullptr) {
        config_file = fopen(config_file_name, "r");
    }

    std::vector<const char*> other_paths = {"/etc/weaver.yaml", "/usr/local/etc/weaver.yaml", "/home/dubey/installs/etc/weaver.yaml"};

    for (const auto &s: other_paths) {
        if (config_file == nullptr) {
            config_file_name = s;
            config_file = fopen(s, "r");
        } else {
            break;
        }
    }

    if (config_file == nullptr) {
        WDEBUG << "Neither the provided config file name nor the defaults exist, exiting now." << std::endl;
        return false;
    } else {
        //std::cout << "HERE Using config file name " << config_file_name << std::endl;
    }

    yaml_parser_t parser;
    if (!yaml_parser_initialize(&parser)) {
        WDEBUG << "yaml error initialize" << std::endl;
        return false;
    }

    yaml_parser_set_input_file(&parser, config_file);

    yaml_token_t token;

#define PARSE_ASSERT_TYPE(t) \
    yaml_parser_scan(&parser, &token); \
    assert(token.type == t); \

#define PARSE_ASSERT_TYPE_DELETE(t) \
    PARSE_ASSERT_TYPE(t); \
    yaml_token_delete(&token);

#define PARSE_KEY_SCALAR \
    PARSE_ASSERT_TYPE_DELETE(YAML_KEY_TOKEN); \
    PARSE_ASSERT_TYPE(YAML_SCALAR_TOKEN);

#define PARSE_VALUE_SCALAR \
    PARSE_ASSERT_TYPE_DELETE(YAML_VALUE_TOKEN); \
    PARSE_ASSERT_TYPE(YAML_SCALAR_TOKEN);

#define PARSE_INT(X) \
    X = atoi((const char*)token.data.scalar.value); \
    yaml_token_delete(&token);

#define PARSE_FLOAT(X) \
    X = atof((const char*)token.data.scalar.value); \
    yaml_token_delete(&token);

#define PARSE_IPADDR(X) \
    strncpy(X, (const char*)token.data.scalar.value, 32); \
    yaml_token_delete(&token);

#define PARSE_BOOL(X) \
    X = strncmp((const char*)token.data.scalar.value, "true", 4) == 0 \
     || strncmp((const char*)token.data.scalar.value, "t", 1) == 0 \
     || strncmp((const char*)token.data.scalar.value, "True", 4) == 0 \
     || strncmp((const char*)token.data.scalar.value, "T", 1) == 0 \
     || strncmp((const char*)token.data.scalar.value, "Yes", 3) == 0 \
     || strncmp((const char*)token.data.scalar.value, "Y", 1) == 0 \
     || strncmp((const char*)token.data.scalar.value, "yes", 3) == 0 \
     || strncmp((const char*)token.data.scalar.value, "y", 1) == 0; \
    yaml_token_delete(&token);

#define PARSE_CHAR(X) \
    if (token.data.scalar.length == 1) { \
        X = *((char*)token.data.scalar.value); \
    } else { \
        WDEBUG << "unexpected token length " << token.data.scalar.length << " for char value"; \
    } \
    yaml_token_delete(&token);

#define PARSE_STRING(X) \
    if (token.data.scalar.length > 0) { \
        X = (const char*)token.data.scalar.value; \
    } \
    yaml_token_delete(&token);

#define PARSE_VALUE_IPADDR_PORT_BLOCK(v) \
    PARSE_ASSERT_TYPE_DELETE(YAML_VALUE_TOKEN); \
    PARSE_ASSERT_TYPE_DELETE(YAML_BLOCK_SEQUENCE_START_TOKEN); \
    while (true) { \
        yaml_parser_scan(&parser, &token); \
        if (token.type == YAML_BLOCK_END_TOKEN) { \
            yaml_token_delete(&token); \
            break; \
        } \
        assert(token.type == YAML_BLOCK_ENTRY_TOKEN); \
        yaml_token_delete(&token); \
        PARSE_ASSERT_TYPE_DELETE(YAML_BLOCK_MAPPING_START_TOKEN); \
        PARSE_ASSERT_TYPE_DELETE(YAML_KEY_TOKEN); \
        PARSE_ASSERT_TYPE(YAML_SCALAR_TOKEN); \
        char ipaddr[32]; \
        PARSE_IPADDR(ipaddr) \
        PARSE_VALUE_SCALAR \
        uint16_t port; \
        PARSE_INT(port) \
        v.emplace_back(std::make_pair(std::string(ipaddr), port)); \
        PARSE_ASSERT_TYPE_DELETE(YAML_BLOCK_END_TOKEN); \
    }

#define TOKEN_STRCMP_LEN(n) \
    (token.data.scalar.length < n ? token.data.scalar.length : n)

    PARSE_ASSERT_TYPE_DELETE(YAML_STREAM_START_TOKEN);
    PARSE_ASSERT_TYPE_DELETE(YAML_BLOCK_MAPPING_START_TOKEN);

    do {
        yaml_parser_scan(&parser, &token);

        switch (token.type) {

            case YAML_BLOCK_END_TOKEN:
                break;

            case YAML_KEY_TOKEN:
                PARSE_ASSERT_TYPE(YAML_SCALAR_TOKEN);
                if (strncmp((const char*)token.data.scalar.value, NumVts_str.c_str(), NumVts_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(NumVts);

                } else if (strncmp((const char*)token.data.scalar.value, MaxCacheEntries_str.c_str(), MaxCacheEntries_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(MaxCacheEntries);

                } else if (strncmp((const char*)token.data.scalar.value, HyperdexCoord_str.c_str(), HyperdexCoord_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_IPADDR_PORT_BLOCK(HyperdexCoord);
                    assert(!HyperdexCoord.empty());
                    HyperdexCoordIpaddr = HyperdexCoord[0].first;
                    HyperdexCoordPort = HyperdexCoord[0].second;

                } else if (strncmp((const char*)token.data.scalar.value, HyperdexDaemons_str.c_str(), HyperdexDaemons_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_IPADDR_PORT_BLOCK(HyperdexDaemons);

                } else if (strncmp((const char*)token.data.scalar.value, Kronos_str.c_str(), Kronos_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_IPADDR_PORT_BLOCK(KronosLocs);
                    assert(!KronosLocs.empty());
                    KronosIpaddr = KronosLocs[0].first;
                    KronosPort = KronosLocs[0].second;

                } else if (strncmp((const char*)token.data.scalar.value, WeaverCoord_str.c_str(), WeaverCoord_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_IPADDR_PORT_BLOCK(ServerManagerLocs);
                    assert(!ServerManagerLocs.empty());
                    ServerManagerIpaddr = ServerManagerLocs[0].first;
                    ServerManagerPort = ServerManagerLocs[0].second;

                } else if (strncmp((const char*)token.data.scalar.value, NoDynamicPartitioning_str.c_str(), NoDynamicPartitioning_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_BOOL(NoDynamicPartitioning);

                } else if (strncmp((const char*)token.data.scalar.value, AuxIndex_str.c_str(), AuxIndex_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_BOOL(AuxIndex);

                } else if (strncmp((const char*)token.data.scalar.value, BulkLoadPropertyValueDelimiter_str.c_str(), BulkLoadPropertyValueDelimiter_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_CHAR(BulkLoadPropertyValueDelimiter);

                } else if (strncmp((const char*)token.data.scalar.value, BulkLoadNodeAliasKey_str.c_str(), BulkLoadNodeAliasKey_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_STRING(BulkLoadNodeAliasKey);

                } else if (strncmp((const char*)token.data.scalar.value, BulkLoadEdgeIndexKey_str.c_str(), BulkLoadEdgeIndexKey_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_STRING(BulkLoadEdgeIndexKey);

                } else if (strncmp((const char*)token.data.scalar.value, BulkLoadEdgeHandlePrefix_str.c_str(), BulkLoadEdgeHandlePrefix_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_STRING(BulkLoadEdgeHandlePrefix);

                } else if (strncmp((const char*)token.data.scalar.value, NodesPerMap_str.c_str(), NodesPerMap_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(NodesPerMap);

                } else if (strncmp((const char*)token.data.scalar.value, MaxMemory_str.c_str(), MaxMemory_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_FLOAT(MaxMemory);

                } else if (strncmp((const char*)token.data.scalar.value, RdNopPeriod_str.c_str(), RdNopPeriod_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(RdNopPeriod);

                } else if (strncmp((const char*)token.data.scalar.value, WrNopPeriod_str.c_str(), WrNopPeriod_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(WrNopPeriod);

                } else if (strncmp((const char*)token.data.scalar.value, ClkGossipPeriod_str.c_str(), ClkGossipPeriod_str.length()) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(ClkGossipPeriod);

                } else {
                    WDEBUG << "unexpected key " << token.data.scalar.value << std::endl;
                }
                break;

            default:
                WDEBUG << "unexpected token type " << token.type << std::endl;
                yaml_token_delete(&token);
                break;
        }

    } while (token.type != YAML_BLOCK_END_TOKEN);
    yaml_token_delete(&token);
    PARSE_ASSERT_TYPE_DELETE(YAML_STREAM_END_TOKEN);

    yaml_parser_delete(&parser);
    fclose(config_file);

#undef PARSE_VALUE_SCALAR
#undef PARSE_INT
#undef PARSE_FLOAT
#undef PARSE_BOOL
#undef PARSE_CHAR
#undef PARSE_IPADDR
#undef PARSE_VALUE_IPADDR_PORT_BLOCK
#undef TOKEN_STRCMP_LEN

    if (UINT64_MAX == NumVts
     || UINT16_MAX == MaxCacheEntries
     || HyperdexCoordIpaddr.empty()
     || UINT16_MAX == HyperdexCoordPort
     || HyperdexDaemons.empty()
     || KronosIpaddr.empty()
     || UINT16_MAX == KronosPort
     || ServerManagerIpaddr.empty()
     || UINT16_MAX == ServerManagerPort) {
        return false;
    }

    ClkSz = NumVts+1; // one entry for each vt + an (configuration) epoch number
    NumShards = 0;
    MaxNumServers = 1000; // should be greater than NumActualServers = (NumEffectiveServers * (1+NumBackups))
    ShardIdIncr = NumVts;

    return true;
}

void
update_config_constants(uint64_t num_shards)
{
    NumShardsLock.wrlock();
    assert(num_shards >= NumShards);
    NumShards = num_shards;
    WDEBUG << "update #shards = " << NumShards << std::endl;
    NumShardsLock.unlock();
}

uint64_t
get_num_shards()
{
    NumShardsLock.rdlock();
    uint64_t num_shards = NumShards;
    NumShardsLock.unlock();

    return num_shards;
}
