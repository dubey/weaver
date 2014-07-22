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
#include "common/config_constants.h"
#include "common/cache_constants.h"

void
init_config_constants(const char *config_file_name)
{
    NumVts = UINT64_MAX;
    NumShards = UINT64_MAX;
    NumBackups = UINT64_MAX;
    MaxCacheEntries = UINT16_MAX;
    HyperdexCoordIpaddr = NULL;
    HyperdexCoordPort = UINT16_MAX;
    KronosIpaddr = NULL;
    KronosPort = UINT16_MAX;
    ServerManagerIpaddr = NULL;
    ServerManagerPort = UINT16_MAX;

    FILE *config_file = fopen(config_file_name, "r");
    yaml_parser_t parser;

    if (!yaml_parser_initialize(&parser)) {
        WDEBUG << "yaml error initialize" << std::endl;
        return;
    }
    if (config_file == NULL) {
        WDEBUG << "yaml error file open" << std::endl;
        return;
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

#define PARSE_IPADDR(X) \
    X = (char*)malloc(32); \
    strncpy(X, (const char*)token.data.scalar.value, 32); \
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
        char *ipaddr; \
        PARSE_IPADDR(ipaddr) \
        PARSE_VALUE_SCALAR \
        uint16_t port; \
        PARSE_INT(port) \
        v.emplace_back(std::make_pair(ipaddr, port)); \
        PARSE_ASSERT_TYPE_DELETE(YAML_BLOCK_END_TOKEN); \
    }

    PARSE_ASSERT_TYPE_DELETE(YAML_STREAM_START_TOKEN);
    PARSE_ASSERT_TYPE_DELETE(YAML_BLOCK_MAPPING_START_TOKEN);

    do {
        yaml_parser_scan(&parser, &token);

        switch (token.type) {

            case YAML_BLOCK_END_TOKEN:
                break;

            case YAML_KEY_TOKEN:
                PARSE_ASSERT_TYPE(YAML_SCALAR_TOKEN);
                if (strncmp((const char*)token.data.scalar.value, "num_vts", 7) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(NumVts);

                //} else if (strncmp((const char*)token.data.scalar.value, "num_shards", 10) == 0) {
                //    yaml_token_delete(&token);
                //    PARSE_VALUE_SCALAR;
                //    PARSE_INT(NumShards);

                } else if (strncmp((const char*)token.data.scalar.value, "num_backups", 11) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(NumBackups);

                } else if (strncmp((const char*)token.data.scalar.value, "max_cache_entries", 17) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(MaxCacheEntries);

                } else if (strncmp((const char*)token.data.scalar.value, "hyperdex_coord_ipaddr", 21) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_IPADDR(HyperdexCoordIpaddr);

                } else if (strncmp((const char*)token.data.scalar.value, "hyperdex_coord_port", 19) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_SCALAR;
                    PARSE_INT(HyperdexCoordPort);

                } else if (strncmp((const char*)token.data.scalar.value, "hyperdex_daemons", 16) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_IPADDR_PORT_BLOCK(HyperdexDaemons);

                } else if (strncmp((const char*)token.data.scalar.value, "kronos", 6) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_IPADDR_PORT_BLOCK(KronosLocs);
                    assert(!KronosLocs.empty());
                    KronosIpaddr = KronosLocs[0].first;
                    KronosPort = KronosLocs[0].second;

                } else if (strncmp((const char*)token.data.scalar.value, "server_manager", 14) == 0) {
                    yaml_token_delete(&token);
                    PARSE_VALUE_IPADDR_PORT_BLOCK(ServerManagerLocs);
                    assert(!ServerManagerLocs.empty());
                    ServerManagerIpaddr = ServerManagerLocs[0].first;
                    ServerManagerPort = ServerManagerLocs[0].second;

                } else {
                    WDEBUG << "unexpected key " << token.data.scalar.value << std::endl;
                    return;
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
#undef PARSE_IPADDR
#undef PARSE_VALUE_IPADDR_PORT_BLOCK

    assert(UINT64_MAX != NumVts);
    assert(UINT64_MAX != NumBackups);
    assert(UINT16_MAX != MaxCacheEntries);
    assert(NULL != HyperdexCoordIpaddr);
    assert(UINT16_MAX != HyperdexCoordPort);
    assert(!HyperdexDaemons.empty());
    assert(NULL != KronosIpaddr);
    assert(UINT16_MAX != KronosPort);
    assert(NULL != ServerManagerIpaddr);
    assert(UINT16_MAX != ServerManagerPort);

    NumShards = 0;
    NumEffectiveServers = NumVts + NumShards;
    NumActualServers = NumEffectiveServers * (1+NumBackups);
    ShardIdIncr = NumVts;
}

void
update_config_constants(uint64_t num_shards)
{
    NumShardsLock.wrlock();
    assert(num_shards >= NumShards);
    NumShards = num_shards;
    WDEBUG << "update #shards = " << NumShards << std::endl;
    NumEffectiveServers = NumVts + NumShards;
    NumActualServers = NumEffectiveServers * (1+NumBackups);
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

uint64_t
get_num_effective_servers()
{
    NumShardsLock.rdlock();
    uint64_t num_effective_servers = NumEffectiveServers;
    NumShardsLock.unlock();

    return num_effective_servers;
}

uint64_t
get_num_actual_servers()
{
    NumShardsLock.rdlock();
    uint64_t num_actual_servers = NumActualServers;
    NumShardsLock.unlock();

    return num_actual_servers;
}
