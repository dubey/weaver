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

#define PARSE_FILLER \
    yaml_parser_scan(&parser, &token); \
    assert(token.type == YAML_VALUE_TOKEN); \
    yaml_parser_scan(&parser, &token); \
    assert(token.type == YAML_SCALAR_TOKEN);

#define PARSE_INT(X) \
    X = atoi((const char*)token.data.scalar.value);

#define PARSE_IPADDR(X) \
    X = (char*)malloc(32); \
    strncpy(X, (const char*)token.data.scalar.value, 32);

    do {
        yaml_parser_scan(&parser, &token);
        switch (token.type) {

            case YAML_KEY_TOKEN:
                yaml_parser_scan(&parser, &token);
                assert(token.type == YAML_SCALAR_TOKEN);
                if (strncmp((const char*)token.data.scalar.value, "num_vts", 7) == 0) {
                    PARSE_FILLER;
                    PARSE_INT(NumVts);

                } else if (strncmp((const char*)token.data.scalar.value, "num_shards", 10) == 0) {
                    PARSE_FILLER;
                    PARSE_INT(NumShards);

                } else if (strncmp((const char*)token.data.scalar.value, "num_backups", 11) == 0) {
                    PARSE_FILLER;
                    PARSE_INT(NumBackups);

                } else if (strncmp((const char*)token.data.scalar.value, "max_cache_entries", 17) == 0) {
                    PARSE_FILLER;
                    PARSE_INT(MaxCacheEntries);

                } else if (strncmp((const char*)token.data.scalar.value, "hyperdex_coord_ipaddr", 21) == 0) {
                    PARSE_FILLER;
                    PARSE_IPADDR(HyperdexCoordIpaddr);

                } else if (strncmp((const char*)token.data.scalar.value, "hyperdex_coord_port", 19) == 0) {
                    PARSE_FILLER;
                    PARSE_INT(HyperdexCoordPort);

                } else if (strncmp((const char*)token.data.scalar.value, "kronos_ipaddr", 13) == 0) {
                    PARSE_FILLER;
                    PARSE_IPADDR(KronosIpaddr);

                } else if (strncmp((const char*)token.data.scalar.value, "kronos_port", 11) == 0) {
                    PARSE_FILLER;
                    PARSE_INT(KronosPort);

                } else if (strncmp((const char*)token.data.scalar.value, "server_manager_ipaddr", 21) == 0) {
                    PARSE_FILLER;
                    PARSE_IPADDR(ServerManagerIpaddr);

                } else if (strncmp((const char*)token.data.scalar.value, "server_manager_port", 19) == 0) {
                    PARSE_FILLER;
                    PARSE_INT(ServerManagerPort);

                } else {
                    WDEBUG << "unexpected token" << std::endl;
                    return;
                }
                break;

            default:
                break;
        }

        if (token.type != YAML_STREAM_END_TOKEN) {
            yaml_token_delete(&token);
        }
    } while (token.type != YAML_STREAM_END_TOKEN);
    yaml_token_delete(&token);

    yaml_parser_delete(&parser);
    fclose(config_file);

#undef PARSE_FILLER
#undef PARSE_INT
#undef PARSE_IPADDR

    assert(UINT64_MAX != NumVts);
    assert(UINT64_MAX != NumShards);
    assert(UINT64_MAX != NumBackups);
    assert(UINT16_MAX != MaxCacheEntries);
    assert(NULL != HyperdexCoordIpaddr);
    assert(UINT16_MAX != HyperdexCoordPort);
    assert(NULL != KronosIpaddr);
    assert(UINT16_MAX != KronosPort);
    assert(NULL != ServerManagerIpaddr);
    assert(UINT16_MAX != ServerManagerPort);

    NumEffectiveServers = NumVts + NumShards;
    NumActualServers = NumEffectiveServers * (1+NumBackups);
    ShardIdIncr = NumVts;
}
