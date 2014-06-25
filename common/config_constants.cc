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

void
init_config_constants(const char *config_file_name)
{
    NumVts = UINT64_MAX;
    NumShards = UINT64_MAX;
    NumBackups = UINT64_MAX;

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
    do {
        yaml_parser_scan(&parser, &token);
        switch (token.type) {

            case YAML_KEY_TOKEN:
                yaml_parser_scan(&parser, &token);
                assert(token.type == YAML_SCALAR_TOKEN);
                if (strcmp((const char*)token.data.scalar.value, "num_vts") == 0) {
                    yaml_parser_scan(&parser, &token);
                    assert(token.type == YAML_VALUE_TOKEN);
                    yaml_parser_scan(&parser, &token);
                    assert(token.type == YAML_SCALAR_TOKEN);
                    NumVts = atoi((const char*)token.data.scalar.value);
                } else if (strcmp((const char*)token.data.scalar.value, "num_shards") == 0) {
                    yaml_parser_scan(&parser, &token);
                    assert(token.type == YAML_VALUE_TOKEN);
                    yaml_parser_scan(&parser, &token);
                    assert(token.type == YAML_SCALAR_TOKEN);
                    NumShards = atoi((const char*)token.data.scalar.value);
                } else if (strcmp((const char*)token.data.scalar.value, "num_backups") == 0) {
                    yaml_parser_scan(&parser, &token);
                    assert(token.type == YAML_VALUE_TOKEN);
                    yaml_parser_scan(&parser, &token);
                    assert(token.type == YAML_SCALAR_TOKEN);
                    NumBackups = atoi((const char*)token.data.scalar.value);
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

    assert(UINT64_MAX != NumVts);
    assert(UINT64_MAX != NumShards);
    assert(UINT64_MAX != NumBackups);
    WDEBUG << "num vts " << NumVts << ", NumShards " << NumShards << std::endl;

    NumEffectiveServers = NumVts + NumShards;
    NumActualServers = NumEffectiveServers * (1+NumBackups);
    ShardIdIncr = NumVts;
}
