/*
 * ===============================================================
 *    Description:  Parse config for startup scripts.
 *
 *        Created:  2014-07-16 16:14:07
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#include <iostream>
#include <e/popt.h>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"

DECLARE_CONFIG_CONSTANTS;

int
main(int argc, const char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    //google::InstallFailureSignalHandler();
    google::LogToStderr();

    const char *config_name = "";
    const char *config_file = "/etc/weaver.yaml";
    // arg parsing borrowed from HyperDex
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('c', "config-param")
            .description("name of configuration parameter")
            .metavar("param").as_string(&config_name);
    ap.arg().name('f', "config-file")
            .description("full path of weaver.yaml configuration file (default /etc/weaver.yaml)")
            .metavar("file").as_string(&config_file);

    if (!ap.parse(argc, argv) || ap.args_sz() != 0) {
        WDEBUG << "args parsing failure" << std::endl;
        return -1;
    }

    // configuration file parse
    if (!init_config_constants(config_file)) {
        WDEBUG << "error in init_config_constants, exiting now." << std::endl;
        return -1;
    }

    std::string config(config_name);

    if (config == "num_vts") {
        std::cout << NumVts << std::endl;
    } else if (config == "max_cache_entries") {
        std::cout << MaxCacheEntries << std::endl;
    } else if (config == "hyperdex_coord_ipaddr") {
        std::cout << HyperdexCoordIpaddr << std::endl;
    } else if (config == "hyperdex_coord_port") {
        std::cout << HyperdexCoordPort << std::endl;
    } else if (config == "hyperdex_daemons_ipaddr") {
        for (auto &p: HyperdexDaemons) {
            std::cout << p.first << " ";
        }
        std::cout << std::endl;
    } else if (config == "hyperdex_daemons_port") {
        for (auto &p: HyperdexDaemons) {
            std::cout << p.second << " ";
        }
        std::cout << std::endl;
    } else if (config == "kronos_ipaddr") {
        for (auto &p: KronosLocs) {
            std::cout << p.first << " ";
        }
        std::cout << std::endl;
    } else if (config == "kronos_port") {
        for (auto &p: KronosLocs) {
            std::cout << p.second << " ";
        }
        std::cout << std::endl;
    } else if (config == "server_manager_ipaddr") {
        for (auto &p: ServerManagerLocs) {
            std::cout << p.first << " ";
        }
        std::cout << std::endl;
    } else if (config == "server_manager_port") {
        for (auto &p: ServerManagerLocs) {
            std::cout << p.second << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "bad config name" << std::endl;
    }

    return 0;
}


