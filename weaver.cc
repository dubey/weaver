/*
 * ===============================================================
 *    Description:  Weaver wrapper program that calls all other
 *                  Weaver programs.
 *
 *        Created:  2014-11-25 08:55:19
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <vector>
#include <e/subcommand.h>

int
main(int argc, const char* argv[])
{
    std::vector<e::subcommand> cmds;
    cmds.push_back(e::subcommand("timestamper",           "Start a new Weaver timestamper"));
    cmds.push_back(e::subcommand("shard",                 "Start a new Weaver shard"));
    cmds.push_back(e::subcommand("parse-config",          "Parse the Weaver configuration file"));

    return dispatch_to_subcommands(argc, argv,
                                   "weaver", "Weaver",
                                   PACKAGE_VERSION,
                                   "weaver-",
                                   "WEAVER_EXEC_PATH", WEAVER_EXEC_DIR,
                                   &cmds.front(), cmds.size());
}
