/*
 * ===============================================================
 *    Description:  Run test suite.
 *
 *        Created:  01/21/2013 11:45:25 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/debug.h"
#define __WEAVER_DEBUG__
#include "common/weaver_constants.h"

#include "message_test.h"
#include "cache_test.h"
#include "basic_client.h"
#include "repetitive_reach_program.h"
#include "multiple_reach_program.h"
#include "multiple_locality.h"
#include "basic_migration_test.h"
#include "line_reach_program.h"
#include "clique_reach_program.h"
#include "unreachable_reach_program.h"
//#include "dijkstra_prog_test.h"
#include "dijkstra_tree_test.h"
#include "multiple_widest_path.h"
//#include "clustering_prog_test.h"

int
main(int argc, char *argv[])
{
    UNUSED(argc);
    UNUSED(argv);

    DEBUG << "Starting tests." << std::endl;
#ifdef __ALL_TESTS__
    message_test();
    DEBUG << "Message packing/unpacking ok." << std::endl;
    cache_test();
    DEBUG << "Shard cache ok." << std::endl;
    basic_client_test();
    DEBUG << "Basic client ok." << std::endl;
    repetitive_reach_prog(false);
    DEBUG << "Repetitive reach program ok." << std::endl;
    multiple_sparse_reachability(false);
    multiple_dense_reachability(false);
    DEBUG << "Multiple reach program ok." << std::endl;
    basic_migration_test(false);
    DEBUG << "Basic migration ok." << std::endl;
    line_reach_prog(false);
    DEBUG << "Line reach program ok." << std::endl;
    clique_reach_prog(false);
    DEBUG << "Clique reach program ok." << std::endl;
    //multiple_wp_prog(false);
    DEBUG << "Widest path program ok." << std::endl;
    //dijkstra_prog_test();
    dijkstra_tree_test(true);
    DEBUG << "Shortest path tree test ok." << std::endl;
    //clustering_prog_test();
#endif
#ifndef __ALL_TESTS__
    //multiple_sparse_reachability(true);
    //unreachable_reach_prog(true);
    //clique_reach_prog(true);
    //multiple_wp_prog(true);
    multiple_sparse_locality(true);
#endif
    DEBUG << "All tests completed." << std::endl;

    return 0;
}
