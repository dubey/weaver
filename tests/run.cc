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

#define __ALL_TESTS__
#ifdef __ALL_TESTS__
#include "message_test.h"
#include "cache_test.h"
#include "basic_client.h"
#include "repetitive_reach_program.h"
#include "multiple_reach_program.h"
#include "basic_migration_test.h"
#include "line_reach_program.h"
#include "clique_reach_program.h"
//#include "dijkstra_prog_test.h"
#include "dijkstra_tree_test.h"
#include "multiple_widest_path.h"
//#include "clustering_prog_test.h"
#endif
#ifndef __ALL_TESTS__
#include "line_reach_program.h"
#endif

int
main(int argc, char *argv[])
{
    UNUSED(argc);
    UNUSED(argv);

    DEBUG << "Starting tests." << std::endl;
#ifdef __ALL_TESTS
    message_test();
    DEBUG << "Message packing/unpacking ok." << std::endl;
    cache_test();
    DEBUG << "Shard cache ok." << std::endl;
    basic_client_test();
    DEBUG << "Basic client ok." << std::endl;
    repetitive_reach_prog();
    DEBUG << "Repetitive reach program ok." << std::endl;
    multiple_reach_prog();
    DEBUG << "Multiple reach program ok." << std::endl;
    basic_migration_test();
    DEBUG << "Basic migration ok." << std::endl;
    line_reach_prog();
    DEBUG << "Line reach program ok." << std::endl;
    clique_reach_prog();
    DEBUG << "Clique reach program ok." << std::endl;
    multiple_wp_prog();
    DEBUG << "Widest path program ok." << std::endl;
    //dijkstra_prog_test();
    dijkstra_tree_test();
    DEBUG << "Shortest path tree test ok." << std::endl;
    //clustering_prog_test();
#endif
#ifndef __ALL_TESTS__
    line_reach_prog();
#endif
    DEBUG << "All tests completed." << std::endl;

    return 0;
}
