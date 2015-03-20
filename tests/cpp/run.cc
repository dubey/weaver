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

#define weaver_debug_
#include "common/weaver_constants.h"

#include "tests/cpp/read_only_vertex_bench.h"
//#include "message_test.h"
//#include "message_tx.h"
//#include "tx_msg_nmap.h"
//#include "vc_ordering.h"
//#include "create_graph.h"
//#include "nmap_unit_tests.h"
//#include "cache_test.h"
//#include "basic_client.h"
//#include "repetitive_reach_program.h"
//#include "new_reachability_test.h"
//#include "multiple_reach_program.h"
//#include "multiple_locality.h"
//#include "multiple_nhop_locality.h"
//#include "multiple_caching.h"
//#include "basic_migration_test.h"
//#include "line_reach_program.h"
//#include "clique_reach_program.h"
//#include "unreachable_reach_program.h"
////#include "dijkstra_prog_test.h"
//#include "dijkstra_tree_test.h"
//#include "multiple_widest_path.h"
//#include "clustering_prog_test.h"
//#include "scalability.h"

int
main(int argc, char *argv[])
{
    UNUSED(argc);
    UNUSED(argv);

    run_read_only_vertex_bench("test", 81306, 25000);
#ifdef __ALL_TESTS__
    //message_test();
    //WDEBUG << "Message packing/unpacking ok." << std::endl;
    //cache_test();
    //WDEBUG << "Shard cache ok." << std::endl;
    //basic_client_test();
    //WDEBUG << "Basic client ok." << std::endl;
    //repetitive_reach_prog(false);
    //WDEBUG << "Repetitive reach program ok." << std::endl;
    //multiple_sparse_reachability(false);
    //multiple_dense_reachability(false);
    //WDEBUG << "Multiple reach program ok." << std::endl;
    //basic_migration_test(false);
    //WDEBUG << "Basic migration ok." << std::endl;
    //line_reach_prog(false);
    //WDEBUG << "Line reach program ok." << std::endl;
    //clique_reach_prog(false);
    //WDEBUG << "Clique reach program ok." << std::endl;
    ////multiple_wp_prog(false);
    //WDEBUG << "Widest path program ok." << std::endl;
    ////dijkstra_prog_test();
    //dijkstra_tree_test(true);
    //WDEBUG << "Shortest path tree test ok." << std::endl;
    ////clustering_prog_test();
#endif
#ifndef __ALL_TESTS__
    //multiple_sparse_reachability(true);
    //unreachable_reach_prog(true);
    //clique_reach_prog(true);
    //multiple_wp_prog(true);
    //multiple_sparse_locality(true);
    //multiple_dense_locality(true);
    //scale_test();
    //multiple_nhop_sparse_locality(true);
    //multiple_nhop_dense_locality(true);
    //repetitive_reach_prog(true);
    //multiple_dense_caching(true);
    //nmap_unit_tests();
    //message_tx_test();
    //tx_msg_nmap_test();
    //vc_ordering_test();
    //create_graph_test();
    //new_reachability_test();
    //line_reach_prog(true);
    //clustering_prog_test();

#endif

    return 0;
}
