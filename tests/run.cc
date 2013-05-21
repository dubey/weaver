/*
 * ===============================================================
 *    Description:  Run all tests!
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

//#include "message_test.h"
//#include "cache_test.h"
//#include "basic_client.h"
#include "repetitive_reach_program.h"
#include "multiple_reach_program.h"
#include "dijkstra_prog_test.h"
#include "tree_test.h"
//#include "reach_prog_test.h"
#include "clustering_prog_test.h"

int
main(int argc, char *argv[])
{
    //std::set_terminate(debug_terminate);
    std::cout << "Starting tests." << std::endl;
    //message_test();
    //std::cout << "Message packing/unpacking ok." << std::endl;
    //cache_test();
    //std::cout << "Shard cache ok." << std::endl;
    //basic_client_test();
    //std::cout << "Basic client ok." << std::endl;
    //stress_client_test();
    //std::cout << "Stress client ok." << std::endl;
    //reach_prog_test();
    //repetitive_reach_prog();
    multiple_reach_prog();
    //delete_prog_test();
    //dijkstra_prog_test();
    //tree_test();
    //clustering_prog_test();
    std::cout << "Node prog tests done." << std::endl;
    //clustering_test();
    //std::cout <<"Clustering ok.\n";
    //dijkstra_test();
    //std::cout <<"Dijkstra ok.\n";

    return 0;
}
