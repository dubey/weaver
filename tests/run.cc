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

//#include "message_test.h"
//#include "cache_test.h"
//#include "basic_client.h"
//#include "simple_stress_client.h"
#include "multiple_stress_client.h"
//#include "repetitive_stress_client.h"
//#include "clustering_test.h"

int
main(int argc, char *argv[])
{
    std::cout << "Starting tests." << std::endl;
    //message_test();
    //std::cout << "Message packing/unpacking ok." << std::endl;
    //cache_test();
    //std::cout << "Shard cache ok." << std::endl;
    //basic_client_test();
    //std::cout << "Basic client ok." << std::endl;
    //stress_client_test();
    //std::cout << "Stress client ok." << std::endl;
#define DEBUG
    multiple_stress_client();
    std::cout << "Multiple stress client ok." << std::endl;
    //repetitive_stress_client();
    //std::cout << "Repetitive stress client ok." << std::endl;
    //clustering_test();
    //std::cout <<"Clustering ok.\n";

    return 0;
}
