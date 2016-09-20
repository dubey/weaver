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

//#include "tests/cpp/read_only_vertex_bench.h"

int
main(int argc, char *argv[])
{
    UNUSED(argc);
    UNUSED(argv);

    if (argc != 2) {
        WDEBUG << "need exactly 1 arg: name of latency log file" << std::endl;
        return 1;
    }

    //run_read_only_vertex_bench(argv[1], 4847571, 1000);

    return 0;
}
