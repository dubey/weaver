/*
 * ===============================================================
 *    Description:  Test hyper stub performance.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "db/hyper_stub.h"
#include "common/clock.h"
#include "common/config_constants.h"

DECLARE_CONFIG_CONSTANTS;

int main(int argc, char *argv[])
{
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <node_name>" << std::endl;
        return -1;
    }

    init_config_constants();
    vclock_ptr_t dummy_clk;
    db::node n(argv[1], 0, dummy_clk, nullptr);
    db::hyper_stub hs(0,0); 
    wclock::weaver_timer timer;

    uint64_t start = timer.get_real_time_millis();
    if (hs.recover_node(n)) {
        std::cout << "node " << n.get_handle()
                  << " has #edges=" << n.out_edges.size()
                  << std::endl;
    } else {
        std::cerr << "error in get_node, probably " << argv[1] << " does not exist" << std::endl;
    }
    uint64_t end   = timer.get_real_time_millis();
    float secs     = (end-start) / 1000.0;
    std::cout << "time taken=" << secs << " s" << std::endl;

    n.out_edges.clear();
    return 0;
}
