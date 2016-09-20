/*
 * ===============================================================
 *    Description:  Test vector clock ordering, including
 *                  ambiguous cases which involve Kronos calls.
 *
 *        Created:  09/10/2013 02:05:20 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/event_order.h"

void
vc_ordering_test()
{
    vc::vclock clk1, clk2;
    clk1.vt_id = 0;
    clk2.vt_id = 1;
    for (uint64_t i = 0; i < NUM_VTS; i++) {
        clk1.clock.push_back(1);
        clk2.clock.push_back(0);
    }
    assert(order::compare_two_clocks(clk1.clock, clk2.clock) == 1);
    assert(order::compare_two_clocks(clk2.clock, clk1.clock) == 0);
    assert(order::compare_two_clocks(clk2.clock, clk2.clock) == 2);
    assert(order::compare_two_vts(clk1, clk2) == 1);
    assert(order::compare_two_vts(clk2, clk1) == 0);
    assert(order::compare_two_vts(clk2, clk2) == 2);
    clk2.clock.at(0) = 2;
    assert(order::compare_two_clocks(clk1.clock, clk2.clock) == -1);
    assert(order::compare_two_clocks(clk2.clock, clk1.clock) == -1);
    order::kronos_cl = new chronos_client(KRONOS_IPADDR, KRONOS_PORT);
    assert(NUM_VTS == KRONOS_NUM_VTS);
    std::cout << order::compare_two_vts(clk1, clk2) << std::endl;
    std::cout << order::compare_two_vts(clk2, clk1) << std::endl;
    assert(order::compare_two_vts(clk1, clk2) == 0);
    assert(order::compare_two_vts(clk2, clk1) == 1);
}
