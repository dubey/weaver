/*
 * ===============================================================
 *    Description:  Stress test to measure event ordering
 *                  performance.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2016, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/event_order.h"
#include "common/vclock.h"
#include "common/clock.h"

void
oracle_stress()
{
    if (NumVts < 2) {
        WDEBUG << "NumVts should be >= 2, exiting now" << std::endl;
        return;
    }

    order::oracle oracle;
    std::vector<vc::vclock> clocks;
    uint64_t num_ops = 100000000;
    wclock::weaver_timer timer;

    for (uint64_t i = 0; i < NumVts; i++) {
        vc::vclock clk(i, 0);
        clocks.push_back(clk);
    }

    WDEBUG << "Starting oracle stress test..." << std::endl;
    double start_time = timer.get_real_time_millis();
    double cur_time   = timer.get_real_time_millis();
    for (uint64_t i = 0; i < num_ops; i++) {
        for (uint64_t j = 0; j < NumVts; j++) {
            clocks[j].increment_clock();
        }

        oracle.compare_vts(clocks);

        if (i % 100 == 0) {
            double prev_time = cur_time;
            cur_time         = timer.get_real_time_millis();
            double cur_tput  = 100*1000 / (cur_time - prev_time);
            WDEBUG << "Done " << i << ", tput over last 100 ops = " << cur_tput << std::endl;
        }
    }
    WDEBUG << "Completed oracle stress test..." << std::endl;
    double end_time = timer.get_real_time_millis();

    double tput = num_ops*1000 / (end_time - start_time);
    WDEBUG << "tput=" << tput << std::endl;
}
