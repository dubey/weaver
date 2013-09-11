/*
 * ===============================================================
 *    Description:  Basic Kronos client test.
 *
 *        Created:  09/09/2013 02:35:50 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <chronos.h>

int
main()
{
    enum chronos_returncode status;
    chronos_client cl("127.0.0.1", 1982, KRONOS_NUM_SHARDS);
    uint64_t event_ids[10];
    for (int i = 0; i < 10; i++) {
        uint64_t ev_id = 12345678;
        int64_t ret = cl.create_event(&status, &ev_id);
        ret = cl.wait(ret, 1000000, &status);
        std::cout << ev_id << " " << ret << std::endl;
        event_ids[i] = ev_id;
    }
    std::cout << "Kronos orders:" << std::endl 
            << "CHRONOS_HAPPENS_BEFORE " << CHRONOS_HAPPENS_BEFORE << std::endl
            << "CHRONOS_HAPPENS_AFTER " << CHRONOS_HAPPENS_AFTER << std::endl
            << "CHRONOS_CONCURRENT " << CHRONOS_CONCURRENT << std::endl
            << "CHRONOS_WOULDLOOP " << CHRONOS_WOULDLOOP << std::endl
            << "CHRONOS_NOEXIST " << CHRONOS_NOEXIST << std::endl;
    for (int i = 0; i < 9; i++) {
        chronos_pair p;
        ssize_t cret;
        p.lhs = event_ids[i];
        p.rhs = event_ids[i+1];
        int64_t ret = cl.query_order(&p, 1, &status, &cret);
        ret = cl.wait(ret, 1000000, &status);
        std::cout << "for query pair (" << p.lhs << "," << p.rhs << "), got ret " << ret
                << " and order " << p.order << std::endl;
    }
}
