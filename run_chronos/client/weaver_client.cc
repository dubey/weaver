/*
 * ===============================================================
 *    Description:  Weaver order test.
 *
 *        Created:  09/10/2013 10:42:47 AM
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
    chronos_client cl("127.0.0.1", 1992);
    std::cout << "Kronos orders:" << std::endl 
            << "CHRONOS_HAPPENS_BEFORE " << CHRONOS_HAPPENS_BEFORE << std::endl
            << "CHRONOS_HAPPENS_AFTER " << CHRONOS_HAPPENS_AFTER << std::endl
            << "CHRONOS_CONCURRENT " << CHRONOS_CONCURRENT << std::endl
            << "CHRONOS_WOULDLOOP " << CHRONOS_WOULDLOOP << std::endl
            << "CHRONOS_NOEXIST " << CHRONOS_NOEXIST << std::endl;
    weaver_pair p;
    p.lhs = (uint64_t*)malloc(sizeof(uint64_t) * KRONOS_NUM_VTS);
    p.rhs = (uint64_t*)malloc(sizeof(uint64_t) * KRONOS_NUM_VTS);
    p.flags = CHRONOS_SOFT_FAIL;
    ssize_t cret;
    for (int i = 0; i < 9; i++) {
        for (int j = 0 ; j < KRONOS_NUM_VTS; j++) {
            if (j == 0) {
                p.lhs[j] = i;
                p.rhs[j] = i+1;
            } else {
                p.lhs[j] = 0;
                p.rhs[j] = 0;
            }
        }
        p.lhs_id = 0;
        p.rhs_id = 0;
        p.order = CHRONOS_HAPPENS_BEFORE;
        std::cerr << "calling weaver order" << std::endl;
        int64_t ret = cl.weaver_order(&p, 1, &status, &cret);
        ret = cl.wait(ret, 1000000, &status);
        std::cout << "for query pair (" << i << "," << (i+1) << "), got ret " << ret
                << " and order " << p.order << std::endl;
    }
    for (int i = 0; i < 9; i++) {
        for (int j = 0 ; j < KRONOS_NUM_VTS; j++) {
            if (j == 0) {
                p.lhs[j] = i+1;
                p.rhs[j] = i;
            } else {
                p.lhs[j] = 0;
                p.rhs[j] = 0;
            }
        }
        p.order = CHRONOS_HAPPENS_BEFORE;
        int64_t ret = cl.weaver_order(&p, 1, &status, &cret);
        ret = cl.wait(ret, 1000000, &status);
        std::cout << "for query pair (" << (i+1) << "," << (i) << "), got ret " << ret
                << " and order " << p.order << std::endl;
    }
    free(p.lhs);
    free(p.rhs);
    weaver_pair *wpair = (weaver_pair*)malloc(sizeof(weaver_pair) * 45);
    weaver_pair *wp = wpair;
    for (int i = 0; i < 9; i++) {
        for (int k = i+1; k <= 9; k++) {
            wp->lhs = (uint64_t*)malloc(sizeof(uint64_t) * KRONOS_NUM_VTS);
            wp->rhs = (uint64_t*)malloc(sizeof(uint64_t) * KRONOS_NUM_VTS);
            for (int j = 0 ; j < KRONOS_NUM_VTS; j++) {
                if (j == 0) {
                    wp->lhs[j] = i;
                    wp->rhs[j] = k;
                } else {
                    wp->lhs[j] = 0;
                    wp->rhs[j] = 0;
                }
            }
            wp->order = CHRONOS_HAPPENS_BEFORE;
            wp->flags = CHRONOS_SOFT_FAIL;
            wp++;
        }
    }
    int64_t ret = cl.weaver_order(wpair, 1, &status, &cret);
    ret = cl.wait(ret, 1000000, &status);
    wp = wpair;
    for (int i = 0; i < 9; i++) {
        for (int k = i+1; k <= 9; k++) {
            std::cout << "for query pair (" << (i) << "," << (k) << "), got ret " << ret
                    << " and order " << wp->order << std::endl;
            assert(wp->lhs[0] == i);
            assert(wp->rhs[0] == k);
            free(wp->lhs);
            free(wp->rhs);
            wp++;
        }
    }
    free(wpair);
}
