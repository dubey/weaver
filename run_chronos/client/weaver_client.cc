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
    //std::cout << "Kronos orders:" << std::endl 
    //        << "CHRONOS_HAPPENS_BEFORE " << CHRONOS_HAPPENS_BEFORE << std::endl
    //        << "CHRONOS_HAPPENS_AFTER " << CHRONOS_HAPPENS_AFTER << std::endl
    //        << "CHRONOS_CONCURRENT " << CHRONOS_CONCURRENT << std::endl
    //        << "CHRONOS_WOULDLOOP " << CHRONOS_WOULDLOOP << std::endl
    //        << "CHRONOS_NOEXIST " << CHRONOS_NOEXIST << std::endl;
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
        int64_t ret = cl.weaver_order(&p, 1, &status, &cret);
        ret = cl.wait(ret, 1000000, &status);
        assert(p.order == CHRONOS_HAPPENS_BEFORE);
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
        assert(p.order == CHRONOS_HAPPENS_AFTER);
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
            wp->lhs_id = 0;
            wp->rhs_id = 0;
            wp++;
        }
    }
    int64_t ret = cl.weaver_order(wpair, 45, &status, &cret);
    ret = cl.wait(ret, 1000000, &status);
    wp = wpair;
    for (int i = 0; i < 9; i++) {
        for (int k = i+1; k <= 9; k++) {
            assert(wp->lhs[0] == i);
            assert(wp->rhs[0] == k);
            assert(wp->order == CHRONOS_HAPPENS_BEFORE);
            wp->order = CHRONOS_HAPPENS_AFTER;
            wp++;
        }
    }
    ret = cl.weaver_order(wpair, 45, &status, &cret);
    ret = cl.wait(ret, 1000000, &status);
    wp = wpair;
    for (int i = 0; i < 9; i++) {
        for (int k = i+1; k <= 9; k++) {
            assert(wp->lhs[0] == i);
            assert(wp->rhs[0] == k);
            assert(wp->order == CHRONOS_HAPPENS_BEFORE);
            free(wp->lhs);
            free(wp->rhs);
            wp++;
        }
    }
    free(wpair);

    // transitive vector clock ordering test
    weaver_pair *wp_arr = (weaver_pair*)malloc(sizeof(weaver_pair) * 2);
    for (int i = 0; i < 2; i++) {
        wp_arr[i].lhs = (uint64_t*)malloc(sizeof(uint64_t) * KRONOS_NUM_VTS);
        wp_arr[i].rhs = (uint64_t*)malloc(sizeof(uint64_t) * KRONOS_NUM_VTS);
        for (int j = 0; j < KRONOS_NUM_VTS; j++) {
            wp_arr[i].lhs[j] = 0;
            wp_arr[i].rhs[j] = 0;
        }
        wp_arr[i].order = CHRONOS_HAPPENS_BEFORE;
        wp_arr[i].flags = CHRONOS_SOFT_FAIL;
    }
    // (2,0,0..) < (0,1,1,....)
    wp_arr[0].lhs[0] = 2;
    wp_arr[0].rhs[1] = 1;
    wp_arr[0].rhs[2] = 1;
    wp_arr[0].lhs_id = 0;
    wp_arr[0].rhs_id = 1;
    // (1,0,0,...) <? (0,1,1,....)
    wp_arr[1].lhs[1] = 1;
    wp_arr[1].lhs[2] = 1;
    wp_arr[1].rhs[0] = 1;
    wp_arr[1].lhs_id = 1;
    wp_arr[1].rhs_id = 0;
    ret = cl.weaver_order(wp_arr, 2, &status, &cret);
    ret = cl.wait(ret, 1000000, &status);
    assert(wp_arr[0].order == CHRONOS_HAPPENS_BEFORE);
    assert(wp_arr[1].order == CHRONOS_HAPPENS_AFTER);
    for (int i = 0; i < 2; i++) {
        free(wp_arr[i].lhs);
        free(wp_arr[i].rhs);
    }
    free(wp_arr);
}
