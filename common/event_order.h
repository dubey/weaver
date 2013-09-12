/*
 * ===============================================================
 *    Description:  Static methods and objects for consistently
 *                  ordering events. Uses Kronos if necessary.
 *
 *        Created:  09/06/2013 02:25:23 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __WEAVER_ORDERING__
#define __WEAVER_ORDERING__

#include <chronos.h>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "common/vclock.h"

namespace order
{
    // static chronos client (and assoc. mutex), which ensures only one client per shard
    static chronos_client *kronos_cl;
    static po6::threads::mutex kronos_mutex;

    // return the smaller of the two clocks
    // return -1 if clocks cannot be compared
    // return 2 if clocks are identical
    static inline int
    compare_two_clocks(const vc::vclock_t &clk1, const vc::vclock_t &clk2)
    {
        int ret = 2;
        for (uint64_t i = 0; i < NUM_SHARDS; i++) {
            if ((clk1.at(i) < clk2.at(i)) && (ret != 0)) {
                if (ret == 2) {
                    ret = 0;
                } else {
                    ret = -1;
                    break;
                }
            } else if ((clk1.at(i) > clk2.at(i)) && (ret != 1)) {
                if (ret == 2) {
                    ret = 1;
                } else {
                    ret = -1;
                    break;
                }
            }
        }
        return ret;
    }

    // static method which only compares vector clocks
    // returns bool vector, with i'th entry as true if that clock is definitely larger than some other clock
    static inline std::vector<bool>
    compare_vector_clocks(const std::vector<vc::vclock_t> &clocks)
    {
        uint64_t num_clks = clocks.size();
        std::vector<bool> large(num_clks, false); // keep track of clocks which are definitely not smallest
        uint64_t num_large = 0; // number of true values in previous vector
        for (uint64_t i = 0; i < (num_clks-1); i++) {
            for (uint64_t j = (i+1); j < num_clks; j++) {
                int cmp = compare_two_clocks(clocks.at(i), clocks.at(j));
                if ((cmp == 0) && !large.at(j)) {
                    large.at(j) = true;
                    num_large++;
                } else if ((cmp == 1) && !large.at(i)) {
                    large.at(i) = true;
                    num_large++;
                }
            }
            if (num_large == (num_clks-1)) {
                return large;
            }
        }
        assert(num_large < (num_clks-1));
        return large;
    }

    // static vector clock comparison method
    // will call Kronos daemon if clocks are incomparable
    // returns index of earliest clock
    static inline int64_t
    compare_vts(const std::vector<vc::vclock_t> &clocks)
    {
        uint64_t min_pos;
        uint64_t num_clks = clocks.size();
        std::vector<bool> large = compare_vector_clocks(clocks);
        uint64_t num_large = std::count(large.begin(), large.end(), true);
        if (num_large == (num_clks-1)) {
            for (min_pos = 0; min_pos < num_clks; min_pos++) {
                if (!large.at(min_pos)) {
                    break;
                }
            }
            assert(min_pos < num_clks);
            return min_pos;
        } else {
            // need to call Kronos
            uint64_t num_pairs = ((num_clks - num_large) * (num_clks - num_large - 1)) / 2;
            DEBUG << "num pairs = " << num_pairs << std::endl;
            weaver_pair *wpair = (weaver_pair*)malloc(sizeof(weaver_pair) * num_pairs);
            weaver_pair *wp = wpair;
            for (uint64_t i = 0; i < num_clks; i++) {
                for (uint64_t j = i+1; j < num_clks; j++) {
                    if (!large.at(i) && !large.at(j)) {
                        wp->lhs = (uint64_t*)malloc(sizeof(uint64_t) * NUM_SHARDS);
                        wp->rhs = (uint64_t*)malloc(sizeof(uint64_t) * NUM_SHARDS);
                        for (uint64_t k = 0; k < NUM_SHARDS; k++) {
                            wp->lhs[k] = clocks.at(i).at(k);
                            wp->rhs[k] = clocks.at(j).at(k);
                            DEBUG << wp->lhs[k] << " " << wp->rhs[k] << std::endl;
                        }
                        wp->flags = CHRONOS_SOFT_FAIL;
                        if (i == 0) {
                            wp->order = CHRONOS_HAPPENS_BEFORE;
                            DEBUG << "assigning preference of happens before to (" << i << "," << j << ")\n";
                        } else {
                            wp->order = CHRONOS_CONCURRENT;
                        }
                        wp++;
                    }
                }
            }
            chronos_returncode status;
            ssize_t cret;
            kronos_mutex.lock();
            int64_t ret = kronos_cl->weaver_order(wpair, 1, &status, &cret);
            ret = kronos_cl->wait(ret, 100000, &status);
            kronos_mutex.unlock();
            wp = wpair;
            std::vector<bool> large_upd = large;
            for (uint64_t i = 0; i < num_clks; i++) {
                for (uint64_t j = i+1; j < num_clks; j++) {
                    if (!large.at(i) && !large.at(j)) {
                        // retrieve and set order
                        switch (wp->order) {
                            case CHRONOS_HAPPENS_BEFORE:
                                large_upd.at(j) = true;
                                break;

                            case CHRONOS_HAPPENS_AFTER:
                                large_upd.at(i) = true;
                                break;

                            default:
                                DEBUG << "unexpected Kronos order" << wp->order << std::endl;
                        }
                        free(wp->lhs);
                        free(wp->rhs);
                    }
                }
            }
            free(wpair);
            for (uint64_t min_pos = 0; min_pos < num_clks; min_pos++) {
                if (!large_upd.at(min_pos)) {
                    return min_pos;
                }
            }
            // should never reach here
            return num_clks;
        }
    }

    // compare two vector clocks
    // return 0 if first is smaller, 1 if second is smaller, 2 if identical
    static inline int64_t
    compare_two_vts(const vc::vclock_t &clk1, const vc::vclock_t &clk2)
    {
        int cmp = compare_two_clocks(clk1, clk2);
        if (cmp == -1) {
            std::vector<vc::vclock_t> compare_vclks;
            compare_vclks.push_back(clk1);
            compare_vclks.push_back(clk2);
            cmp = compare_vts(compare_vclks);
        }
        return cmp;
    }
}

#endif
