/*
 * ===============================================================
 *    Description:  Implementation of ordering methods.
 *
 *        Created:  03/19/2014 11:25:02 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/event_order.h"
#include "common/config_constants.h"

namespace order
{
    chronos_client *kronos_cl = chronos_client_create(KRONOS_IPADDR, KRONOS_PORT);
    po6::threads::mutex kronos_mutex;
    uint64_t cache_hits = 0;
    kronos_cache kcache;
}

// return the smaller of the two clocks
// return -1 if clocks cannot be compared
// return 2 if clocks are identical
int
order :: compare_two_clocks(const vc::vclock_t &clk1, const vc::vclock_t &clk2)
{
    int ret = 2;
    if (clk1.size() != NUM_VTS) {
        for (uint64_t c: clk1) {
            std::cerr << c << " ";
        }
        std::cerr << std::endl;
        assert(false);
    }
    if (clk2.size() != NUM_VTS) {
        for (uint64_t c: clk2) {
            std::cerr << c << " ";
        }
        std::cerr << std::endl;
        assert(false);
    }
    assert(clk1.size() == NUM_VTS);
    assert(clk2.size() == NUM_VTS);
    for (uint64_t i = 0; i < NUM_VTS; i++) {
        if ((clk1.at(i) < clk2.at(i)) && (ret != 0)) {
            if (ret == 2) {
                ret = 0;
            } else {
                return -1;
            }
        } else if ((clk1.at(i) > clk2.at(i)) && (ret != 1)) {
            if (ret == 2) {
                ret = 1;
            } else {
                return -1;
            }
        }
    }
    return ret;
}

// method which only compares vector clocks
// returns bool vector, with i'th entry as true if that clock is definitely larger than some other clock
std::vector<bool>
order :: compare_vector_clocks(const std::vector<vc::vclock> &clocks)
{
    uint64_t num_clks = clocks.size();
    std::vector<bool> large(num_clks, false); // keep track of clocks which are definitely not smallest
    uint64_t num_large = 0; // number of true values in previous vector
    for (uint64_t i = 0; i < (num_clks-1); i++) {
        for (uint64_t j = (i+1); j < num_clks; j++) {
            int cmp = compare_two_clocks(clocks.at(i).clock, clocks.at(j).clock);
            assert(cmp != 2);
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

// return the index of the (only) 'false' in a bool vector
uint64_t
order :: get_false_position(std::vector<bool> &large)
{
    for (uint64_t min_pos = 0; min_pos < large.size(); min_pos++) {
        if (!large[min_pos]) {
            return min_pos;
        }
    }
    assert(false);
    return UINT64_MAX;
}

// plain old vector clock comparison
// if the vector has a single clock which happens before all the others, the index of that clock is stored in small_idx
// large[i] is true if clocks[i] is definitely not the earliest clock
// i.e. large[i] <=> \exists j clock[j] -> clock[i], where -> is the happens before relationship for vector clocks
void
order :: compare_vts_no_kronos(const std::vector<vc::vclock> &clocks, std::vector<bool> &large, int64_t &small_idx)
{
    large = compare_vector_clocks(clocks);
    assert(clocks.size() == large.size());

    if (std::count(large.begin(), large.end(), false) == 1) {
        // Kronos not required
        small_idx = get_false_position(large);
    }
}

// vector clock comparison method
// will call Kronos if clocks are incomparable
// returns index of earliest clock`
int64_t
order :: compare_vts(const std::vector<vc::vclock> &clocks)
{
    std::vector<bool> large;
    int64_t ret_idx = INT64_MAX;

    compare_vts_no_kronos(clocks, large, ret_idx);
    if (ret_idx != INT64_MAX) {
        // Kronos not required
        return ret_idx;
    } else {
        // check cache
        uint64_t num_clks = clocks.size();
        for (uint64_t i = 0; i < num_clks; i++) {
            for (uint64_t j = i+1; j < num_clks; j++) {
                if (!large.at(i) && !large.at(j)) {
                    int cmp = kcache.compare(clocks[i].clock, clocks[j].clock);
                    if (cmp == 0) {
                        large[j] = true;
                        cache_hits++;
                    } else if (cmp == 1) {
                        large[i] = true;
                        cache_hits++;
                    } else {
                        assert(cmp == -1);
                    }
                }
            }
        }
        uint64_t num_large = std::count(large.begin(), large.end(), true);
        if (num_large == (num_clks-1)) {
            // Kronos not required
            return get_false_position(large);
        }

        // need to call Kronos
        uint64_t num_pairs = ((num_clks - num_large) * (num_clks - num_large - 1)) / 2;
        weaver_pair *wpair = (weaver_pair*)malloc(sizeof(weaver_pair) * num_pairs);
        weaver_pair *wp = wpair;
        for (uint64_t i = 0; i < num_clks; i++) {
            for (uint64_t j = i+1; j < num_clks; j++) {
                if (!large.at(i) && !large.at(j)) {
                    wp->lhs = (uint64_t*)malloc(sizeof(uint64_t) * NUM_VTS);
                    wp->rhs = (uint64_t*)malloc(sizeof(uint64_t) * NUM_VTS);
                    for (uint64_t k = 0; k < NUM_VTS; k++) {
                        wp->lhs[k] = clocks.at(i).clock.at(k);
                        wp->rhs[k] = clocks.at(j).clock.at(k);
                    }
                    wp->lhs_id = clocks.at(i).vt_id;
                    wp->rhs_id = clocks.at(j).vt_id;
                    wp->flags = CHRONOS_SOFT_FAIL;
                    wp->order = CHRONOS_HAPPENS_BEFORE;
                    wp++;
                }
            }
        }
        chronos_returncode status;
        ssize_t cret;

        kronos_mutex.lock();
        int64_t ret = kronos_cl->weaver_order(wpair, num_pairs, &status, &cret);
        ret = kronos_cl->wait(ret, 100000, &status);
        kronos_mutex.unlock();

        wp = wpair;
        std::vector<bool> large_upd = large;
        for (uint64_t i = 0; i < num_clks; i++) {
            for (uint64_t j = i+1; j < num_clks; j++) {
                if (!large.at(i) && !large.at(j)) {
                    // retrieve and set order
                    assert((wp->order == CHRONOS_HAPPENS_BEFORE) || (wp->order == CHRONOS_HAPPENS_AFTER));
                    // fine-grained locking for cache.add() so that other requests are not blocked for a long time
                    switch (wp->order) {
                        case CHRONOS_HAPPENS_BEFORE:
                            large_upd.at(j) = true;
                            kcache.add(clocks[i].clock, clocks[j].clock);
                            break;

                        case CHRONOS_HAPPENS_AFTER:
                            large_upd.at(i) = true;
                            kcache.add(clocks[j].clock, clocks[i].clock);
                            break;

                        default:
                            WDEBUG << "cannot reach here" << std::endl;
                            assert(false);
                    }
                    free(wp->lhs);
                    free(wp->rhs);
                    wp++;
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
        assert(false);
        return INT64_MAX;
    }
}

// compare two vector clocks
// return 0 if first is smaller, 1 if second is smaller, 2 if identical
int64_t
order :: compare_two_vts(const vc::vclock &clk1, const vc::vclock &clk2)
{
    int cmp = compare_two_clocks(clk1.clock, clk2.clock);
    if (cmp == -1) {
        std::vector<vc::vclock> compare_vclks;
        compare_vclks.push_back(clk1);
        compare_vclks.push_back(clk2);
        cmp = compare_vts(compare_vclks);
    }
    return cmp;
}

// return true if the first clock occurred between the second two
bool
order :: clock_creat_before_del_after(const vc::vclock &req_vclock, const vc::vclock &creat_time, const vc::vclock &del_time)
{
    int64_t cmp_1 = compare_two_vts(del_time, req_vclock);
    assert(cmp_1 != 2);
    bool toRet = (cmp_1 == 1);
    if (toRet) {
        int64_t cmp_2 = compare_two_vts(creat_time, req_vclock);
        assert(cmp_2 != 2);
        toRet = (cmp_2 == 0);
    }
    return toRet;
}
