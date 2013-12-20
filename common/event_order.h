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

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <chronos.h>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "common/vclock.h"
#include "common/clock.h"

namespace order
{
    class kronos_cache
    {
        private:
            // vclock -> id which represents the clock. Space saving optimization
            std::unordered_map<vc::vclock_t, uint64_t> id_map;
            // unique clock id generating counter
            uint64_t id_gen;
            // vclock -> set of vclocks that happen after it
            std::unordered_map<uint64_t, std::unordered_set<uint64_t>> happens_before_list;
            po6::threads::mutex mutex;

        public:
            kronos_cache() : id_gen(0) { }

            // return the index (0 or 1) of smaller clock if exists in cache
            // return -1 if doesn't exist
            int compare(const vc::vclock_t &clk1, const vc::vclock_t &clk2)
            {
                mutex.lock();
                if (id_map.find(clk1) != id_map.end()
                 && id_map.find(clk2) != id_map.end()) {
                    uint64_t id1 = id_map[clk1];
                    uint64_t id2 = id_map[clk2];
                    WDEBUG << "possible Kronos cache, looking up now\n";
                    if (happens_before_list.find(id1) != happens_before_list.end()) {
                        if (happens_before_list[id1].find(id2) != happens_before_list[id1].end()) {
                            mutex.unlock();
                            WDEBUG << "Kronos cache hit!\n";
                            return 0;
                        }
                    }
                    if (happens_before_list.find(id2) != happens_before_list.end()) {
                        if (happens_before_list[id2].find(id1) != happens_before_list[id2].end()) {
                            mutex.unlock();
                            WDEBUG << "Kronos cache hit!\n";
                            return 1;
                        }
                    }
                }
                mutex.unlock();
                return -1;
            }

            // clk1 happens before clk2
            void add(const vc::vclock_t &clk1, const vc::vclock_t &clk2)
            {
                mutex.lock();
                uint64_t id1, id2;

                if (id_map.find(clk1) != id_map.end()) {
                    id1 = id_map[clk1];
                } else {
                    id1 = id_gen++;
                    id_map[clk1] = id1;
                }

                if (id_map.find(clk2) != id_map.end()) {
                    id2 = id_map[clk2];
                } else {
                    id2 = id_gen++;
                    id_map[clk2] = id2;
                }

                happens_before_list[id1].emplace(id2);
                mutex.unlock();
            }

            void remove(const vc::vclock_t &clk)
            {
                mutex.lock();
                if (id_map.find(clk) != id_map.end()) {
                    happens_before_list.erase(id_map[clk]);
                    id_map.erase(clk);
                }
                mutex.unlock();
            }
    };

    // static chronos client (and assoc. mutex), which ensures only one client per shard
    static chronos_client *kronos_cl;
    static po6::threads::mutex kronos_mutex;
    static std::list<uint64_t> *call_times;
    static uint64_t cache_hits = 0;
    static kronos_cache kcache;

    // return the smaller of the two clocks
    // return -1 if clocks cannot be compared
    // return 2 if clocks are identical
    static inline int
    compare_two_clocks(const vc::vclock_t &clk1, const vc::vclock_t &clk2)
    {
        int ret = 2;
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

    // static method which only compares vector clocks
    // returns bool vector, with i'th entry as true if that clock is definitely larger than some other clock
    static inline std::vector<bool>
    compare_vector_clocks(const std::vector<vc::vclock> &clocks)
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
    inline uint64_t
    get_false_position(std::vector<bool> &large)
    {
        for (uint64_t min_pos = 0; min_pos < large.size(); min_pos++) {
            if (!large[min_pos]) {
                return min_pos;
            }
        }
        assert(false);
        return UINT64_MAX;
    }

    // static vector clock comparison method
    // will call Kronos if clocks are incomparable
    // returns index of earliest clock
    static inline int64_t
    compare_vts(const std::vector<vc::vclock> &clocks)
    {
        uint64_t num_clks = clocks.size();
        std::vector<bool> large = compare_vector_clocks(clocks);
        uint64_t num_large = std::count(large.begin(), large.end(), true);

        if (num_large == (num_clks-1)) {
            // Kronos not required
            return get_false_position(large);
        } else {
            // check cache
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
            num_large = std::count(large.begin(), large.end(), true);
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
            timespec ts;
            uint64_t start_time = wclock::get_time_elapsed(ts);
            int64_t ret = kronos_cl->weaver_order(wpair, num_pairs, &status, &cret);
            ret = kronos_cl->wait(ret, 100000, &status);
            uint64_t end_time = wclock::get_time_elapsed(ts);
            call_times->emplace_back(end_time-start_time);
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
    static inline int64_t
    compare_two_vts(const vc::vclock &clk1, const vc::vclock &clk2)
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
}

#endif
