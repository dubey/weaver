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

#ifndef weaver_common_event_order_h_
#define weaver_common_event_order_h_

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"
#include "common/vclock.h"
#include "chronos/chronos.h"

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

    extern chronos_client *kronos_cl;
    extern po6::threads::mutex kronos_mutex;
    extern uint64_t cache_hits;
    extern kronos_cache kcache;

    int compare_two_clocks(const vc::vclock_t &clk1, const vc::vclock_t &clk2);
    std::vector<bool> compare_vector_clocks(const std::vector<vc::vclock> &clocks);
    uint64_t get_false_position(std::vector<bool> &large);
    void compare_vts_no_kronos(const std::vector<vc::vclock> &clocks, std::vector<bool> &large, int64_t &small_idx);
    int64_t compare_vts(const std::vector<vc::vclock> &clocks);
    int64_t compare_two_vts(const vc::vclock &clk1, const vc::vclock &clk2);
    bool clock_creat_before_del_after(const vc::vclock &req_vclock, const vc::vclock &creat_time, const vc::vclock &del_time);
    bool assign_vt_order(const std::vector<vc::vclock> &before, const vc::vclock &after);

}

#endif
