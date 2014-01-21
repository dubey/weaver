/*
 * ===============================================================
 *    Description: Graph node class 
 *
 *        Created:  Tuesday 16 October 2012 02:24:02  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 * 
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __NODE__
#define __NODE__

#include <stdint.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <deque>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "common/weaver_constants.h"
#include "db/cache/prog_cache.h"
#include "element.h"
#include "edge.h"

namespace message
{
    class message;
}

namespace db
{
namespace element
{
    class node : public element
    {
        public:
            node(uint64_t handle, vc::vclock &vclk, po6::threads::mutex *mtx);

        public:
            enum mode
            {
                NASCENT = 0,
                STABLE,
                MOVED
            };
        
        public:
            enum mode state;
            std::unordered_map<uint64_t, edge*> out_edges;
            po6::threads::cond cv; // for locking node
            po6::threads::cond migr_cv; // make reads/writes wait while node is being migrated
            std::deque<std::pair<uint64_t, uint64_t>> tx_queue; // queued txs, identified by <vt_id, queue timestamp> tuple
            bool in_use;
            uint32_t waiters; // count of number of waiters
            bool permanently_deleted;
            std::unique_ptr<std::vector<uint64_t>> cached_req_ids; // requests which have been cached

            // for migration
            uint64_t new_loc;
            uint64_t update_count;
            std::vector<double> migr_score;
            std::vector<uint32_t> msg_count;
            bool updated, already_migr;
            uint32_t dependent_del;
            // queued requests, for the time when the node is marked in transit
            // but requests cannot yet be forwarded to new location which is still
            // setting up the node
            std::vector<std::unique_ptr<message::message>> pending_requests;

            // for node prog caching
            caching::program_cache cache; // TODO init me, also XXX migrate me
            bool checking_cache;


        public:
            void add_edge(edge *e);
            void add_cached_req(uint64_t req_id);
            void remove_cached_req(uint64_t req_id);
            std::unique_ptr<std::vector<uint64_t>> purge_cache();
    };

    inline
    node :: node(uint64_t handle, vc::vclock &vclk, po6::threads::mutex *mtx)
        : element(handle, vclk)
        , state(mode::NASCENT)
        , cv(mtx)
        , migr_cv(mtx)
        , in_use(true)
        , waiters(0)
        , permanently_deleted(false)
        , cached_req_ids(new std::vector<uint64_t>())
        , new_loc(UINT64_MAX)
        , update_count(1)
        , migr_score(NUM_SHARDS, 0)
        , updated(true)
        , already_migr(false)
        , dependent_del(0)
        , cache()
        , checking_cache(false)
    { }

    inline void
    node :: add_edge(edge *e)
    {
        out_edges.emplace(e->get_handle(), e);
    }

    inline void
    node :: add_cached_req(uint64_t req_id)
    {
        cached_req_ids->push_back(req_id);
    }

    inline void
    node :: remove_cached_req(uint64_t req_id)
    {
        cached_req_ids->erase(std::remove(cached_req_ids->begin(), cached_req_ids->end(), req_id), cached_req_ids->end());
    }

    inline std::unique_ptr<std::vector<uint64_t>>
    node :: purge_cache()
    {
        std::unique_ptr<std::vector<uint64_t>> ret = std::move(cached_req_ids);
        cached_req_ids.reset(new std::vector<uint64_t>());
        return ret;
    }

    inline bool
    compare_msg_cnt(const node *n1, const node *n2)
    {
        uint64_t s1 = 0;
        uint64_t s2 = 0;
        for (uint32_t i: n1->msg_count) {
            s1 += (uint64_t)i;
        }
        for (uint32_t i: n2->msg_count) {
            s2 += (uint64_t)i;
        }
        return (s1<s2);
    }
}
}

#endif
