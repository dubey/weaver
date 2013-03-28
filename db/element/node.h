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
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "common/weaver_constants.h"
#include "element.h"
#include "edge.h"

namespace db
{
class update_request;
namespace element
{
    class node : public element
    {
        public:
            node();
            node(uint64_t time);

        public:
            enum mode
            {
                NASCENT = 0,
                STABLE,
                IN_TRANSIT,
                MOVED
            };
        
        public:
            enum mode state;
            std::unordered_map<uint64_t, edge*> out_edges;
            std::unordered_map<uint64_t, edge*> in_edges;
            po6::threads::mutex update_mutex;
            std::unordered_set<uint64_t> seen; // requests which have been seen
            std::unique_ptr<std::vector<uint64_t>> cached_req_ids; // requests which have been cached
            // for migration
            size_t new_handle;
            int prev_loc, new_loc;
            std::vector<uint32_t> msg_count;

        private:
            uint32_t out_edge_ctr, in_edge_ctr;

        public:
            uint64_t add_edge(edge *e, bool in_or_out);
            bool check_and_add_seen(uint64_t id);
            void remove_seen(uint64_t id);
            void set_seen(std::unordered_set<uint64_t> &seen);
            void add_cached_req(uint64_t req_id);
            void remove_cached_req(uint64_t req_id);
            std::unique_ptr<std::vector<uint64_t>> purge_cache();
    };

    inline
    node :: node()
        : state(mode::NASCENT)
        , cached_req_ids(new std::vector<uint64_t>)
        , prev_loc(-1)
        , new_loc(-1)
        , msg_count(NUM_SHARDS, 0)
        , out_edge_ctr(0)
        , in_edge_ctr(0)
    {
    }

    inline
    node :: node(uint64_t time)
        : element(time)
        , state(mode::NASCENT)
        , cached_req_ids(new std::vector<uint64_t>())
        , prev_loc(-1)
        , new_loc(-1)
        , msg_count(NUM_SHARDS, 0)
        , out_edge_ctr(0)
        , in_edge_ctr(0)
    {
    }

    inline uint64_t
    node :: add_edge(edge *e, bool in_or_out)
    {
        if (in_or_out) {
            out_edges.emplace(e->get_creat_time(), e);
        } else {
            in_edges.emplace(e->get_creat_time(), e);
        }
        return e->get_creat_time();
    }

    inline bool
    node :: check_and_add_seen(uint64_t id)
    {
        if (seen.find(id) != seen.end()) {
            return true;
        } else {
            seen.insert(id);
            return false;
        }
    }

    inline void
    node :: remove_seen(uint64_t id)
    {
        seen.erase(id);
    }

    inline void
    node :: set_seen(std::unordered_set<uint64_t> &s)
    {
        seen = s;
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

#endif //__NODE__
