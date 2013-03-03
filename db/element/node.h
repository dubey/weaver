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

#include "element.h"
#include "edge.h"

namespace db
{
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
            std::unordered_map<size_t, edge*> out_edges;
            std::unordered_map<size_t, edge*> in_edges;
            po6::threads::mutex update_mutex;
            std::unordered_set<size_t> seen; // requests which have been seen
            std::unique_ptr<std::vector<size_t>> cached_req_ids; // requests which have been cached
            // for migration
            bool in_transit;
            uint32_t num_pending_updates;
            int prev_loc, new_loc;

        private:
            uint32_t out_edge_ctr, in_edge_ctr;

        public:
            size_t add_edge(edge *e, bool in_or_out);
            bool check_and_add_seen(size_t id);
            void remove_seen(size_t id);
            void set_seen(std::unordered_set<size_t> &seen);
            void add_cached_req(size_t req_id);
            void remove_cached_req(size_t req_id);
            std::unique_ptr<std::vector<size_t>> purge_cache();
    };

    inline
    node :: node()
        : state(mode::NASCENT)
        , cached_req_ids(new std::vector<size_t>)
        , out_edge_ctr(0)
        , in_edge_ctr(0)
        , num_pending_updates(0)
        , prev_loc(-1)
        , new_loc(-1)
    {
    }

    inline
    node :: node(uint64_t time)
        : element(time)
        , cached_req_ids(new std::vector<size_t>())
        , in_transit(false)
        , nascent(true)
        , out_edge_ctr(0)
        , in_edge_ctr(0)
        , num_pending_updates(0)
        , prev_loc(-1)
        , new_loc(-1)
    {
    }

    inline size_t
    node :: add_edge(edge *e, bool in_or_out)
    {
        if (in_or_out) {
            out_edges.emplace(out_edge_ctr, e);
            return (out_edge_ctr++);
        } else {
            in_edges.emplace(in_edge_ctr, e);
            return (in_edge_ctr++);
        }
    }

    inline bool
    node :: check_and_add_seen(size_t id)
    {
        if (seen.find(id) != seen.end()) {
            return true;
        } else {
            seen.insert(id);
            return false;
        }
    }

    inline void
    node :: remove_seen(size_t id)
    {
        seen.erase(id);
    }

    inline void
    node :: set_seen(std::unordered_set<size_t> &s)
    {
        seen = s;
    }

    inline void
    node :: add_cached_req(size_t req_id)
    {
        cached_req_ids->push_back(req_id);
    }

    inline void
    node :: remove_cached_req(size_t req_id)
    {
        cached_req_ids->erase(std::remove(cached_req_ids->begin(), cached_req_ids->end(), req_id), cached_req_ids->end());
    }

    inline std::unique_ptr<std::vector<size_t>>
    node :: purge_cache()
    {
        std::unique_ptr<std::vector<size_t>> ret = std::move(cached_req_ids);
        cached_req_ids.reset(new std::vector<size_t>());
        return ret;
    }
}
}

#endif //__NODE__
