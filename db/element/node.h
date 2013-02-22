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
    class bool_wrapper
    {
        public:
            bool bval;
            bool_wrapper() {
                bval = false;
            }
    };

    class node : public element
    {
        public:
            node(std::shared_ptr<po6::net::location> server, uint64_t time);
        
        public:
            std::vector<edge *> out_edges;
            po6::threads::mutex update_mutex;
            std::unordered_map<size_t, bool_wrapper> seen; // requests which have been seen
            std::unique_ptr<std::vector<size_t>> cached_req_ids; // requests which have been cached
            void add_edge(edge *e);
            bool check_and_add_seen(size_t id);
            void remove_seen(size_t id);
            void add_cached_req(size_t req_id);
            std::unique_ptr<std::vector<size_t>> purge_cache();
    };

    inline
    node :: node(std::shared_ptr<po6::net::location> server, uint64_t time)
        : element(server, time, (void*)this)
        , cached_req_ids(new std::vector<size_t>())
    {
    }

    inline void
    node :: add_edge(edge *e)
    {
        out_edges.push_back(e);
    }

    inline bool
    node :: check_and_add_seen(size_t id)
    {
        if (seen[id].bval == true) {
            return true;
        } else {
            seen[id].bval = true;
            return false;
        }
    }

    inline void
    node :: remove_seen(size_t id)
    {
        seen.erase(id);
    }

    inline void
    node :: add_cached_req(size_t req_id)
    {
        cached_req_ids->push_back(req_id);
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
