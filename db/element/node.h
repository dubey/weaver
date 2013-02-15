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

#include "element.h"
#include "edge.h"
#include "db/cache/cache.h"

namespace db
{
namespace element
{
    class node : public element
    {
        public:
            node(std::shared_ptr<po6::net::location> server, uint64_t time);
        
        public:
            std::vector<edge *> out_edges;
            cache::reach_cache cache;
            po6::threads::mutex update_mutex;
            std::vector<size_t> seen; // requests which have been seen
            void add_edge(edge *e);
            bool check_and_add_seen(size_t id);
            void remove_seen(size_t id);
    };

    inline
    node :: node(std::shared_ptr<po6::net::location> server, uint64_t time)
        : element(server, time, (void*)this)
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
        std::vector<size_t>::iterator iter;
        for (iter = seen.begin(); iter < seen.end(); iter++)
        {
            if (id == *iter) {
                return true;
            }
        }
        seen.push_back(id);
    }

    inline void
    node :: remove_seen(size_t id)
    {
        size_t pos = 0;
        for (; pos < seen.size(); pos++)
        {
            if (seen[pos] == id) {
                break;
            }
        }
        if (pos < seen.size()) {
            seen.erase(seen.begin() + pos);
        }
    }

}
}

#endif //__NODE__
