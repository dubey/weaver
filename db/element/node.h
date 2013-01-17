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
#include <po6/threads/mutex.h>

#include "element.h"
#include "db/cache/cache.h"

namespace db
{
namespace element
{
    class edge;

    class node : public element
    {
        public:
            node(std::shared_ptr<po6::net::location> server, uint64_t time);
        
        public:
            std::vector<edge *> out_edges;
            cache::reach_cache cache;
            po6::threads::mutex cache_mutex;
            void add_edge(edge *e);
    };

    inline
    node :: node(std::shared_ptr<po6::net::location> server, uint64_t time)
        : element(server, time, (void*)this)
    {
    }

    inline void
    node :: add_edge(edge* e)
    {
        update_mutex.lock();
        out_edges.push_back(e);
        update_mutex.unlock();
    }
}
}

#endif //__NODE__
