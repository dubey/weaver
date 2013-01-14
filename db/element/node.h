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
#include "../cache/cache.h"

namespace db
{
namespace element
{
    class edge;

    class node : public element
    {
        public:
            node(po6::net::location server, uint32_t time, void* mem_addr);
        
        public:
            std::vector<meta_element> out_edges;
            std::vector<meta_element> in_edges;
            cache::reach_cache cache;
            po6::threads::mutex cache_mutex;
    };

    inline
    node :: node(po6::net::location server, uint32_t time, void* mem_addr)
        : element(server, time, (void*) this)
    {
    }
}
}

#endif //__NODE__
