/*
 * ===============================================================
 *    Description:  Coordinator database elements -- shard server 
 *                  and memory address of each node and edge
 *
 *        Created:  11/08/2012 01:11:01 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __GRAPH_ELEM__
#define __GRAPH_ELEM__

#include <po6/net/location.h>

namespace coordinator
{
    class graph_elem
    {
        public:
            graph_elem ();
            graph_elem (po6::net::location _loc1, po6::net::location _loc2,
                void *_mem_addr1, void *_mem_addr2);

        public:
            /* If this is a node, then only loc1, mem_addr1 are relevant
             * If this is an edge, then it is directed 1 --> 2
             */
            po6::net::location loc1, loc2;
            void *mem_addr1, *mem_addr2;
    }; // class graph_elem

    inline
    graph_elem :: graph_elem (po6::net::location _loc1, po6::net::location _loc2,
                void *_mem_addr1, void *_mem_addr2)
        : loc1 (_loc1)
        , loc2 (_loc2)
        , mem_addr1 (_mem_addr1)
        , mem_addr2 (_mem_addr2)
    {
    }

} // namespace coordinator

#endif // __GRAPH_ELEM__
