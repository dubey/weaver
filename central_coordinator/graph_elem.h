/*
 * =====================================================================================
 *
 *       Filename:  graph_elem.h
 *
 *    Description:  The physical server and memory address of a graph element
 *
 *        Version:  1.0
 *        Created:  11/08/2012 01:11:01 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

#ifndef __GRAPH_ELEM__
#define __GRAPH_ELEM__

//po6
#include <po6/net/location.h>

namespace central_coordinator
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

} // namespace central_coordinator

#endif // __GRAPH_ELEM__
