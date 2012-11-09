/*
 * =====================================================================================
 *
 *       Filename:  edge.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  Tuesday 16 October 2012 02:28:29  EDT
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

#ifndef __EDGE__
#define __EDGE__

//C
#include <stdint.h>

//STL
#include <vector>

//po6
#include <po6/net/location.h>

//GraphDB
#include "element.h"

namespace db
{
namespace element
{
	class node;

	/*
	 * An edge is an ordered relation between 2 nodes
	 * The order is always (from, to)
	 */
	class edge : public element
	{
		public:
			//edge (po6::net::location server, uint32_t time, void* mem_addr);
			edge (po6::net::location server, uint32_t time, void* mem_addr, 
				meta_element _from, meta_element _to);
		
		public:
			meta_element from;
			meta_element to;
	};

	/*
	inline
	edge :: edge (po6::net::location server, uint32_t time, void* mem_addr)
		: element (server, time, mem_addr)
	{
	}
	*/

	inline
	edge :: edge (po6::net::location server, uint32_t time, void* mem_addr, 
		meta_element _from, meta_element _to)
		: element (server, time, (void*) this)
		, from (_from)
		, to (_to)
	{
	}
}
}

#endif //__NODE__
