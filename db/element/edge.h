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

//STL
#include <vector>

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
			edge ();
			edge (node *_from, node *_to);
		
		public:
			node *from;
			node *to;
	};

	inline
	edge :: edge (node *_from, node *_to)
	{
		from = _from;
		to = _to;
	}
}
}

#endif //__NODE__
