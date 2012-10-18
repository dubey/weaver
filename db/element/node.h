/*
 * =====================================================================================
 *
 *       Filename:  node.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  Tuesday 16 October 2012 02:24:02  EDT
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

#ifndef __NODE__
#define __NODE__

//STL
#include <vector>

//GraphDB
#include "element.h"

namespace db
{
namespace element
{
	class edge;

	class node : public element
	{
		public:
			node ();
		
		public:
			std::vector<edge *> out_edges;
			std::vector<edge *> in_edges;	
	};

	inline
	node :: node()
		: element()
	{
	}
}
}

#endif //__NODE__
