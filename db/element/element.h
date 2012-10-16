/*
 * =====================================================================================
 *
 *       Filename:  element.h
 *
 *    Description:  Graph element (edges and vertices)
 *
 *        Version:  1.0
 *        Created:  Thursday 11 October 2012 11:15:20  EDT
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

//STL
#include <vector>


//Weaver
#include "element/property.h"

namespace db
{
namespace element
{
	class element
	{
		public:
			element ();
			
		private:
			std::vector<property> properties;

		public:
			void add_property (property prop);
			int get_prop_value (char *_key, char *_value);
	};

	inline
	element :: buffer ()
		: properties(0)
	{
	}

	inline void
	element :: add_property (property prop)
	{
		properties.push_back (prop);
	}

	inline int
	element :: get_prop_value (char *_key, char *_value)
	{
		std::vector<property> iterator iter;
		for (iter = properties.begin(); iter < properties.end(); iter++)
		{
			if (iter->key == _key
		}
	}
	}
}
}
