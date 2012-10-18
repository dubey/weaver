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

#ifndef __ELEMENT__
#define __ELEMENT__

//STL
#include <vector>
#include <algorithm>
#include <string.h>

//GraphDB
#include "property.h"

namespace db
{
namespace element
{
	class element
	{
		public:
			element ();
			
		protected:
			std::vector<property> properties;

		public:
			void add_property (property prop);
			void remove_property (property prop);
			int get_prop_value (char *_key, char **_value);
			bool has_property (property prop);
			
		public:
			//Testing functions
			int num_properties ();
	};

	inline
	element :: element ()
		: properties(0)
	{
	}

	inline void
	element :: add_property (property prop)
	{
		properties.push_back (prop);
	}

	inline void
	element :: remove_property (property prop)
	{
		std::vector<property>::iterator iter;
		iter = std::find (properties.begin(), properties.end(), prop);
		properties.erase (iter);
	}

	int
	element :: get_prop_value (char *_key, char **_value)
	{
		std::vector<property>::iterator iter;
		for (iter = properties.begin(); iter < properties.end(); iter++)
		{
			if (strcmp (iter->key, _key) == 0)
			{
				*_value = (char *) malloc(strlen (iter->value));
				strncpy (*_value, iter->value, strlen (iter->value));
				return 0;
			}
		}
		return -1;
	}

	bool
	element :: has_property (property prop)
	{
		std::vector<property>::iterator iter;
		for (iter = properties.begin(); iter<properties.end(); iter++)
		{
			if ((strcmp (iter->key, prop.key) == 0) && 
				(strcmp (iter->value, prop.value) == 0))
			{
				return true;
			}
		}
		return false;
	}

	inline int
	element :: num_properties ()
	{
		return properties.size();
	}
}
}

#endif //__ELEMENT__
