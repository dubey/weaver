/*
 * =====================================================================================
 *
 *       Filename:  graph.h
 *
 *    Description:  Instance of a graph
 *
 *        Version:  1.0
 *        Created:  Tuesday 16 October 2012 11:00:03  EDT
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */
//Testing
#include <iostream>

//C++
#include <sstream>

//STL
#include <vector>

//GraphDB
#include "element/property.h"
#include "element/node.h"
#include "element/edge.h"

namespace db
{
	class graph
	{
		public:
			graph ();

		private:
			std::vector<element::node *> V;
			std::vector<element::edge *> E;

		public:
			int add_node (element::node *n);
			bool find_node (element::node *n);
			element::edge *add_edge (element::node *n1, element::node *n2);
			bool find_edge (element::edge *e);
			bool detect_cycle (element::node *start);
			std::vector<element::node *> transclosure (element::node *start);
	};

	inline
	graph :: graph ()
		: V(0)
		, E(0)
	{
	}

	inline int
	graph :: add_node (element::node *n)
	{
		V.push_back (n);
		return 0;
	}

	inline bool
	graph :: find_node (element::node *n)
	{
		return (std::find (V.begin(), V.end(), n) != V.end());
	}
	
	inline bool
	graph :: find_edge (element::edge *e)
	{
		return (std::find (E.begin(), E.end(), e) != E.end());
	}

	inline element::edge *
	graph :: add_edge (element::node *n1, element::node *n2)
	{
		element::edge *e = new element::edge (n1, n2);
		n1->out_edges.push_back (e);
		n2->in_edges.push_back (e);
		E.push_back (e);
		return e;
	}

	
	bool
	graph :: detect_cycle (element::node *start)
	{
		//TODO
		return false;
	}

	std::vector<element::node *> 
	graph :: transclosure (element::node *start)
	{
		/*  A unique identifier for every call to
	 	 *  the transclosure function, used to add
	 	 *  visited property */
		static int uid = 0;
		std::vector<element::node *> reachable;
		std::vector<element::node *> to_visit;
		element::node *cur;
		char *prop_value = (char *) malloc (2);
		prop_value[0] = 'v';
		prop_value[1] = '\0';
		std::stringstream ss;
		ss << (uid++);
		char *prop_key = (char *) malloc (ss.str().size()+1);
		prop_key[ss.str().size()] = 0;
		strncpy (prop_key, ss.str().c_str(), ss.str().size());
		element::property visited (prop_key, prop_value);

		//Starting the graph search process
		to_visit.push_back (start);
		while (to_visit.size() > 0)
		{
			cur = to_visit.front();
			to_visit.erase (to_visit.begin());
			std::vector<element::edge *>::iterator iter;
			for (iter = cur->out_edges.begin(); iter<cur->out_edges.end(); iter++)
			{
				if (!((*iter)->to->has_property (visited))) 
				{
					to_visit.push_back ((*iter)->to);
				}
			}
			cur->add_property (visited);
			reachable.push_back (cur);
		}

		//Done with graph search, deleting transient visited property
		std::vector<element::node *>::iterator iter;
		for (iter = reachable.begin(); iter < reachable.end(); iter++)
		{
			(*iter)->remove_property (visited);
		}

		return reachable;
}
}
