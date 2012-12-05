/*
 * =====================================================================================
 *
 *       Filename:  graph.h
 *
 *    Description:  The part of a graph stored on a particular server
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

#ifndef __GRAPH__
#define __GRAPH__

//Testing
#include <iostream>

//C++
#include <sstream>
#include <stdlib.h>

//STL
#include <vector>

//po6
#include <po6/net/location.h>

//Busybee
#include <busybee_sta.h>

//GraphDB
#include "element/property.h"
#include "element/meta_element.h"
#include "element/node.h"
#include "element/edge.h"

namespace db
{
	class graph
	{
		public:
			graph (const char* ip_addr, in_port_t port);

		private:
			std::vector<element::node *> V;
			std::vector<element::edge *> E;

		public:
			int node_count;
			po6::net::location myloc;
			busybee_sta bb;
			element::node* create_node (uint32_t time);
			element::edge* create_edge (element::meta_element* n1,
				element::meta_element* n2, uint32_t direction, uint32_t time);
			bool mark_visited (element::node *n, uint32_t req_counter);
			//void delete_node (element::node 
			//bool find_node (element::node **n);
			//bool find_edge (element::edge **e);
			//std::vector<element::node *> transclosure (element::node *start);
	
	}; //class graph

	inline
	graph :: graph (const char* ip_addr, in_port_t port)
		: node_count (0)
		, myloc (ip_addr, port)
		, bb (myloc.address, myloc.port, 0)
	{
	}

	inline element::node*
	graph :: create_node (uint32_t time)
	{
		element::node* new_node = new element::node (myloc, time, NULL);
		V.push_back (new_node);
		
		std::cout << "Creating node, addr = " << (void*) new_node 
				  << " and node count " << (++node_count) << std::endl;
		return new_node;
	}

	inline element::edge*
	graph :: create_edge (element::meta_element* n1, element::meta_element* n2,
		uint32_t direction, uint32_t time)
	{
		element::node *local_node = (element::node *) n1->get_addr();
		element::edge *new_edge;
		if (direction == 0) 
		{
			new_edge = new element::edge (myloc, time, NULL, *n1, *n2);
			local_node->out_edges.push_back (new_edge->get_meta_element());
		} else if (direction == 1)
		{
			new_edge = new element::edge (myloc, time, NULL, *n2, *n1);
			local_node->in_edges.push_back (new_edge->get_meta_element());
		} else
		{
			std::cerr << "edge direction error: " << direction << std::endl;
			return NULL;
		}
		E.push_back (new_edge);

		std::cout << "Creating edge, addr = " << (void *) new_edge << std::endl;
		return new_edge;
	}

	inline bool
	graph :: mark_visited (element::node *n, uint32_t req_counter)
	{
		uint32_t key = 0; //visited key
		/*char key[] = "v\0";
		char *value = (char *) malloc (10);
		memset (value, '\0', 10);
		std::stringstream out;
		out << req_counter;
		strncpy (value, out.str().c_str(), out.str().length());
		std::cout << "string of req counter " << key
				  << "," << value << " " ;
		*/
		element::property p (key, req_counter);
		if (n->has_property (p)) {
			return true;
		} else {
			n->add_property (p);
			return false;
		}
	}

} //namespace db

#endif //__GRAPH__
