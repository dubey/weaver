/*
 * =====================================================================================
 *
 *       Filename:  db_tester.cc
 *
 *    Description:  Testing GraphDB
 *
 *        Version:  1.0
 *        Created:  Tuesday 16 October 2012 03:03:11  EDT
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

//C++
#include <iostream>

//STL
#include <vector>

//GraphDB
#include "graph.h"
#include "element/node.h"
#include "element/edge.h"
#include "element/property.h"

int
main (int argc, char *argv[])
{
	std::cout << "Testing GraphDB" << std::endl;
	
	db::graph G;
	//Creating 2 nodes
	db::element::node n1;
	db::element::node n2;
	G.add_node (&n1);
	G.add_node (&n2);
	std::cout << "Node " << (void *)&n1 << std::endl;
	std::cout << "Node " << (void *)&n2 << std::endl;
	
	//Creating 1 edge
	db::element::edge *e = G.add_edge (&n1, &n2);

	//Adding some properties
	db::element::property p1 ((char *) "name", (char *) "ad");
	db::element::property p2 ((char *) "name", (char *) "srb");
	n1.add_property (p1);
	n2.add_property (p2);
	db::element::property p_edge ((char *) "relation", (char *) "friends");
	e->add_property (p_edge);
	
	//Finding all reachable vertices from n1
	std::vector<db::element::node *> nodes = G.transclosure (&n1);
	std::vector<db::element::node *>::iterator node_iter;
	std::cout << "Nodes reachable from node " << (void *)&n1 << " are:" << std::endl;
	for (node_iter = nodes.begin(); node_iter<nodes.end(); node_iter++)
	{
		std::cout << (void *)(*node_iter) << std::endl;
	}
	return 0;
}
