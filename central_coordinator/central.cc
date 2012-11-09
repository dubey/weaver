/*
 * =====================================================================================
 *
 *       Filename:  central.cc
 *
 *    Description:  Central server test run
 *
 *        Version:  1.0
 *        Created:  11/06/2012 11:47:04 AM
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

//e
#include "e/buffer.h"

//Busybee
#include "busybee_constants.h"

//Weaver
#include "central.h"
#include "../message/message.h"

int
main (int argc, char* argv[])
{
	central_coordinator::central server;
	void *mem_addr1, *mem_addr2;
	enum message::edge_direction dir;
	central_coordinator::graph_elem *elem, *elem1, *elem2;
	po6::net::location *temp_loc;
	message::message msg (message::ERROR);
	busybee_returncode ret;
	
	while (1) {
	uint32_t choice;
	std::cout << "Options:\n1. Create edge\n2. Create vertex\n";
	std::cin >> choice;
	
	switch (choice)
	{
		case 1:
			std::cout << "Enter node 1" << std::endl;
			std::cin >> mem_addr1;
			std::cout << "Enter node 2" << std::endl;
			std::cin >> mem_addr2;
			elem1 = (central_coordinator::graph_elem *) mem_addr1;
			elem2 = (central_coordinator::graph_elem *) mem_addr2;
			
			dir = message::FIRST_TO_SECOND;
			msg.change_type (message::EDGE_CREATE_REQ);
			msg.prep_edge_create ((size_t) elem1->mem_addr1, (size_t) elem2->mem_addr1,
				elem2->loc1, dir);
			if ((ret = server.bb.send (elem1->loc1, msg.buf)) != BUSYBEE_SUCCESS)
			{
				std::cerr << "msg send error: " << ret << std::endl;
				continue;
			}
			if ((ret = server.bb.recv (&elem1->loc1, &msg.buf)) != BUSYBEE_SUCCESS) 
			{
				std::cerr << "msg recv error: " << ret << std::endl;
				continue;
			}
			msg.unpack_create_ack (&mem_addr1);

			dir = message::SECOND_TO_FIRST;
			msg.change_type (message::EDGE_CREATE_REQ);
			msg.prep_edge_create ((size_t) elem2->mem_addr1, (size_t) elem1->mem_addr1, 
				elem1->loc1, dir);
			if ((ret = server.bb.send (elem2->loc1, msg.buf)) != BUSYBEE_SUCCESS) 
			{
				std::cerr << "msg send error: " << ret << std::endl;
				continue;
			}
			if ((ret = server.bb.recv (&elem2->loc1, &msg.buf)) != BUSYBEE_SUCCESS) 
			{
				std::cerr << "msg recv error: " << ret << std::endl;
				continue;
			}
			msg.unpack_create_ack (&mem_addr2);

			elem = new central_coordinator::graph_elem (elem1->loc1, elem2->loc1, 
				mem_addr1, mem_addr2);
			server.elements.push_back (elem);
			
			std::cout << "Edge id is " << (void *) elem << std::endl;
			break;

		case 2:
			server.port_ctr = (server.port_ctr + 1) % (MAX_PORT+1);
			if (server.port_ctr == 0) 
			{
				server.port_ctr = CENTRAL_PORT;
			}
	
			temp_loc = new po6::net::location (LOCAL_IPADDR, server.port_ctr);
			msg.change_type (message::NODE_CREATE_REQ);
			msg.prep_node_create();
			ret = server.bb.send (*temp_loc, msg.buf);
			if (ret != BUSYBEE_SUCCESS) 
			{
				//error occurred
				std::cerr << "msg send error: " << ret << std::endl;
				continue;
			}
			if ((ret = server.bb.recv (temp_loc, &msg.buf)) != BUSYBEE_SUCCESS)
			{
				//error occurred
				std::cerr << "msg recv error: " << ret << std::endl;
				continue;
			}
			msg.unpack_create_ack (&mem_addr1);
			elem = new central_coordinator::graph_elem (*temp_loc, *temp_loc, mem_addr1,
				mem_addr1);
			server.elements.push_back (elem);

			std::cout << "Node id is " << (void *) elem << std::endl;
			break;

	} //end switch
	} //end while
}
