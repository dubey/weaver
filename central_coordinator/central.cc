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

void*
create_edge (void *mem_addr1, void *mem_addr2, 
	central_coordinator::central *server)
{
	central_coordinator::graph_elem *elem;
	central_coordinator::graph_elem *elem1 = 
		(central_coordinator::graph_elem *) mem_addr1;
	central_coordinator::graph_elem *elem2 = 
		(central_coordinator::graph_elem *) mem_addr2;
	po6::net::location send_loc1 (elem1->loc1);
	po6::net::location send_loc2 (elem2->loc1);
	enum message::edge_direction dir = message::FIRST_TO_SECOND;
	message::message msg (message::EDGE_CREATE_REQ);
	busybee_returncode ret;

	msg.prep_edge_create ((size_t) elem1->mem_addr1, (size_t) elem2->mem_addr1,
		elem2->loc1, dir);
	if ((ret = server->bb.send (send_loc1, msg.buf)) != BUSYBEE_SUCCESS)
	{
		std::cerr << "msg send error: " << ret << std::endl;
		return NULL;
	}
	if ((ret = server->bb.recv (&send_loc1, &msg.buf)) != BUSYBEE_SUCCESS) 
	{
		std::cerr << "msg recv error: " << ret << std::endl;
		return NULL;
	}
	msg.unpack_create_ack (&mem_addr1);

	dir = message::SECOND_TO_FIRST;
	msg.change_type (message::EDGE_CREATE_REQ);
	msg.prep_edge_create ((size_t) elem2->mem_addr1, (size_t) elem1->mem_addr1, 
		elem1->loc1, dir);
	if ((ret = server->bb.send (send_loc2, msg.buf)) != BUSYBEE_SUCCESS) 
	{
		std::cerr << "msg send error: " << ret << std::endl;
		return NULL;
	}
	if ((ret = server->bb.recv (&send_loc2, &msg.buf)) != BUSYBEE_SUCCESS) 
	{
		std::cerr << "msg recv error: " << ret << std::endl;
		return NULL;
	}
	msg.unpack_create_ack (&mem_addr2);

	elem = new central_coordinator::graph_elem (elem1->loc1, elem2->loc1, 
		mem_addr1, mem_addr2);
	server->elements.push_back (elem);
			
	std::cout << "Edge id is " << (void *) elem << std::endl;
	return (void *) elem;
} //end create edge

void*
create_node (central_coordinator::central *server)
{
	po6::net::location *temp_loc;
	busybee_returncode ret;
	central_coordinator::graph_elem *elem;
	void *mem_addr1;
	message::message msg (message::NODE_CREATE_REQ);
	server->port_ctr = (server->port_ctr + 1) % (MAX_PORT+1);
	if (server->port_ctr == 0) 
	{
		server->port_ctr = CENTRAL_PORT+1;
	}
	
	temp_loc = new po6::net::location (LOCAL_IPADDR, server->port_ctr);
	msg.prep_node_create();
	ret = server->bb.send (*temp_loc, msg.buf);
	if (ret != BUSYBEE_SUCCESS) 
	{
		//error occurred
		std::cerr << "msg send error: " << ret << std::endl;
		return NULL;
	}
	if ((ret = server->bb.recv (temp_loc, &msg.buf)) != BUSYBEE_SUCCESS)		
	{
		//error occurred
		std::cerr << "msg recv error: " << ret << std::endl;
		return NULL;
	}
	//std::cout << "Got back confirmation from port " << temp_loc->port <<
	//	std::endl;
	temp_loc = new po6::net::location (LOCAL_IPADDR, server->port_ctr);
	msg.unpack_create_ack (&mem_addr1);
	elem = new central_coordinator::graph_elem (*temp_loc, *temp_loc, mem_addr1,
		mem_addr1);
	server->elements.push_back (elem);
	
	std::cout << "Node id is " << (void *) elem << " at port " << 
		elem->loc1.port << " " << elem->loc2.port << std::endl;
	return (void *) elem;
} //end create node

void
reachability_request (void *mem_addr1, void *mem_addr2,
	central_coordinator::central *server)
{
	static uint32_t req_counter = 0;

	uint32_t rec_counter;
	bool is_reachable;
	po6::net::location rec_loc (LOCAL_IPADDR, CENTRAL_PORT);
	central_coordinator::graph_elem *elem1 = 
		(central_coordinator::graph_elem *) mem_addr1;
	central_coordinator::graph_elem *elem2 = 
		(central_coordinator::graph_elem *) mem_addr2;
	message::message msg(message::REACHABLE_PROP);
	busybee_returncode ret;
	std::vector<size_t> src;

	req_counter++;
	src.push_back ((size_t)elem1->mem_addr1);
	std::cout << "Reachability request number " << req_counter << " from source"
		<< " node " << mem_addr1 << " to destination node " << mem_addr2
		<< std::endl;
	if (msg.prep_reachable_prop (src, (size_t) elem2->mem_addr1,
		elem2->loc1.port, req_counter) != 0) {
		std::cerr << "invalid msg packing" << std::endl;
		return;
	}
	if ((ret = server->bb.send (elem1->loc1, msg.buf)) != BUSYBEE_SUCCESS)
	{
		std::cerr << "msg send error: " << ret << std::endl;
		return;
	}
	if ((ret = server->bb.recv (&rec_loc, &msg.buf)) != BUSYBEE_SUCCESS) 
	{
		std::cerr << "msg recv error: " << ret << std::endl;
		return;
	}
	msg.unpack_reachable_rep (&rec_counter, &is_reachable);
	std::cout << "Reachable reply is " << is_reachable << " for " << 
		"request " << rec_counter << std::endl;
} //end reachability request

int
main (int argc, char* argv[])
{
	central_coordinator::central server;
	void *mem_addr1, *mem_addr2, *mem_addr3, *mem_addr4;
	
	mem_addr1 = create_node (&server);
	mem_addr2 = create_node (&server);
	mem_addr3 = create_node (&server);
	//mem_addr4 = create_node (&server);
	create_edge (mem_addr1, mem_addr2, &server);
	create_edge (mem_addr2, mem_addr3, &server);
	//create_edge (mem_addr3, mem_addr4, &server);
	reachability_request (mem_addr1, mem_addr3, &server);

	while (0) {
	uint32_t choice;
	std::cout << "Options:\n1. Create edge\n2. Create vertex\n3. Reachability"
		<< " request\n";
	std::cin >> choice;
	
	switch (choice)
	{
		case 1:
			std::cout << "Enter node 1" << std::endl;
			std::cin >> mem_addr1;
			std::cout << "Enter node 2" << std::endl;
			std::cin >> mem_addr2;
			create_edge (mem_addr1, mem_addr2, &server);
			break;

		case 2:
			create_node (&server);
			break;

		case 3:
			std::cout << "Enter node 1" << std::endl;
			std::cin >> mem_addr1;
			std::cout << "Enter node 2" << std::endl;
			std::cin >> mem_addr2;
			reachability_request (mem_addr1, mem_addr2, &server);
			break;

	} //end switch
	} //end while
} //end main
