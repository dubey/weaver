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

//C
#include <unistd.h>

//C++
#include <iostream>
#include <time.h>

//e
#include "e/buffer.h"

//Busybee
#include "busybee_constants.h"

//Weaver
#include "central.h"
#include "../message/message.h"

#define NUM_NODES 100000
#define NUM_EDGES 150000
#define NUM_REQUESTS 100000

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
			
	//std::cout << "Edge id is " << (void *) elem << std::endl;
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
	static int node_cnt = 0;
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
	temp_loc = new po6::net::location (LOCAL_IPADDR, server->port_ctr);
	msg.unpack_create_ack (&mem_addr1);
	elem = new central_coordinator::graph_elem (*temp_loc, *temp_loc, mem_addr1,
												mem_addr1);
	server->elements.push_back (elem);
	
	//std::cout << "Node id is " << (void *) elem << " at port " 
	//		  << elem->loc1.port << " " << (node_cnt++) << std::endl;
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
	size_t src_node;
	uint16_t src_port;

	req_counter++;
	src.push_back ((size_t)elem1->mem_addr1);
	std::cout << "Reachability request number " << req_counter << " from source"
			  << " node " << mem_addr1 << " to destination node " << mem_addr2
			  << std::endl;
	if (msg.prep_reachable_prop (src, CENTRAL_PORT, (size_t) elem2->mem_addr1,
								 elem2->loc1.port, req_counter, req_counter) 
		!= 0) 
	{
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
	msg.unpack_reachable_rep (&rec_counter, &is_reachable, &src_node, &src_port);
	std::cout << "Reachable reply is " << is_reachable << " for " << 
		"request " << rec_counter << std::endl;
} //end reachability request

timespec
diff (timespec start, timespec end)
{
		timespec temp;
		if ((end.tv_nsec-start.tv_nsec)<0) {
			temp.tv_sec = end.tv_sec-start.tv_sec-1;
			temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
		} else {
			temp.tv_sec = end.tv_sec-start.tv_sec;
			temp.tv_nsec = end.tv_nsec-start.tv_nsec;
		}
		return temp;
}

int
main (int argc, char* argv[])
{
	central_coordinator::central server;
	void *mem_addr1, *mem_addr2, *mem_addr3, *mem_addr4;
	int i;
	std::vector<void *> nodes;
	timespec start, end, time_taken;
	uint32_t time_ms;
	
	for (i = 0; i < NUM_NODES; i++)
	{
		nodes.push_back (create_node (&server));
	}
	srand (time (NULL));
	for (i = 0; i < NUM_EDGES; i++)
	{
		int first = rand() % NUM_NODES;
		int second = rand() % NUM_NODES;
		while (second == first) //no self-loop edges
		{
			second = rand() % NUM_NODES;
		}
		create_edge (nodes[first], nodes[second], &server);
	}
	//clock_gettime (CLOCK_PROCESS_CPUTIME_ID, &start);	
	clock_gettime (CLOCK_MONOTONIC, &start);	
	for (i = 0; i < NUM_REQUESTS; i++)
	{
		reachability_request (nodes[rand() % NUM_NODES], nodes[rand() %
							  NUM_NODES], &server);
	}
	clock_gettime (CLOCK_MONOTONIC, &end);	
	//clock_gettime (CLOCK_PROCESS_CPUTIME_ID, &end);	

	time_taken = diff (start, end);
	time_ms = time_taken.tv_sec * 1000 + time_taken.tv_nsec/1000000;
	std::cout << "Time = " << time_ms << std::endl;

	std::cin >> i;

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
