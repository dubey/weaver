/*
 * =====================================================================================
 *
 *       Filename:  graph.cc
 *
 *    Description:  Graph BusyBee loop for each server
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

//C
#include <cstdlib>

//C++
#include <iostream>

//STL
#include <vector>

//po6
#include <po6/net/location.h>

//e
#include <e/buffer.h>

//Busybee
#include "busybee_constants.h"

//Weaver
#include "graph.h"
#include "../message/message.h"

#define IP_ADDR "127.0.0.1"
#define PORT_BASE 5200

int order, port;

void
runner (db::graph* G)
{
	busybee_returncode ret;
	int sent = 0;
	po6::net::location central (IP_ADDR, PORT_BASE);
	message::message msg (message::ERROR);

	db::element::node *n;
	db::element::edge *e;
	db::element::meta_element *n1, *n2;
	void *mem_addr1, *mem_addr2;
	po6::net::location *local, *remote;
	uint32_t direction;

	uint32_t code;
	enum message::msg_type mtype;

	uint32_t loop_count = 0;
	while (1)
	{
		std::cout << "While loop " << (++loop_count) << std::endl;
		if ((ret = G->bb.recv (&central, &msg.buf)) != BUSYBEE_SUCCESS)
		{
			std::cerr << "msg recv error: " << ret << std::endl;
			continue;
		}
		msg.buf->unpack_from (BUSYBEE_HEADER_SIZE) >> code;
		mtype = (enum message::msg_type) code;
		switch (mtype)
		{
			case message::NODE_CREATE_REQ:
				n = G->create_node (0);
				msg.change_type (message::NODE_CREATE_ACK);
				if (msg.prep_create_ack ((size_t) n) != 0) 
				{
					continue;
				}
				if ((ret = G->bb.send (central, msg.buf)) != BUSYBEE_SUCCESS) 
				{
					std::cerr << "msg send error: " << ret << std::endl;
					continue;
				}
				break;

			case message::EDGE_CREATE_REQ:
				if (msg.unpack_edge_create (&mem_addr1, &mem_addr2, &remote, &direction) != 0)
				{
					continue;
				}
				local = new po6::net::location (IP_ADDR, port);
				n1 = new db::element::meta_element (*local, 0, UINT_MAX,
					mem_addr1);
				n2 = new db::element::meta_element (*remote, 0, UINT_MAX,
					mem_addr2);
				
				e = G->create_edge (n1, n2, (uint32_t) direction, 0);
				msg.change_type (message::EDGE_CREATE_ACK);
				if (msg.prep_create_ack ((size_t) e) != 0) 
				{
					continue;
				}
				if ((ret = G->bb.send (central, msg.buf)) != BUSYBEE_SUCCESS) 
				{
					std::cerr << "msg send error: " << ret << std::endl;
					continue;
				}
				break;
				
			default:
				std::cerr << "unexpected msg type " << code << std::endl;
		
		} //end switch

	} //end while

}

int
main (int argc, char* argv[])
{
	if (argc != 2) 
	{
		std::cerr << "Usage: " << argv[0] << " <order> " << std::endl;
		return -1;
	}

	std::cout << "Testing GraphDB" << std::endl;
	
	order = atoi (argv[1]);
	port = PORT_BASE + order;

	db::graph G (IP_ADDR, port);
	
	runner (&G);

	return 0;
}
