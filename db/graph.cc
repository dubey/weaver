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
	po6::net::location sender (IP_ADDR, PORT_BASE);
	message::message msg (message::ERROR);

	db::element::node *n;
	db::element::edge *e;
	db::element::meta_element *n1, *n2;
	void *mem_addr1, *mem_addr2;
	po6::net::location *local, *remote;
	uint32_t direction;
	uint16_t to_port;
	uint32_t req_counter;

	po6::net::location reach_requests[100];
	uint32_t local_req_counter = 0;

	uint32_t code;
	enum message::msg_type mtype;

	uint32_t loop_count = 0;
	while (1)
	{
		//std::cout << "While loop " << (++loop_count) << std::endl;
		if ((ret = G->bb.recv (&sender, &msg.buf)) != BUSYBEE_SUCCESS)
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

			case message::REACHABLE_REQ:
				if (msg.unpack_reachable_req (&mem_addr1, &mem_addr2, &to_port, 
					&req_counter) != 0) {
					continue;
				}
				//no error checking needed here
				n = (db::element::node *) mem_addr1;
				if (G->mark_visited (n, req_counter))
				{
					std::vector<db::element::meta_element>::iterator iter;
					bool reached = false;
					for (iter = n->out_edges.begin(); iter < n->out_edges.end();
						iter++)
					{
						db::element::edge *nbr = (db::element::edge *)
							iter->get_addr();
						if (nbr->to.get_addr() == mem_addr2 &&
							nbr->to.get_port() == to_port)
						{ //Done! Send msg back to central server
							msg.change_type (message::REACHABLE_REPLY);
							msg.prep_reachable_rep (req_counter, true);
							remote = &central;
							reached = true;
						} else
						{ //Continue propagating reachability request
							msg.prep_reachable_req ((size_t)nbr->to.get_addr(),
								(size_t) mem_addr2, to_port, req_counter);
							remote = new po6::net::location (IP_ADDR,
								nbr->to.get_port());
						}

						if ((ret = G->bb.send (*remote, msg.buf)) !=
								BUSYBEE_SUCCESS) {
							std::cerr << "msg send error: " << ret << std::endl;
						} else {
							if (reached) {
								break;
							}
						}
					}
				} //else TODO
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

	std::cout << "Testing Weaver" << std::endl;
	
	order = atoi (argv[1]);
	port = PORT_BASE + order;

	db::graph G (IP_ADDR, port);
	
	runner (&G);

	return 0;
}
