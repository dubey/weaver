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
#include <thread>

//STL
#include <vector>
#include <unordered_map>

//po6
#include <po6/net/location.h>
#include <po6/threads/mutex.h>

//e
#include <e/buffer.h>

//Busybee
#include "busybee_constants.h"

//Weaver
#include "graph.h"
#include "../message/message.h"

#define IP_ADDR "127.0.0.1"
#define PORT_BASE 5200

class mytuple
{
	public:
		uint16_t port;
		uint32_t counter; //prev req id
		int num; //number of requests
		bool reachable;

		mytuple ()
		{
			port = 0;
			counter = 0;
			num = 0;
			reachable = false;
		}

		mytuple (uint16_t p, uint32_t c, int n)
			: port(p)
			, counter(c)
			, num(n)
			, reachable (false)
		{
		}

};

int order, port;
uint32_t batch_req_counter, local_req_counter;
po6::threads::mutex batch_req_counter_mutex, local_req_counter_mutex;
std::unordered_map<uint32_t, mytuple> outstanding_req; 
std::unordered_map<uint32_t, uint32_t> pending_batch;

void
handle_create_node (db::graph *G)
{
	db::element::node *n = G->create_node (0);
	message::message msg (message::NODE_CREATE_ACK);
	po6::net::location central (IP_ADDR, PORT_BASE);
	busybee_returncode ret;

	if (msg.prep_create_ack ((size_t) n) != 0) 
	{
		return;
	}
	G->bb_lock.lock();
	if ((ret = G->bb.send (central, msg.buf)) != BUSYBEE_SUCCESS) 
	{
		std::cerr << "msg send error: " << ret << std::endl;
		G->bb_lock.unlock();
		return;
	}
	G->bb_lock.unlock();
} //end create_node

void
handle_create_edge (db::graph *G, std::shared_ptr<message::message> msg)
{
	void *mem_addr1, *mem_addr2;
	po6::net::location *remote, *local;
	uint32_t direction;
	std::unique_ptr<db::element::meta_element> n1, n2;
	db::element::edge *e;
	po6::net::location central (IP_ADDR, PORT_BASE);
	busybee_returncode ret;

	if (msg->unpack_edge_create (&mem_addr1, &mem_addr2, &remote, &direction) != 0)
	{
		return;
	}
	local = new po6::net::location (IP_ADDR, port);
	n1.reset (new db::element::meta_element (*local, 0, UINT_MAX,
											 mem_addr1));
	n2.reset (new db::element::meta_element (*remote, 0, UINT_MAX,
											 mem_addr2));
	
	e = G->create_edge (std::move(n1), std::move(n2), (uint32_t) direction, 0);
	msg->change_type (message::EDGE_CREATE_ACK);
	if (msg->prep_create_ack ((size_t) e) != 0) 
	{
		return;
	}
	G->bb_lock.lock();
	if ((ret = G->bb.send (central, msg->buf)) != BUSYBEE_SUCCESS) 
	{
		std::cerr << "msg send error: " << ret << std::endl;
		G->bb_lock.unlock();
		return;
	}
	G->bb_lock.unlock();
	delete remote;
	delete local;
} //end create_edge

void
handle_reachable_request (db::graph *G, std::shared_ptr<message::message> msg)
{
	void *mem_addr2;
	db::element::node *n;
	busybee_returncode ret;
	std::unique_ptr<po6::net::location> remote;
	uint16_t from_port, //previous node's port
			 to_port; //target node's port
	uint32_t req_counter, //central server req counter
			 prev_req_counter, //previous node req counter
			 my_batch_req_counter, //this request's number
			 my_local_req_counter; //each forward batched req number
	bool reached = false;
	bool send_msg = false;
	std::unordered_map<uint16_t, std::vector<size_t>> msg_batch;
	std::vector<size_t> src_nodes;
	std::vector<size_t>::iterator src_iter;

	src_nodes = msg->unpack_reachable_prop (&from_port, 
										    &mem_addr2, 
										    &to_port,
										    &req_counter, 
										    &prev_req_counter);
	//new request received, assigning number
	batch_req_counter_mutex.lock();
	my_batch_req_counter = batch_req_counter++;
	batch_req_counter_mutex.unlock();

	for (src_iter = src_nodes.begin(); src_iter < src_nodes.end();
		 src_iter++)
	{
	static int node_ctr = 0;
	//no error checking needed here
	n = (db::element::node *) (*src_iter);
	//TODO mem leak! Remove old properties
	if (!G->mark_visited (n, req_counter))
	{
		std::vector<db::element::meta_element>::iterator iter;
		for (iter = n->out_edges.begin(); iter < n->out_edges.end();
			 iter++)
		{
			db::element::edge *nbr = (db::element::edge *)
									  iter->get_addr();
			send_msg = true;
			if (nbr->to.get_addr() == mem_addr2 &&
				nbr->to.get_port() == to_port)
			{	//Done! Send msg back to central server
				reached = true;
				break;
			} else
			{ 	//Continue propagating reachability request
				msg_batch[nbr->to.get_port()].push_back
					((size_t)nbr->to.get_addr());
			}
		}
	} //end if visited
	if (reached)
	{
		break;
	}
	} //end src_nodes loop
	
	//send messages
	if (reached)
	{ 	//need to send back ack
		msg->change_type (message::REACHABLE_REPLY);
		msg->prep_reachable_rep (prev_req_counter, true);
		remote.reset (new po6::net::location (IP_ADDR, from_port));
		G->bb_lock.lock();
		if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
		{
			std::cerr << "msg send error: " << ret << std::endl;
		}	
		G->bb_lock.unlock();
	} else if (send_msg)
	{ 	//need to send batched msges onwards
		std::unordered_map<uint16_t, std::vector<size_t>>::iterator loc_iter;
		//no need for mutex for outstanding_req
		//as my_batch_req_counter is unique
		outstanding_req[my_batch_req_counter].port = from_port;
		outstanding_req[my_batch_req_counter].counter = prev_req_counter;
		outstanding_req[my_batch_req_counter].num = 0;
		for (loc_iter = msg_batch.begin(); loc_iter !=
			 msg_batch.end(); loc_iter++)
		{
			msg->change_type (message::REACHABLE_PROP);
			//new batched request
			local_req_counter_mutex.lock();
			my_local_req_counter = local_req_counter++;
			local_req_counter_mutex.unlock();
			msg->prep_reachable_prop (loc_iter->second,
									  G->myloc.port, 
									  (size_t)mem_addr2, 
									  to_port,
									  req_counter,
									  (my_local_req_counter));
			if (loc_iter->first == G->myloc.port)
			{	//no need to send message since it is local
				std::thread t (handle_reachable_request, G, msg);
				t.detach();
			} else
			{
				static int loop = 0;
				remote.reset (new po6::net::location (IP_ADDR, loc_iter->first));
				G->bb_lock.lock();
				if ((ret = G->bb.send (*remote, msg->buf)) !=
					BUSYBEE_SUCCESS)
				{
					std::cerr << "msg send error: " << ret <<
					std::endl;
				}
				G->bb_lock.unlock();
				//adding this as a pending request
				outstanding_req[my_batch_req_counter].num++;
				pending_batch[my_local_req_counter] = my_batch_req_counter;
			}
			//adding this as a pending request
			outstanding_req[my_batch_req_counter].num++;
			pending_batch[my_local_req_counter] = my_batch_req_counter;
		}
		if (outstanding_req[my_batch_req_counter].num == 0)
		{	//all messages were local, clean this up
			outstanding_req.erase (my_batch_req_counter);
		}
		msg_batch.clear();	
	} else
	{ 	//need to send back nack
		msg->change_type (message::REACHABLE_REPLY);
		msg->prep_reachable_rep (prev_req_counter, false);
		remote.reset (new po6::net::location (IP_ADDR, from_port));
		G->bb_lock.lock();
		if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
		{
			std::cerr << "msg send error: " << ret << std::endl;
		}
		G->bb_lock.unlock();
	}
} //end reachable_request

void
handle_reachable_reply (db::graph *G, std::shared_ptr<message::message> msg)
{
	busybee_returncode ret;
	uint32_t my_local_req_counter, my_batch_req_counter, prev_req_counter;
	bool reachable_reply;
	uint16_t from_port;
	std::unique_ptr<po6::net::location> remote;

	msg->unpack_reachable_rep (&my_local_req_counter, &reachable_reply);
	my_batch_req_counter = pending_batch[my_local_req_counter];
	pending_batch.erase (my_local_req_counter);
	--outstanding_req[my_batch_req_counter].num;
	from_port = outstanding_req[my_batch_req_counter].port;
	prev_req_counter = outstanding_req[my_batch_req_counter].counter;
	/*
	 * check if this is the last expected reply for this batched request
	 * and we got all negative replies till now
	 * or this is a positive reachable reply
	 */
	if (((outstanding_req[my_batch_req_counter].num == 0) || reachable_reply)
		&& !outstanding_req[my_batch_req_counter].reachable)
	{
		outstanding_req[my_batch_req_counter].reachable |= reachable_reply;
		msg->prep_reachable_rep (prev_req_counter, reachable_reply);
		remote.reset (new po6::net::location (IP_ADDR, from_port));
		G->bb_lock.lock();
		if ((ret = G->bb.send (*remote, msg->buf)) != BUSYBEE_SUCCESS)
		{
			std::cerr << "msg send error: " << ret << std::endl;
		}
		G->bb_lock.unlock();
	}
	if (outstanding_req[my_batch_req_counter].num == 0)
	{
		outstanding_req.erase (my_batch_req_counter);
	}
} //end reachable_reply

void
runner (db::graph *G)
{
	busybee_returncode ret;
	po6::net::location sender (IP_ADDR, PORT_BASE);
	message::message msg (message::ERROR);
	uint32_t code;
	enum message::msg_type mtype;
	std::shared_ptr<message::message> rec_msg;
	std::unique_ptr<std::thread> t;

	uint32_t loop_count = 0;
	while (1)
	{
		//G->bb_lock.lock();
		if ((ret = G->bb_recv.recv (&sender, &msg.buf)) != BUSYBEE_SUCCESS)
		{
			std::cerr << "msg recv error: " << ret << std::endl;
			//G->bb_lock.unlock();
			continue;
		}
		//G->bb_lock.unlock();
		rec_msg.reset (new message::message (msg));
		rec_msg->buf->unpack_from (BUSYBEE_HEADER_SIZE) >> code;
		mtype = (enum message::msg_type) code;
		switch (mtype)
		{
			case message::NODE_CREATE_REQ:
				t.reset (new std::thread (handle_create_node, G));
				t->detach();
				break;

			case message::EDGE_CREATE_REQ:
				t.reset (new std::thread (handle_create_edge, G, rec_msg));
				t->detach();
				break;
			
			case message::REACHABLE_PROP:
				t.reset (new std::thread (handle_reachable_request, G, rec_msg));
				t->detach();
				break;

			case message::REACHABLE_REPLY:
				t.reset (new std::thread (handle_reachable_reply, G, rec_msg));
				t->detach();
				break;

			default:
				std::cerr << "unexpected msg type " << code << std::endl;
		
		} //end switch

	} //end while

}// end runner

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
	local_req_counter = 0;
	batch_req_counter = 0;

	db::graph G (IP_ADDR, port);
	
	runner (&G);

	return 0;
} //end main
