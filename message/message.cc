/*
 * =====================================================================================
 *
 *       Filename:  message.cc
 *
 *    Description:  Message tester
 *
 *        Version:  1.0
 *        Created:  11/07/2012 03:49:13 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

#include "message.h"

int
main (int argc, char *argv[])
{
	po6::net::location random ("127.0.0.1", 5200);
	po6::net::location *retloc;
	void *first = (void *)238947328978763;
	void *second = (void *)23948230489224;
	uint16_t port = 42, src_port = 666;
	uint32_t dir;
	message::message msg (message::EDGE_CREATE_REQ);
	message::message msg2 (message::REACHABLE_REQ);
	message::message msg3 (message::REACHABLE_REPLY);
	message::message msg4 (message::REACHABLE_PROP);
	uint32_t temp;
	uint32_t r_count = 84;
	bool reachable;
	std::vector<size_t> src_nodes, srces;
	src_nodes.push_back ((size_t)first); src_nodes.push_back ((size_t)second);
	std::cout << "sizeof bool = " << sizeof (bool) << std::endl;
	int ret = msg.prep_edge_create ((size_t) first, (size_t) second, random,
		message::FIRST_TO_SECOND);
	int ret2 = msg2.prep_reachable_req ((size_t) first, (size_t) second, port,
		r_count);
	int ret3 = msg3.prep_reachable_rep (r_count, true);
	int ret4 = msg4.prep_reachable_prop (src_nodes, src_port, (size_t)second, port,
		r_count);
	std::cout << "Got " << ret << "  " << ret2 << " " << ret3 << std::endl;
	//std::cout << "Sent ipaddr " << random.address.get() << std::endl;

	ret = msg.unpack_edge_create (&first, &second, &retloc, &dir);
	std::cout << "Unpacking got " << ret << " and port number " <<
		retloc->port << " and dir " << dir << " and retloc " <<
		retloc /*<< " and addr " << retloc->address.get() */<< std::endl;
	std::cout << "First = " << first << " second " << second << std::endl;

	ret2 = msg2.unpack_reachable_req (&first, &second, &port, &r_count);
	std::cout << "\nUnpacking msg 2, got " << ret2 << " first " << first <<
		" second " << second << " port " << port << " rcnt " << r_count << std::endl;
	
	ret3 = msg3.unpack_reachable_rep (&temp, &reachable);
	std::cout << "\nUnpacking msg 3, got " << ret3 << " req counter " << temp <<
		" reachable " << reachable << std::endl; 
	reachable = true;
	temp = (uint32_t) reachable;
	std::cout << "testing true bools " << reachable << " as uint " << temp;
	reachable = false;
	temp = (uint32_t) reachable;
	std::cout << "\ntesting false bools " << reachable << " as uint " << temp <<
	'\n';

	srces = msg4.unpack_reachable_prop (&src_port, &second, &port, &r_count);
	std::cout << "\nUnpacking msg 4, got srces len " << srces.size() <<
		" second " << second << " src_port " << src_port << " to_port " << port << " r_count " << r_count <<
		'\n';
	for (temp = 0; temp < srces.size(); temp++)
	{
		std::cout << temp << " src is " << (void *) srces[temp] << std::endl;
	}
}
