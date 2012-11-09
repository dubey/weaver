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
	void *first = (void *)0x12345678;
	void *second = (void *)0x87654321;
	uint32_t dir;
	message::message msg (message::EDGE_CREATE_REQ);
	uint32_t temp;
	int ret = msg.prep_edge_create ((size_t) first, (size_t) second, random,
		message::FIRST_TO_SECOND);
	
	std::cout << "Got " << ret << std::endl;
	std::cout << "Sent ipaddr " << random.address.get() << std::endl;

	ret = msg.unpack_edge_create (&first, &second, &retloc, &dir);
	std::cout << "Unpacking got " << ret << " and port number " <<
		retloc->port << " and dir " << dir << " and retloc " <<
		retloc << " and addr " << retloc->address.get() << std::endl;
	std::cout << "First = " << first << " second " << second << std::endl;
}
