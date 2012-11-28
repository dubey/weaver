/*
 * =====================================================================================
 *
 *       Filename:  central.h
 *
 *    Description:  Central server responsible for handling all queries
 *
 *        Version:  1.0
 *        Created:  10/27/2012 05:20:01 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

#ifndef __CENTRAL__
#define __CENTRAL__

//C++
#include <vector>

//Busybee
#include <busybee_sta.h>

//po6
#include <po6/net/location.h>

//Weaver
#include "graph_elem.h"

//Constants
#define CENTRAL_PORT 5200
#define LOCAL_IPADDR "127.0.0.1"
/* This also defines the number of physical servers
 * = MAX_PORT - CENTRAL_PORT
 */
#define MAX_PORT 5203

namespace central_coordinator
{
	class central
	{
		public:
			central ();
		private:
			po6::net::location myloc;

		public:
			busybee_sta bb;
			uint32_t time;
			//A list of graph elements, which would probably be some server,
			//id/address pair
			int port_ctr;
			std::vector<central_coordinator::graph_elem *> elements;
	};

	inline
	central :: central ()
		: myloc (LOCAL_IPADDR, CENTRAL_PORT)
		, bb (myloc.address, myloc.port, 0)
	{
		time = 0;
		port_ctr = CENTRAL_PORT;
	}

} //namespace central_coordinator

#endif // __CENTRAL__
