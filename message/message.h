/*
 * =====================================================================================
 *
 *       Filename:  message.h
 *
 *    Description:  Inter-server message class
 *
 *        Version:  1.0
 *        Created:  11/07/2012 01:40:52 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

#ifndef __MESSAGE__
#define __MESSAGE__

//C++
#include <memory>

//e
#include <e/buffer.h>

//po6
#include <po6/net/location.h>

//Busybee
#include <busybee_constants.h>

namespace message
{
	enum msg_type
	{
		//Central server to other servers
		NODE_CREATE_REQ,
		EDGE_CREATE_REQ,
		NODE_UPDATE_REQ,
		EDGE_UPDATE_REQ,
		REACHABLE_REQ,
		//Other servers to central server
		NODE_CREATE_ACK,
		EDGE_CREATE_ACK,
		NODE_UPDATE_ACK,
		EDGE_UPDATE_ACK,
		ERROR,
		REACHABLE_REPLY
	};

	enum edge_direction
	{
		FIRST_TO_SECOND = 0,
		SECOND_TO_FIRST = 1
	};

	class message
	{
		public:
			message (enum msg_type t);

		public:
			enum msg_type type;
			std::auto_ptr<e::buffer> buf;

		public:
			//Central server to other servers
			void change_type (enum msg_type t);
			int prep_node_create ();
			int prep_edge_create (size_t local_node, size_t remote_node,
				po6::net::location remote_server, enum edge_direction dir);
			int unpack_edge_create (void **local_node, void **remote_node, 
				po6::net::location **server, uint32_t *dir);
			//TODO: Add update and reachability functions
			//Other servers to central server
			int prep_node_create_ack (size_t mem_addr);
			int prep_edge_create_ack (size_t mem_addr);
			int prep_create_ack (size_t mem_addr);
			int unpack_create_ack (void **mem_addr);
			//TODO: Add update and reachability functions
			int prep_error ();
	};

	inline
	message :: message (enum msg_type t)
		: type (t)
	{
	}

	inline void
	message :: change_type (enum msg_type t)
	{
		type = t;
	}

	inline int
	message :: prep_node_create ()
	{
		uint32_t index = BUSYBEE_HEADER_SIZE;
		e::buffer *b;
		
		if (type != NODE_CREATE_REQ) {
			return -1;
		}
		b = e::buffer::create (BUSYBEE_HEADER_SIZE + sizeof (enum msg_type));
		buf.reset (b);
		
		buf->pack_at (index) << type;
		return 0; 
	}

	inline int
	message :: prep_edge_create (size_t local_node, size_t remote_node,
		po6::net::location remote_server, enum edge_direction dir)
	{
		uint32_t index = BUSYBEE_HEADER_SIZE;
		e::buffer *b;
		
		if (type != EDGE_CREATE_REQ) {
			return -1;
		}
		b = e::buffer::create (BUSYBEE_HEADER_SIZE +
			2 * sizeof (size_t) +
			sizeof (enum msg_type) +
			sizeof (remote_server.address.get()) +
			sizeof (remote_server.port) +
			sizeof (enum edge_direction));
		buf.reset (b);
		
		buf->pack_at (index) << type;
		index += sizeof (enum msg_type);
		buf->pack_at (index) << local_node << remote_node
			<< remote_server.address.get() << remote_server.port << dir;
		return 0;
	}

	inline int
	message :: unpack_edge_create (void **local_node, void **remote_node, 
		po6::net::location **server, uint32_t *dir)
	{
		uint32_t index = BUSYBEE_HEADER_SIZE;
		uint32_t _type;
		uint32_t ip_addr, direction;
		uint16_t port;
		size_t mem_addr1, mem_addr2;
		in_addr_t inaddr;
		po6::net::ipaddr *addr;
		
		buf->unpack_from (index) >> _type;
		if (_type != EDGE_CREATE_REQ) 
		{
			return -1;
		}
		type = EDGE_CREATE_REQ;
		
		index += sizeof (enum msg_type);
		buf->unpack_from (index) >> mem_addr1 >> mem_addr2
			>> ip_addr >> port >> direction;
		inaddr = (in_addr_t) ip_addr;
		addr = new po6::net::ipaddr (ip_addr);

		*local_node = (void *) mem_addr1;
		*remote_node = (void *) mem_addr2;
		*server = new po6::net::location (*addr, port);
		*dir = direction;
		return 0;
	}

	inline int
	message :: prep_create_ack (size_t mem_addr)
	{
		uint32_t index = BUSYBEE_HEADER_SIZE;
		e::buffer *b;

		if ((type != NODE_CREATE_ACK) && (type != EDGE_CREATE_ACK))
		{
			return -1;
		}
		b = e::buffer::create (BUSYBEE_HEADER_SIZE + 
			sizeof (enum msg_type) + 
			sizeof (size_t));
		buf.reset (b);

		buf->pack_at (index) << type;
		index += sizeof (enum msg_type);
		buf->pack_at (index) << mem_addr;
		return 0; 
	}

	inline int
	message :: unpack_create_ack (void **mem_addr)
	{
		uint32_t index = BUSYBEE_HEADER_SIZE;
		uint32_t _type;
		size_t addr;
		buf->unpack_from (index) >> _type;
		if ((_type != NODE_CREATE_ACK) && (_type != EDGE_CREATE_ACK))
		{
			return -1;
		}
		type = (enum msg_type) _type;
		index += sizeof (uint32_t);

		buf->unpack_from (index) >> addr;
		*mem_addr = (void *) addr;
		return 0;
	}

} //namespace message

#endif //__MESSAGE__
