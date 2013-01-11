/*
 * =====================================================================================
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
#include <string.h>

//e
#include <e/buffer.h>

//po6
#include <po6/net/location.h>

//Busybee
#include <busybee_constants.h>

//Weaver
#include "../db/element/property.h"

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
        REACHABLE_REPLY,
        //Between other servers
        REACHABLE_PROP
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
            message (message& copy);

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
            int prep_node_update (db::element::property p);
            int unpack_node_update (db::element::property **p);
            int prep_reachable_req (size_t from_node, size_t to_node, uint16_t
                to_port, uint32_t req_counter);
            int unpack_reachable_req (void **from_node, void **to_node, uint16_t
                *to_port, uint32_t *req_counter);
            //TODO: Add update functions
            //Other servers to central server
            int prep_node_create_ack (size_t mem_addr);
            int prep_edge_create_ack (size_t mem_addr);
            int prep_create_ack (size_t mem_addr);
            int unpack_create_ack (void **mem_addr);
            int prep_reachable_rep (uint32_t req_counter, 
                                    bool is_reachable,
                                    size_t src_node,
                                    uint16_t src_port);
            int unpack_reachable_rep (uint32_t *req_counter, 
                                      bool *is_reachable,
                                      size_t *src_node,
                                      uint16_t *src_port);
            //TODO: Add update functions
            int prep_reachable_prop (std::vector<size_t> src_nodes,
                                     uint16_t src_port,
                                     size_t to_node, 
                                     uint16_t to_port, 
                                     uint32_t req_counter,
                                     uint32_t prev_req_counter);
            std::vector<size_t> unpack_reachable_prop (uint16_t *src_port,
                                     void **to_node,
                                     uint16_t *to_port, 
                                     uint32_t *req_counter,
                                     uint32_t *prev_req_counter);
            int prep_error ();
    };

    inline
    message :: message (enum msg_type t)
        : type (t)
    {
    }

    inline 
    message :: message (message& copy)
        : type (copy.type)
    {
        buf = copy.buf;
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
        
        if (type != NODE_CREATE_REQ) 
        {
            return -1;
        }
        b = e::buffer::create (BUSYBEE_HEADER_SIZE + sizeof (enum msg_type));
        buf.reset (b);
        
        buf->pack_at (index) << type;
        return 0; 
    }

    inline int
    message :: prep_edge_create (size_t local_node, size_t remote_node,
                                po6::net::location remote_server, 
                                enum edge_direction dir)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        e::buffer *b;
        uint32_t port32;
        
        if (type != EDGE_CREATE_REQ) 
        {
            return -1;
        }
        b = e::buffer::create (BUSYBEE_HEADER_SIZE +
                               2 * sizeof (size_t) +
                               sizeof (enum msg_type) +
                               //sizeof (remote_server.address.get()) +
                               sizeof (uint32_t) +
                               sizeof (enum edge_direction));
        buf.reset (b);
        
        buf->pack_at (index) << type;
        index += sizeof (enum msg_type);
        port32 = (uint32_t) remote_server.port;
        buf->pack_at (index) << local_node << remote_node
                             /*<< remote_server.address.get() <<*/ 
                             << port32 << dir;
        return 0;
    }

    inline int
    message :: unpack_edge_create (void **local_node, void **remote_node, 
        po6::net::location **server, uint32_t *dir)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        uint32_t ip_addr, direction;
        uint32_t port;
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
            >> /*ip_addr >>*/ port >> direction;
        inaddr = (in_addr_t) ip_addr;
        addr = new po6::net::ipaddr (ip_addr);

        *local_node = (void *) mem_addr1;
        *remote_node = (void *) mem_addr2;
        *server = new po6::net::location (*addr, port);
        *dir = direction;
        return 0;
    }

    inline int
    message :: prep_node_update (db::element::property p)
    {
        /*
        uint32_t index = BUSYBEE_HEADER_SIZE;
        e::buffer *b;
        size_t ctr, len_key, len_value;

        if (type != NODE_UPDATE_REQ) {
            return -1;
        }
        len_key = strlen (p.key);
        len_value = strlen (p.value);
        b = e::buffer::create (BUSYBEE_HEADER_SIZE +
            sizeof (enum msg_type) +
            2 * sizeof (size_t) +
            len_key +
            len_value);
        buf.reset (b);

        buf->pack_at (index) << type;
        index += sizeof (enum msg_type);
        buf->pack_at (index) << len_key << len_value;
        index += 2 * sizeof (size_t);
        for (ctr = 0; ctr < len_key; ctr++, index++)
        {
            buf->pack_at (index) << p.key[ctr];
        }
        for (ctr = 0; ctr < len_value; ctr++, index++)
        {
            buf->pack_at (index) << p.value[ctr];
        }
        return 0;
        */
        return -1;
    }

    inline int
    message :: prep_reachable_req (size_t from_node, size_t to_node, uint16_t
        to_port, uint32_t req_counter)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        e::buffer *b;

        if (type != REACHABLE_REQ)
        {
            return -1;
        }
        b = e::buffer::create (BUSYBEE_HEADER_SIZE +
            sizeof (enum msg_type) +
            2 * sizeof (size_t) +
            sizeof (uint16_t) +
            sizeof (uint32_t));
        buf.reset (b);

        buf->pack_at (index) << type;
        index += sizeof (enum msg_type);
        buf->pack_at (index) << from_node << to_node;
        index += 2 * sizeof (size_t);
        buf->pack_at (index) << to_port << req_counter;
        return 0;
    }

    inline int
    message :: prep_reachable_rep (uint32_t req_counter, 
                                   bool is_reachable,
                                   size_t src_node,
                                   uint16_t src_port)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        e::buffer *b;
        uint32_t temp = (uint32_t) is_reachable;

        if (type != REACHABLE_REPLY) {
            return -1;
        }
        b = e::buffer::create (BUSYBEE_HEADER_SIZE +
            sizeof (enum msg_type) +
            sizeof (uint32_t) +
            sizeof (uint32_t) +
            sizeof (size_t) +
            sizeof (uint16_t));
        buf.reset (b);

        buf->pack_at (index) << type;
        index += sizeof (enum msg_type);
        buf->pack_at (index) << req_counter << temp << src_node << src_port;
        return 0;
    }

    inline int
    message :: unpack_node_update (db::element::property **p)
    {
        /*
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        char *key, *value;
        size_t ctr, len_key, len_value;

        buf->unpack_from (index) >> _type;
        if (_type != NODE_UPDATE_REQ)
        {
            return -1;
        }
        type = NODE_UPDATE_REQ;

        index += sizeof (enum msg_type);
        buf->unpack_from (index) >> len_key >> len_value;
        index += 2 * sizeof (size_t);
        key = (char *) malloc (len_key);
        value = (char *) malloc (len_value);
        for (ctr = 0; ctr < len_key; ctr++, index++)
        {
            buf->unpack_from (index) >> key[ctr];
        }
        for (ctr = 0; ctr < len_value; ctr++, index++)
        {
            buf->unpack_from (index) >> value[ctr];
        }
        *p = new db::element::property (key, value);
        return 0;
        */
        return -1;
    }

    inline int
    message :: unpack_reachable_req (void **from_node, void **to_node, uint16_t
        *to_port, uint32_t *req_counter)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t f_node, t_node;
        uint16_t t_port;
        uint32_t req_cnt;

        buf->unpack_from (index) >> _type;
        if (_type != REACHABLE_REQ)
        {
            return -1;
        }
        type = REACHABLE_REQ;

        index += sizeof (enum msg_type);
        buf->unpack_from (index) >> f_node >> t_node >> t_port >> req_cnt;
        *from_node = (void *) f_node;
        *to_node = (void *) t_node;
        *to_port = t_port;
        *req_counter = req_cnt;
        return 0;
    }

    inline int
    message :: unpack_reachable_rep (uint32_t *req_counter, 
                                     bool *is_reachable,
                                     size_t *src_node,
                                     uint16_t *src_port)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        uint32_t r_count;
        uint32_t reachable;
        size_t s_node;
        uint16_t s_port;

        buf->unpack_from (index) >> _type;
        if (_type != REACHABLE_REPLY) {
            return -1;
        }
        type = REACHABLE_REPLY;

        index += sizeof (enum msg_type);
        buf->unpack_from (index) >> r_count >> reachable >> s_node >> s_port;
        *req_counter = r_count;
        *is_reachable = (bool) reachable;
        *src_node = s_node;
        *src_port = s_port;
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

    inline int 
    message :: prep_reachable_prop (std::vector<size_t> src_nodes,
                                    uint16_t src_port,
                                    size_t to_node, 
                                    uint16_t to_port, 
                                    uint32_t req_counter,
                                    uint32_t prev_req_counter)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        e::buffer *b;
        size_t num_nodes = src_nodes.size();
        size_t i;

        if (type != REACHABLE_PROP)
        {
            return -1;
        }
        b = e::buffer::create (BUSYBEE_HEADER_SIZE +
                               sizeof (enum msg_type) +
                               sizeof (size_t) + //num_nodes
                               num_nodes * sizeof (size_t) + //src_nodes
                               sizeof (uint16_t) + //src_port
                               sizeof (size_t) + //to_node
                               sizeof (uint16_t) + //to_port
                               sizeof (uint32_t) +//req_counter
                               sizeof (uint32_t) //prev_req_counter
                               );
        buf.reset (b);

        buf->pack_at (index) << type;
        index += sizeof (enum msg_type);
        buf->pack_at (index) << num_nodes;
        for (i = 0, index += sizeof (size_t); i < num_nodes; i++, 
            index += sizeof (size_t))
        {
            buf->pack_at (index) << src_nodes[i];
        }
        buf->pack_at (index) << src_port << to_node << to_port << req_counter
                             << prev_req_counter;
        return 0;
    }

    inline std::vector<size_t>
    message :: unpack_reachable_prop (uint16_t *src_port, 
                                      void **to_node, 
                                      uint16_t *to_port, 
                                      uint32_t *req_counter,
                                      uint32_t *prev_req_counter)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        std::vector<size_t> src;
        size_t dest, num_nodes, i, temp;
        uint16_t from_port, dest_port;
        uint32_t r_count, p_r_count;

        buf->unpack_from (index) >> _type;
        if (_type != REACHABLE_PROP)
        {
            return src;
        }
        type = REACHABLE_PROP;
        index += sizeof (enum msg_type);

        buf->unpack_from (index) >> num_nodes;
        for (i = 0, index += sizeof (size_t); i < num_nodes; i++, index +=
             sizeof (size_t))
        {
            buf->unpack_from (index) >> temp;
            src.push_back (temp); 
        }
        buf->unpack_from (index) >> from_port >> dest >> dest_port 
                                 >> r_count >> p_r_count;

        *src_port = from_port;
        *to_node = (void *) dest;
        *to_port = dest_port;
        *req_counter = r_count;
        *prev_req_counter = p_r_count;
        return src;
    }

} //namespace message

#endif //__MESSAGE__
