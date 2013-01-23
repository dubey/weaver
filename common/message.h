/*
 * ================================================================
 *    Description:  Inter-server message packing and unpacking
 *
 *        Created:  11/07/2012 01:40:52 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __MESSAGE__
#define __MESSAGE__

#include <memory>
#include <string.h>
#include <e/buffer.h>
#include <po6/net/location.h>
#include <busybee_constants.h>

#include "common/weaver_constants.h"
#include "db/element/property.h"
#include "common/meta_element.h"
#include "common/vclock.h"

namespace message
{
    enum msg_type
    {
        CLIENT_NODE_CREATE_REQ = 0,
        CLIENT_EDGE_CREATE_REQ,
        CLIENT_NODE_DELETE_REQ,
        CLIENT_EDGE_DELETE_REQ,
        CLIENT_REACHABLE_REQ,
        CLIENT_REQUEST,
        CLIENT_REPLY,
        NODE_CREATE_REQ,
        EDGE_CREATE_REQ,
        NODE_CREATE_ACK,
        EDGE_CREATE_ACK,
        NODE_DELETE_REQ,
        EDGE_DELETE_REQ,
        NODE_DELETE_ACK,
        EDGE_DELETE_ACK,
        REACHABLE_REPLY,
        REACHABLE_PROP,
        ERROR
    };

    enum edge_direction
    {
        FIRST_TO_SECOND = 0,
        SECOND_TO_FIRST = 1
    };

    class message
    {
        public:
            message(enum msg_type t);
            message(message& copy);

        public:
            enum msg_type type;
            std::auto_ptr<e::buffer> buf;

        public:
            // Create functions
            void change_type(enum msg_type t);
            void prep_node_create(size_t req_id, uint64_t creat_time);
            void unpack_node_create(size_t *req_id, uint64_t *creat_time);
            void prep_edge_create(size_t req_id, size_t local_node, size_t remote_node, 
                std::shared_ptr<po6::net::location> remote_server,
                uint64_t remote_node_creat_time, uint64_t edge_creat_time);
            void unpack_edge_create(size_t *req_id, void **local_node,
                std::unique_ptr<common::meta_element> *remote_node,
                uint64_t *edge_creat_time);
            void prep_node_create_ack(size_t req_id, size_t mem_addr);
            void prep_edge_create_ack(size_t req_id, size_t mem_addr);
            void unpack_create_ack(size_t *req_id, void **mem_addr);
            // Update functions
            void prep_node_delete(size_t req_id, size_t node_handle, uint64_t del_time);
            void unpack_node_delete(size_t *req_id, void **node_handle, uint64_t *del_time);
            void prep_edge_delete(size_t req_id, size_t node_handle, size_t edge_handle, uint64_t del_time);
            void unpack_edge_delete(size_t *req_id, void **node_handle, void **edge_handle, uint64_t *del_time);
            void prep_node_delete_ack(size_t req_id);
            void prep_edge_delete_ack(size_t req_id);
            void unpack_delete_ack(size_t *req_id);
            // Reachability functions
            void prep_reachable_prop(std::vector<size_t> *src_nodes,
                std::shared_ptr<po6::net::location> src_loc,
                size_t dest_node,
                std::shared_ptr<po6::net::location> dest_loc,
                size_t req_id,
                size_t prev_req_id,
                std::shared_ptr<std::vector<uint64_t>> vector_clock);
            std::unique_ptr<std::vector<size_t>> unpack_reachable_prop(
                std::unique_ptr<po6::net::location> *src_loc,
                void **dest_node,
                std::shared_ptr<po6::net::location> *dest_loc,
                size_t *req_id,
                size_t *prev_req_id,
                std::shared_ptr<std::vector<uint64_t>> *vector_clock);
            void prep_reachable_rep(size_t req_id, 
                bool is_reachable,
                size_t src_node,
                std::shared_ptr<po6::net::location> src_loc,
                std::unique_ptr<std::vector<size_t>> del_nodes,
                std::unique_ptr<std::vector<uint64_t>> del_times);
            std::unique_ptr<po6::net::location> unpack_reachable_rep(size_t *req_id, 
                bool *is_reachable,
                size_t *src_node,
                size_t *num_del_nodes,
                std::unique_ptr<std::vector<size_t>> *del_nodes,
                std::unique_ptr<std::vector<uint64_t>> *del_times);
            // Error message
            void prep_error();

        private:
            void prep_create_ack(size_t req_id, size_t mem_addr, bool node);
            void prep_delete_ack(size_t req_id, bool node);
    };

    inline
    message :: message(enum msg_type t)
        : type(t)
    {
    }

    inline 
    message :: message(message& copy)
        : type(copy.type)
    {
        buf = copy.buf;
    }

    inline void
    message :: change_type(enum msg_type t)
    {
        type = t;
    }

    inline void
    message :: prep_node_create(size_t req_id, uint64_t creat_time)
    {
        type = NODE_CREATE_REQ;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + //type
            sizeof(size_t) + //request id
            sizeof(uint64_t))); //creat_time
        
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id << creat_time;
    }

    inline void
    message :: unpack_node_create(size_t *req_id, uint64_t *creat_time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        uint64_t time;
        size_t id;
        buf->unpack_from(index) >> _type;
        assert(_type == NODE_CREATE_REQ);
        type = NODE_CREATE_REQ;

        index += sizeof(enum msg_type);
        buf->unpack_from(index) >> id >> time;
        *req_id = id;
        *creat_time = time;
    }

    inline void
    message :: prep_edge_create(size_t req_id, size_t local_node, size_t remote_node, 
        std::shared_ptr<po6::net::location> remote_server, uint64_t remote_node_creat_time,
        uint64_t edge_creat_time)
    {
        type = EDGE_CREATE_REQ;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + //request id
            2 * sizeof(size_t) + //nodes
            sizeof(uint32_t) + //ip addr
            sizeof(uint16_t) + //port
            2 * sizeof(uint64_t))); // create times
        
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id 
            << local_node << remote_node
            << remote_server->get_addr() << remote_server->port
            << remote_node_creat_time << edge_creat_time;
    }

    inline void
    message :: unpack_edge_create(size_t *req_id, void **local_node,
        std::unique_ptr<common::meta_element> *remote_node,
        uint64_t *edge_creat_time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        uint32_t ip_addr;
        uint16_t port;
        size_t mem_addr1, mem_addr2, id;
        uint64_t edge_time, remote_node_time;
        std::shared_ptr<po6::net::location> remote;
        buf->unpack_from(index) >> _type;
        assert(_type == EDGE_CREATE_REQ);
        type = EDGE_CREATE_REQ;
        
        index += sizeof(enum msg_type);
        buf->unpack_from(index) >> id >> mem_addr1 >> mem_addr2 >> ip_addr >> port 
            >> remote_node_time >> edge_time;

        *req_id = id;
        remote.reset(new po6::net::location(ip_addr, port));
        *local_node = (void *)mem_addr1;
        remote_node->reset(new common::meta_element(remote, remote_node_time,
            MAX_TIME, (void*)mem_addr2));
        *edge_creat_time = edge_time;
    }


    inline void
    message :: prep_create_ack(size_t req_id, size_t mem_addr, bool node)
    {
        if (node) {
            type = NODE_CREATE_ACK;
        } else {
            type = EDGE_CREATE_ACK;
        }
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + 
            sizeof(size_t) + //request id
            sizeof(size_t)));

        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id << mem_addr;
    }

    inline void
    message :: prep_node_create_ack(size_t req_id, size_t mem_addr)
    {
        prep_create_ack(req_id, mem_addr, true);
    }
    inline void
    message :: prep_edge_create_ack(size_t req_id, size_t mem_addr)
    {
        prep_create_ack(req_id, mem_addr, false);
    }

    inline void
    message :: unpack_create_ack(size_t *req_id, void **mem_addr)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t addr, id;
        buf->unpack_from(index) >> _type;
        assert((_type == NODE_CREATE_ACK) || (_type == EDGE_CREATE_ACK));
        type = (enum msg_type)_type;
        index += sizeof(uint32_t);

        buf->unpack_from(index) >> id >> addr;
        *req_id = id;
        *mem_addr = (void *) addr;
    }

    inline void
    message :: prep_node_delete(size_t req_id, size_t node_handle, uint64_t del_time)
    {
        type = NODE_DELETE_REQ;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + //request id
            sizeof(size_t) + // node handle
            sizeof(uint64_t))); // del time

        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id << node_handle << del_time;
    }

    inline void
    message :: unpack_node_delete(size_t *req_id, void **node_handle, uint64_t *del_time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t node_addr, id;
        uint64_t time;
        buf->unpack_from(index) >> _type;
        assert(_type == NODE_DELETE_REQ);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> id >> node_addr >> time;
        *req_id = id;
        *node_handle = (void *)node_addr;
        *del_time = time;
    }
    
    inline void
    message :: prep_edge_delete(size_t req_id, size_t node_handle, size_t edge_handle, uint64_t del_time)
    {
        type = EDGE_DELETE_REQ;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + //request id
            2 * sizeof(size_t) + // handles
            sizeof(uint64_t))); // del time

        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id 
            << node_handle << edge_handle << del_time;
    }

    inline void
    message :: unpack_edge_delete(size_t *req_id, void **node_handle, void **edge_handle, uint64_t *del_time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t node_addr, edge_addr, id;
        uint64_t time;
        buf->unpack_from(index) >> _type;
        assert(_type == EDGE_DELETE_REQ);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> id >> node_addr >> edge_addr >> time;
        *node_handle = (void *)node_addr;
        *edge_handle = (void *)edge_addr;
        *del_time = time;
        *req_id = id;
    }

    inline void
    message :: prep_delete_ack(size_t req_id, bool node)
    {
        if (node) {
            type = NODE_DELETE_ACK;
        } else {
            type = EDGE_DELETE_ACK;
        }
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + 
            sizeof(size_t)));

        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id;
    }


    inline void
    message :: prep_node_delete_ack(size_t req_id)
    {
        prep_delete_ack(req_id, true);
    }

    inline void
    message :: prep_edge_delete_ack(size_t req_id)
    {
        prep_delete_ack(req_id, false);
    }

    inline void
    message :: unpack_delete_ack(size_t *req_id)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t id;
        buf->unpack_from(index) >> _type;
        assert((_type == NODE_DELETE_ACK) || (_type == EDGE_DELETE_ACK));
        type = (enum msg_type)_type;
        index += sizeof(uint32_t);

        buf->unpack_from(index) >> id;
        *req_id = id;
    }

    inline void
    message :: prep_reachable_prop(std::vector<size_t> *src_nodes,
        std::shared_ptr<po6::net::location> src_loc,
        size_t dest_node, 
        std::shared_ptr<po6::net::location> dest_loc, 
        size_t req_id,
        size_t prev_req_id,
        std::shared_ptr<std::vector<uint64_t>> vector_clock)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        size_t num_nodes = src_nodes->size();
        size_t i;
        type = REACHABLE_PROP;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + //num_nodes
            num_nodes * sizeof(size_t) + //src_nodes
            sizeof(uint32_t) + sizeof(uint16_t) + //src_loc
            sizeof(size_t) + //dest_node
            sizeof(uint32_t) + sizeof(uint16_t) + //dest_loc
            sizeof(size_t) +//req_id
            sizeof(size_t) +//prev_req_id
            NUM_SHARDS * sizeof(uint64_t) //vector clock
            ));

        buf->pack_at(index) << type;
        index += sizeof(enum msg_type);
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->pack_at(index) << vector_clock->at(i);
        }
        buf->pack_at(index) << num_nodes;
        index += sizeof(size_t);
        for (i = 0; i < num_nodes; i++, index += sizeof(size_t))
        {
            buf->pack_at(index) << src_nodes->at(i);
        }
        buf->pack_at(index) << src_loc->get_addr() << src_loc->port << dest_node 
            << dest_loc->get_addr() << dest_loc->port << req_id << prev_req_id;
    }

    inline std::unique_ptr<std::vector<size_t>>
    message :: unpack_reachable_prop(std::unique_ptr<po6::net::location> *src_loc, 
        void **dest_node, 
        std::shared_ptr<po6::net::location> *dest_loc, 
        size_t *req_id,
        size_t *prev_req_id,
        std::shared_ptr<std::vector<uint64_t>> *vector_clock)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        std::unique_ptr<std::vector<size_t>> src(new std::vector<size_t>());
        size_t dest, num_nodes, i, temp;
        uint32_t src_ipaddr, dest_ipaddr;
        uint16_t src_port, dest_port;
        size_t r_count, p_r_count;
        std::unique_ptr<po6::net::location> _src_loc;
        std::shared_ptr<po6::net::location> _dest_loc;

        buf->unpack_from(index) >> _type;
        assert(_type == REACHABLE_PROP);
        type = REACHABLE_PROP;
        index += sizeof(enum msg_type);
        
        vector_clock->reset(new std::vector<uint64_t>(NUM_SHARDS, 0));
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->unpack_from(index) >> (**vector_clock)[i]; //dereferencing pointer to pointer
        }

        buf->unpack_from(index) >> num_nodes;
        index += sizeof(size_t);
        for (i = 0; i < num_nodes; i++, index += sizeof(size_t))
        {
            buf->unpack_from(index) >> temp;
            src->push_back(temp);
        }
        buf->unpack_from(index) >> src_ipaddr >> src_port >> dest
            >> dest_ipaddr >> dest_port >> r_count >> p_r_count;

        *dest_node = (void *) dest;
        *req_id = r_count;
        *prev_req_id = p_r_count;
        _src_loc.reset(new po6::net::location(src_ipaddr, src_port));
        _dest_loc.reset(new po6::net::location(dest_ipaddr, dest_port));
        *src_loc = std::move(_src_loc);
        *dest_loc = std::move(_dest_loc);
        return src;
    }

    inline void
    message :: prep_reachable_rep(size_t req_id, 
        bool is_reachable,
        size_t src_node,
        std::shared_ptr<po6::net::location> src_loc,
        std::unique_ptr<std::vector<size_t>> del_nodes,
        std::unique_ptr<std::vector<uint64_t>> del_times)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t temp = (uint32_t) is_reachable;
        size_t i;
        type = REACHABLE_REPLY;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + //req id
            sizeof(uint32_t) + //is_reachable
            sizeof(size_t) + //src_node
            sizeof(uint16_t)+ //port
            sizeof(uint32_t) + //ip addr
            sizeof(size_t) + //no. of deleted nodes
            del_nodes->size() * sizeof(size_t) +
            del_times->size() * sizeof(uint64_t)));

        buf->pack_at(index) << type << del_nodes->size();
        index += sizeof(enum msg_type) + sizeof(size_t);
        for (i = 0; i < del_nodes->size(); i++, index += (sizeof(size_t) + sizeof(uint64_t)))
        {
            buf->pack_at(index) << (*del_nodes)[i] << (*del_times)[i];
        }
        buf->pack_at(index) << req_id << temp << src_node 
            << src_loc->get_addr() << src_loc->port;
    }

    inline std::unique_ptr<po6::net::location>
    message :: unpack_reachable_rep(size_t *req_id, 
        bool *is_reachable,
        size_t *src_node,
        size_t *num_del_nodes,
        std::unique_ptr<std::vector<size_t>> *del_nodes,
        std::unique_ptr<std::vector<uint64_t>> *del_times)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t r_count;
        uint32_t reachable;
        size_t s_node, n_del_nodes, i, del_node;
        uint64_t del_time;
        uint16_t s_port;
        uint32_t s_ipaddr;
        std::unique_ptr<po6::net::location> ret;
        buf->unpack_from(index) >> _type;
        assert(_type == REACHABLE_REPLY);
        type = REACHABLE_REPLY;

        index += sizeof(enum msg_type);
        buf->unpack_from(index) >> n_del_nodes;
        index += sizeof(size_t);
        for (i = 0; i < n_del_nodes; i++, index += (sizeof(size_t) + sizeof(uint64_t)))
        {
            buf->unpack_from(index) >> del_node >> del_time;
            (**del_nodes).push_back(del_node);
            (**del_times).push_back(del_time);
        }
        buf->unpack_from(index) >> r_count >> reachable >> s_node >> s_ipaddr >> s_port;
        *req_id = r_count;
        *is_reachable = (bool) reachable;
        *src_node = s_node;
        *num_del_nodes = n_del_nodes;
        ret.reset(new po6::net::location(s_ipaddr, s_port));
        return ret;
    }

} //namespace message

#endif //__MESSAGE__
