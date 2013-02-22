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
#include "common/property.h"
#include "common/meta_element.h"
#include "common/vclock.h"
#include "unordered_set"
#include "unordered_map"
#include "db/cache/cache.h" //so we get std::hash override for location




namespace message
{
    enum msg_type
    {
        CLIENT_NODE_CREATE_REQ = 0,
        CLIENT_EDGE_CREATE_REQ,
        CLIENT_NODE_DELETE_REQ,
        CLIENT_EDGE_DELETE_REQ,
        CLIENT_ADD_EDGE_PROP,
        CLIENT_DEL_EDGE_PROP,
        CLIENT_CLUSTERING_REQ,
        CLIENT_REACHABLE_REQ,
        CLIENT_REPLY,
        NODE_CREATE_REQ,
        EDGE_CREATE_REQ,
        NODE_CREATE_ACK,
        EDGE_CREATE_ACK,
        NODE_DELETE_REQ,
        EDGE_DELETE_REQ,
        NODE_DELETE_ACK,
        EDGE_DELETE_ACK,
        EDGE_ADD_PROP,
        EDGE_DELETE_PROP,
        REACHABLE_REPLY,
        REACHABLE_PROP,
        CLUSTERING_REQ,
        CLUSTERING_REPLY,
        CLUSTERING_PROP,
        CLUSTERING_PROP_REPLY,
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
            void change_type(enum msg_type t);
            // Client to coordinator
            void prep_client0(uint16_t port);
            void unpack_client0(uint16_t *port);
            void prep_client1(uint16_t port, size_t elem);
            void unpack_client1(uint16_t *port, size_t *elem);
            void prep_client2(uint16_t port, size_t elem1, size_t elem2);
            void unpack_client2(uint16_t *port, size_t *elem1, size_t *elem2);
            void prep_client_add_prop(size_t elem1, size_t elem2, uint32_t key, size_t value);
            void unpack_client_add_prop(size_t *elem1, size_t *elem2, uint32_t *key, size_t *value);
            void prep_client_del_prop(size_t elem1, size_t elem2, uint32_t key);
            void unpack_client_del_prop(size_t *elem1, size_t *elem2, uint32_t *key);
            void prep_client_rr_req(uint16_t port, size_t elem1, size_t elem2,
                std::shared_ptr<std::vector<common::property>> edge_props);
            void unpack_client_rr_req(uint16_t *port, size_t *elem1, size_t *elem2,
                std::shared_ptr<std::vector<common::property>> edge_props);

            void prep_client_clustering_req(uint16_t port, size_t elem,
                std::shared_ptr<std::vector<common::property>> edge_props);
            void unpack_client_clustering_req(uint16_t *port, size_t *elem1, 
                std::shared_ptr<std::vector<common::property>> edge_props);

            void prep_client_rr_reply(bool reachable);
            void unpack_client_rr_reply(bool *reachable);

            void prep_client_clustering_reply(double coeff);
            void unpack_client_clustering_reply(double *coeff);
            // Create functions, coordinator to shards
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
            void prep_add_prop(size_t req_id, size_t node_handle, size_t edge_handle, 
                uint32_t key, size_t value, uint64_t time);
            void unpack_add_prop(size_t *req_id, void **node_handle, void **edge_handle,
                std::unique_ptr<common::property> *new_prop, uint64_t *time);
            void prep_del_prop(size_t req_id, size_t node_handle, size_t edge_handle,
                uint32_t key, uint64_t time);
            void unpack_del_prop(size_t *req_id, void **node_handle, void **edge_handle,
                uint32_t *key, uint64_t *time);
            // Reachability functions
            void prep_reachable_prop(std::vector<size_t> *src_nodes,
                std::shared_ptr<po6::net::location> src_loc,
                size_t dest_node,
                std::shared_ptr<po6::net::location> dest_loc,
                size_t req_id,
                size_t prev_req_id,
                std::shared_ptr<std::vector<common::property>> edge_props,
                std::shared_ptr<std::vector<uint64_t>> vector_clock);
            std::unique_ptr<std::vector<size_t>> unpack_reachable_prop(
                std::unique_ptr<po6::net::location> *src_loc,
                void **dest_node,
                std::shared_ptr<po6::net::location> *dest_loc,
                size_t *req_id,
                size_t *prev_req_id,
                std::shared_ptr<std::vector<common::property>> *edge_props,
                std::shared_ptr<std::vector<uint64_t>> *vector_clock,
                int myid);
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

            void prep_clustering_prop(
                size_t request_id,
                std::unordered_map<po6::net::location, std::unordered_set<size_t>> &nbr_nodes,
                std::shared_ptr<po6::net::location> src_loc,
                std::shared_ptr<std::vector<common::property>> edge_props,
                std::shared_ptr<std::vector<uint64_t>> vector_clock);
            void unpack_clustering_prop(
                size_t *request_id,
                std::unique_ptr<std::unordered_map<po6::net::location, std::unordered_set<size_t>>> &nbr_nodes,
                std::unique_ptr<po6::net::location> *src_loc,
                std::shared_ptr<std::vector<common::property>> *edge_props,
                std::shared_ptr<std::vector<uint64_t>> *vector_clock,
                int myid);

            void prep_clustering_reply(
                size_t request_id,
                double coefficient);
            void unpack_clustering_reply(
                size_t *request_id,
                double *coefficient);

            void prep_clustering_prop_reply(
                size_t request_ptr,
                size_t num_nbrs);
            void unpack_clustering_prop_reply(
                size_t *request_ptr,
                size_t *num_nbrs);

            void prep_clustering_req(
                size_t node,
                std::shared_ptr<po6::net::location> reply_to,
                size_t req_id,
                std::shared_ptr<std::vector<common::property>> edge_props,
                std::shared_ptr<std::vector<uint64_t>> vector_clock);
            void unpack_clustering_req(
                size_t **node,
                std::unique_ptr<po6::net::location> *reply_to,
                size_t *request_id,
                std::shared_ptr<std::vector<common::property>> *edge_props,
                std::shared_ptr<std::vector<uint64_t>> *vector_clock,
                int myid);
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
    message :: prep_client0(uint16_t port)
    {
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            sizeof(uint16_t))); // port
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << port;
    }

    inline void
    message :: unpack_client0(uint16_t *port)
    {
        uint16_t temp;
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> temp;
        *port = temp;
    }

    inline void
    message :: prep_client1(uint16_t port, size_t elem)
    {
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            sizeof(uint16_t) + // port
            sizeof(size_t))); // elem
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << port << elem;
    }

    inline void
    message :: unpack_client1(uint16_t *port, size_t *elem)
    {
        size_t temp2;
        uint16_t temp1;
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> temp1
            >> temp2;
        *port = temp1;
        *elem = temp2;
    }

    inline void
    message :: prep_client2(uint16_t port, size_t elem1, size_t elem2)
    {
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            sizeof(uint16_t) + // port
            2 * sizeof(size_t))); // elems
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << port << elem1 << elem2;
    }

    inline void
    message :: unpack_client2(uint16_t *port, size_t *elem1, size_t *elem2)
    {
        size_t temp1, temp2;
        uint16_t temp3;
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> temp3
            >> temp1 >> temp2;
        *port = temp3;
        *elem1 = temp1;
        *elem2 = temp2;
    }
    
    inline void 
    message :: prep_client_add_prop(size_t elem1, size_t elem2, uint32_t key, size_t value)
    {
        type = CLIENT_ADD_EDGE_PROP;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            2 * sizeof(size_t) + // elems
            sizeof(uint32_t) + // key
            sizeof(size_t))); // value
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << elem1 << elem2 << key << value;
    }
    
    inline void
    message :: unpack_client_add_prop(size_t *elem1, size_t *elem2, uint32_t *key, size_t *value)
    {
        size_t temp1, temp2, val;
        uint32_t k, code;
        buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        assert((enum msg_type)code == CLIENT_ADD_EDGE_PROP);
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(uint32_t)) >> temp1 >> temp2
            >> k >> val;
        *elem1 = temp1;
        *elem2 = temp2;
        *key = k;
        *value = val;
    }

    inline void 
    message :: prep_client_del_prop(size_t elem1, size_t elem2, uint32_t key)
    {
        type = CLIENT_DEL_EDGE_PROP;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            2 * sizeof(size_t) + // elems
            sizeof(uint32_t))); // key
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << elem1 << elem2 << key;
    }
    
    inline void 
    message :: unpack_client_del_prop(size_t *elem1, size_t *elem2, uint32_t *key)
    {
        size_t temp1, temp2;
        uint32_t k, code;
        buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        assert((enum msg_type)code == CLIENT_DEL_EDGE_PROP);
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(uint32_t)) >> temp1 >> temp2 >> k;
        *elem1 = temp1;
        *elem2 = temp2;
        *key = k;
    }

    inline void
    message :: prep_client_rr_req(uint16_t port, size_t elem1, size_t elem2,
        std::shared_ptr<std::vector<common::property>> edge_props)
    {
        size_t num_props = edge_props->size();
        int index, i;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            sizeof(uint16_t) + // port
            2 * sizeof(size_t) + // elems
            sizeof(size_t) + // num props
            edge_props->size() * (sizeof(uint32_t) + sizeof(size_t)))); // edge props
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << port << elem1 << elem2 << num_props;
        index = BUSYBEE_HEADER_SIZE+sizeof(enum msg_type)+sizeof(uint16_t)+3*sizeof(size_t);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t)+sizeof(size_t)))
        {
            buf->pack_at(index) << edge_props->at(i).key << edge_props->at(i).value;
        }
    }

    inline void
    message :: unpack_client_rr_req(uint16_t *port, size_t *elem1, size_t *elem2,
        std::shared_ptr<std::vector<common::property>> edge_props)
    {
        size_t temp1, temp2, num_props;
        uint16_t temp3;
        int index, i;
        uint32_t code;
        buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        assert((enum msg_type)code == CLIENT_REACHABLE_REQ);
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> temp3
            >> temp1 >> temp2 >> num_props;
        index = BUSYBEE_HEADER_SIZE+sizeof(enum msg_type)+sizeof(uint16_t)+3*sizeof(size_t);
        common::property prop(0,0,0);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t)+sizeof(size_t)))
        {
            uint32_t key;
            size_t val;
            buf->unpack_from(index) >> key >> val;
            prop.key = key;
            prop.value = val;
            edge_props->push_back(prop);
        }
        *port = temp3;
        *elem1 = temp1;
        *elem2 = temp2;
    }

    inline void
    message :: prep_client_rr_reply(bool reachable)
    {
        uint32_t temp = (uint32_t)reachable;
        assert(type == CLIENT_REPLY);
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            sizeof(uint32_t))); // reachable
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << temp;
    }
    
    inline void
    message :: unpack_client_rr_reply(bool *reachable)
    {
        uint32_t temp;
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> temp;
        *reachable = (bool)temp;
    }

    inline void
    message :: prep_client_clustering_req(uint16_t port, size_t elem,
        std::shared_ptr<std::vector<common::property>> edge_props)
    {
        size_t num_props = edge_props->size();
        int index, i;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            sizeof(uint16_t) + // port
            sizeof(size_t) + // node
            sizeof(size_t) + // num props
            edge_props->size() * (sizeof(uint32_t) + sizeof(size_t)))); // edge props
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << port << elem << num_props;
        index = BUSYBEE_HEADER_SIZE+sizeof(enum msg_type)+sizeof(uint16_t)+2*sizeof(size_t);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t)+sizeof(size_t)))
        {
            buf->pack_at(index) << edge_props->at(i).key << edge_props->at(i).value;
        }
    }
    
    inline void
    message :: unpack_client_clustering_req(uint16_t *port, size_t *elem1, 
        std::shared_ptr<std::vector<common::property>> edge_props)
    {
        size_t temp1, num_props;
        uint16_t temp3;
        int index, i;
        uint32_t code;
        buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        assert((enum msg_type)code == CLIENT_CLUSTERING_REQ);
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> temp3
            >> temp1 >> num_props;
        index = BUSYBEE_HEADER_SIZE+sizeof(enum msg_type)+sizeof(uint16_t)+2*sizeof(size_t);
        common::property prop(0,0,0);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t)+sizeof(size_t)))
        {
            uint32_t key;
            size_t val;
            buf->unpack_from(index) >> key >> val;
            prop.key = key;
            prop.value = val;
            edge_props->push_back(prop);
        }
        *port = temp3;
        *elem1 = temp1;
    }

    inline void
    message :: prep_client_clustering_reply(double coeff)
    {
        uint64_t temp;
        assert(type == CLIENT_REPLY);
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            sizeof(uint64_t))); // reachable
        memcpy(&temp, &coeff, sizeof(double));
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << temp;
    }

    inline void
    message :: unpack_client_clustering_reply(double *coeff)
    {
        uint64_t temp;
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> temp;
        memcpy(coeff, &temp, sizeof(double));
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
    message :: prep_add_prop(size_t req_id, size_t node_handle, size_t edge_handle, 
        uint32_t key, size_t value, uint64_t time)
    {
        type = EDGE_ADD_PROP;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + // request id
            2 * sizeof(size_t) + // handles
            sizeof(uint32_t) + // key
            sizeof(size_t) + // value
            sizeof(uint64_t))); // del time

        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id
            << node_handle << edge_handle << key << value << time;
    }
    
    inline void 
    message :: unpack_add_prop(size_t *req_id, void **node_handle, void **edge_handle,
        std::unique_ptr<common::property> *new_prop, uint64_t *time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type, key;
        size_t node_addr, edge_addr, id, value;
        uint64_t _time;
        buf->unpack_from(index) >> _type;
        assert(_type == EDGE_ADD_PROP);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> id >> node_addr >> edge_addr
            >> key >> value >> _time;
        *req_id = id;
        *node_handle = (void *)node_addr;
        *edge_handle = (void *)edge_addr;
        new_prop->reset(new common::property(key, value, _time));
        *time = _time;
    }
    
    inline void 
    message :: prep_del_prop(size_t req_id, size_t node_handle, size_t edge_handle,
        uint32_t key, uint64_t time)
    {
        type = EDGE_DELETE_PROP;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + // request id
            2 * sizeof(size_t) + // handles
            sizeof(uint32_t) + // key
            sizeof(uint64_t))); // del time

        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id
            << node_handle << edge_handle << key << time;
    }
    
    inline void 
    message :: unpack_del_prop(size_t *req_id, void **node_handle, void **edge_handle,
        uint32_t *key, uint64_t *time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type, _key;
        size_t node_addr, edge_addr, id;
        uint64_t _time;
        buf->unpack_from(index) >> _type;
        assert(_type == EDGE_DELETE_PROP);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> id >> node_addr >> edge_addr
            >> _key >> _time;
        *req_id = id;
        *node_handle = (void *)node_addr;
        *edge_handle = (void *)edge_addr;
        *key = _key;
        *time = _time;
    }

    inline void
    message :: prep_reachable_prop(std::vector<size_t> *src_nodes,
        std::shared_ptr<po6::net::location> src_loc,
        size_t dest_node, 
        std::shared_ptr<po6::net::location> dest_loc, 
        size_t req_id,
        size_t prev_req_id,
        std::shared_ptr<std::vector<common::property>> edge_props,
        std::shared_ptr<std::vector<uint64_t>> vector_clock)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        size_t num_nodes = src_nodes->size();
        size_t num_props = edge_props->size();
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
            sizeof(size_t) + // num props
            edge_props->size() * (sizeof(uint32_t) + sizeof(size_t)) + // edge props
            NUM_SHARDS * sizeof(uint64_t))); //vector clock

        buf->pack_at(index) << type;
        index += sizeof(enum msg_type);
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->pack_at(index) << vector_clock->at(i);
        }
        buf->pack_at(index) << num_props;
        index += sizeof(size_t);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t) + sizeof(size_t)))
        {
            buf->pack_at(index) << edge_props->at(i).key << edge_props->at(i).value;
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
        std::shared_ptr<std::vector<common::property>> *edge_props,
        std::shared_ptr<std::vector<uint64_t>> *vector_clock,
        int myid)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        std::unique_ptr<std::vector<size_t>> src(new std::vector<size_t>());
        size_t dest, num_nodes, num_props, i, temp;
        uint32_t src_ipaddr, dest_ipaddr;
        uint16_t src_port, dest_port;
        size_t r_count, p_r_count;
        std::unique_ptr<po6::net::location> _src_loc;
        std::shared_ptr<po6::net::location> _dest_loc;
        uint64_t myclock;

        buf->unpack_from(index) >> _type;
        assert(_type == REACHABLE_PROP);
        type = REACHABLE_PROP;
        index += sizeof(enum msg_type);
        
        vector_clock->reset(new std::vector<uint64_t>(NUM_SHARDS, 0));
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->unpack_from(index) >> (**vector_clock)[i]; //dereferencing pointer to pointer
            if (i == myid) {
                myclock = (**vector_clock)[i];
            }
        }
        buf->unpack_from(index) >> num_props;
        index += sizeof(size_t);
        common::property prop(0,0,myclock);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t) + sizeof(size_t)))
        {
            uint32_t key;
            size_t val;
            buf->unpack_from(index) >> key >> val;
            prop.key = key;
            prop.value = val;
            (*edge_props)->push_back(prop);
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

    inline 
    void message :: prep_clustering_prop(
            size_t request_id,
            std::unordered_map<po6::net::location, std::unordered_set<size_t>> &nbr_nodes,
            std::shared_ptr<po6::net::location> src_loc,
            std::shared_ptr<std::vector<common::property>> edge_props,
            std::shared_ptr<std::vector<uint64_t>> vector_clock)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        size_t num_keys = nbr_nodes.size();
        size_t num_nbrs = 0;
        for (auto pair : nbr_nodes)
        {
            num_nbrs += pair.second.size();
        }
        size_t num_props = edge_props->size();
        size_t i;
        type = CLUSTERING_PROP;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
                    sizeof(enum msg_type) +
                    sizeof(size_t) + //map size prefix (num keys)
                    num_keys * sizeof(size_t) + //set size prefix
                    num_keys * (sizeof(uint32_t)+sizeof(uint16_t)) + //map keys ip + port
                    num_nbrs * sizeof(size_t) + //set contents
                    sizeof(uint32_t) + sizeof(uint16_t) + //return_loc
                    sizeof(size_t) +//req_id
                    sizeof(size_t) + // num props
                    sizeof(uint32_t) + //src ip addr
                    sizeof(uint16_t) + //src port
                    edge_props->size() * (sizeof(uint32_t) + sizeof(size_t)) + // edge props
                    NUM_SHARDS * sizeof(uint64_t))); //vector clock

        buf->pack_at(index) << type;
        index += sizeof(enum msg_type);
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->pack_at(index) << vector_clock->at(i);
        }
        buf->pack_at(index) << num_props;
        index += sizeof(size_t);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t) + sizeof(size_t)))
        {
            buf->pack_at(index) << edge_props->at(i).key << edge_props->at(i).value;
        }
        buf->pack_at(index) << num_keys;
        index += sizeof(size_t);
        for (std::pair<po6::net::location, std::unordered_set<size_t>> pair : nbr_nodes)
        {
            buf->pack_at(index) << pair.first.get_addr() << pair.first.port << pair.second.size(); //address and num nbrs at that address
            index += +sizeof(uint32_t) + sizeof(uint16_t) + sizeof(size_t);
            for (size_t nbr : pair.second){
                buf->pack_at(index) << nbr;
                index += sizeof(size_t);
            }
        }
        buf->pack_at(index) << src_loc->get_addr() << src_loc->port << request_id; 
    }

    inline
    void message :: unpack_clustering_prop(
                size_t *request_id,
                std::unique_ptr<std::unordered_map<po6::net::location, std::unordered_set<size_t>>> &nbr_nodes,
                std::unique_ptr<po6::net::location> *src_loc,
                std::shared_ptr<std::vector<common::property>> *edge_props,
                std::shared_ptr<std::vector<uint64_t>> *vector_clock,
                int myid)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        std::unique_ptr<std::vector<size_t>> src(new std::vector<size_t>());
        size_t dest, num_keys, num_props, i, temp;
        uint32_t src_ipaddr;
        uint16_t src_port;
        size_t req_id;
        std::unique_ptr<po6::net::location> _src_loc;
        uint64_t myclock;

        buf->unpack_from(index) >> _type;
        assert(_type == CLUSTERING_PROP);
        type = CLUSTERING_PROP;
        index += sizeof(enum msg_type);
        
        vector_clock->reset(new std::vector<uint64_t>(NUM_SHARDS, 0));
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->unpack_from(index) >> (**vector_clock)[i]; //dereferencing pointer to pointer
            if (i == myid) {
                myclock = (**vector_clock)[i];
            }
        }

        buf->unpack_from(index) >> num_props;
        index += sizeof(size_t);
        common::property prop(0,0,myclock);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t) + sizeof(size_t)))
        {
            uint32_t key;
            size_t val;
            buf->unpack_from(index) >> key >> val;
            prop.key = key;
            prop.value = val;
            (*edge_props)->push_back(prop);
        }

        buf->unpack_from(index) >> num_keys;
        index += sizeof(size_t);
        nbr_nodes.reset(new
                std::unordered_map<po6::net::location,std::unordered_set<size_t>>); // can we set size Here xxx
        for (i = 0; i < num_keys; i++)
        {
            uint32_t temp_addr;
            uint16_t temp_port;
            size_t set_size;
            buf->unpack_from(index) >> temp_addr >> temp_port >> set_size; //address and num nbrs at that address
            po6::net::location *temp_loc = new po6::net::location(temp_addr, temp_port);
            index += +sizeof(uint32_t) + sizeof(uint16_t) + sizeof(size_t);
            for (int j = 0; j < set_size; j++)
            {
                size_t nbr_addr;
                buf->unpack_from(index) >> nbr_addr;
                index += sizeof(size_t);
                (*nbr_nodes)[*temp_loc].insert(nbr_addr);
            }
            //delete temp_loc;//xxx
        }

        buf->unpack_from(index) >> src_ipaddr >> src_port >> req_id;

        *request_id = req_id;
        _src_loc.reset(new po6::net::location(src_ipaddr, src_port));
        *src_loc = std::move(_src_loc);
    }

    inline
    void message :: prep_clustering_reply(
            size_t request_id,
            double coefficient)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        type = CLUSTERING_REPLY;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + //req id
            sizeof(uint64_t))); //coeff
        uint64_t coeff2;
        memcpy(&coeff2, &coefficient, sizeof(double)); //to avoid casting issues

        buf->pack_at(index) << type << request_id << coeff2;
    }

    inline
    void message :: unpack_clustering_reply(
            size_t *request_id,
            double *coefficient)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t id;
        uint64_t coeff;
        buf->unpack_from(index) >> _type;
        assert(_type == CLUSTERING_REPLY);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> id >> coeff;

        *request_id = id;
        memcpy(coefficient, &coeff, sizeof(double)); //to avoid casting issues
    }
    
    inline
    void message :: prep_clustering_prop_reply(
                size_t request_ptr, size_t num_nbrs)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        type = CLUSTERING_PROP_REPLY;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) + sizeof(size_t) + sizeof(size_t)));

        buf->pack_at(index) << type << request_ptr << num_nbrs;
    }

    inline 
    void message :: unpack_clustering_prop_reply(
                size_t *request_ptr, size_t *num_nbrs)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t req_ptr, nbrs;
        buf->unpack_from(index) >> _type;
        assert(_type == CLUSTERING_PROP_REPLY);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> req_ptr >> nbrs;

        *request_ptr = req_ptr;
        *num_nbrs = nbrs;
    }

    inline 
    void message :: prep_clustering_req(
                size_t node,
                std::shared_ptr<po6::net::location> reply_to,
                size_t req_id,
                std::shared_ptr<std::vector<common::property>> edge_props,
                std::shared_ptr<std::vector<uint64_t>> vector_clock)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;

        size_t num_props = edge_props->size();
        size_t i;
        type = CLUSTERING_REQ;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
                    sizeof(enum msg_type) +
                    sizeof(uint32_t) + sizeof(uint16_t) + //return_loc
                    sizeof(size_t) +//node
                    sizeof(size_t) +//req_id
                    sizeof(size_t) + // num props
                    edge_props->size() * (sizeof(uint32_t) + sizeof(size_t)) + // edge props
                    NUM_SHARDS * sizeof(uint64_t))); //vector clock

        buf->pack_at(index) << type;
        index += sizeof(enum msg_type);
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->pack_at(index) << vector_clock->at(i);
        }
        buf->pack_at(index) << num_props;
        index += sizeof(size_t);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t) + sizeof(size_t)))
        {
            buf->pack_at(index) << edge_props->at(i).key << edge_props->at(i).value;
        }

        buf->pack_at(index) << reply_to->get_addr() << reply_to->port << req_id
            << node; 
    }

    inline 
    void message :: unpack_clustering_req(size_t **node,
            std::unique_ptr<po6::net::location> *reply_to,
            size_t *request_id,
            std::shared_ptr<std::vector<common::property>> *edge_props,
            std::shared_ptr<std::vector<uint64_t>> *vector_clock,
            int myid)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        std::unique_ptr<std::vector<size_t>> src(new std::vector<size_t>());
        size_t num_props, i, temp;
        uint32_t reply_ipaddr;
        uint16_t reply_port;
        size_t req_id;
        std::unique_ptr<po6::net::location> _reply_loc;
        uint64_t myclock;
        size_t temp_node;

        buf->unpack_from(index) >> _type;
        assert(_type == CLUSTERING_REQ);
        type = CLUSTERING_REQ;
        index += sizeof(enum msg_type);
        
        vector_clock->reset(new std::vector<uint64_t>(NUM_SHARDS, 0));
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->unpack_from(index) >> (**vector_clock)[i]; //dereferencing pointer to pointer
            if (i == myid) {
                myclock = (**vector_clock)[i];
            }
        }

        buf->unpack_from(index) >> num_props;
        index += sizeof(size_t);
        common::property prop(0,0,myclock);
        for (i = 0; i < num_props; i++, index += (sizeof(uint32_t) + sizeof(size_t)))
        {
            uint32_t key;
            size_t val;
            buf->unpack_from(index) >> key >> val;
            prop.key = key;
            prop.value = val;
            (*edge_props)->push_back(prop);
        }

        buf->unpack_from(index) >> reply_ipaddr >> reply_port >> req_id >>
            temp_node;

        *node = (size_t *) temp_node;
        *request_id = req_id;
        _reply_loc.reset(new po6::net::location(reply_ipaddr, reply_port));
        *reply_to = std::move(_reply_loc);
    }


    template <typename T> 
    inline size_t size(const T& t)
    {
        return sizeof(T);
    }
    template <>  
    inline size_t size<float>(const float& t)
    {
        return (size_t) t+222;
    }
    template <>  
    inline size_t size<int>(const int& t)
    {
        return (size_t) t+22;
    }
    template <>  
    inline size_t size<po6::net::location>(const po6::net::location& t)
    {
        return sizeof(uint32_t) + sizeof(uint16_t);
    }

    template <typename T>
    inline size_t size(const std::unordered_set<T> &t)
    {
        // O(n) size operation can handle elements of differing sizes
        size_t total_size = 0;
        for(T &elem : t)
        {
            total_size += size(elem);
        }
        return sizeof(size_t)+total_size;
    }

    template <typename T1, typename T2>
    inline size_t size(const std::unordered_map<T1, T2>& t)
    {
        size_t total_size = 0;
        // O(n) size operation can handle keys and values of differing sizes
        for (std::pair<T1,T2> &pair : t)
        {
            total_size += size(pair.first) + size(pair.second);
        }
        return sizeof(size_t)+total_size;
    }

    template <typename T>
    inline size_t size(const std::vector<T> &t)
    {
        // first size_t to record size of vector, assumes constant size elements
        if (t.size()>0){
            return sizeof(size_t) + (t.size()*size(t[0]));
        }
        else
        {
            return sizeof(size_t);
        }

    }

/*
    template <>
    inline size_t size<std::vector<uint64_t>>(const std::vector<uint64_t>& t)

    {
        // first size_t to record size of vector
        return sizeof(size_t) + (t.size()*sizeof(uint64_t));
    }

    template <>
    inline size_t size<std::vector<common::property>>(const std::vector<common::property>& t)

    {
        // first size_t to record size of vector
        return sizeof(size_t) + (t.size()*(sizeof(uint32_t)+sizeof(size_t)));
    }
*/
    template <typename T, typename... Args>
    inline size_t size(const T& t, const Args&... args)
    {
        return size(t) + size(args...);
    }

    template <typename T> 
    inline void pack_buffer(e::buffer &buf, uint32_t index, const T& t)
    {
        buf.pack_at(index) << t;
    }

    template <> 
    inline void pack_buffer<double>(e::buffer &buf, uint32_t index, const double& t)
    {
        uint64_t dbl;
        memcpy(&dbl, &t, sizeof(double)); //to avoid casting issues, probably could avoid copy
        buf.pack_at(index) << dbl;
    }

    template <> 
    inline void pack_buffer<po6::net::location>(e::buffer &buf,
            uint32_t index, const po6::net::location& t)
    {
        buf.pack_at(index) << const_cast<po6::net::location&>(t).get_addr() <<
        t.port; //watch out
    }

/*
    template <> 
    inline void pack_buffer<std::vector<uint64_t>>(e::buffer &buf,
            uint32_t index, const std::vector<uint64_t>& t)
    {
        size_t num_props = t.size();
        buf.pack_at(index) << num_props;
        index += sizeof(size_t);
        for (uint64_t num : t)
        {
            buf.pack_at(index) << num;
            index += sizeof(uint64_t);
        }
    }

    template <> 
    inline void pack_buffer<std::vector<common::property>>(e::buffer &buf,
            uint32_t index, const std::vector<common::property>& t)
    {
        size_t num_props = t.size();
        buf.pack_at(index) << num_props;
        index += sizeof(size_t);
        for (common::property p : t)
        {
            buf.pack_at(index) << p.key << p.value;
            index += (sizeof(uint32_t) + sizeof(size_t));
        }
    }
*/
    template <typename T> 
    inline void pack_buffer(e::buffer &buf,
            uint32_t index, const std::vector<T>& t)
    {
        size_t num_props = t.size();
        buf.pack_at(index) << num_props;
        index += sizeof(size_t);
        for (T &elem : t)
        {
            pack_buffer(buf, index, elem);
            index += size(elem);
        }
    }

    template <typename T1, typename T2>
    inline void pack_buffer(e::buffer &buf,
            uint32_t index, const std::unordered_map<T1, T2>& t)
    {
        size_t num_keys = t.size();
        buf.pack_at(index) << num_keys;
        index += sizeof(size_t);
        for (std::pair<T1,T2> &pair : t)
        {
            pack_buffer(buf, index, pair.first);
            index += size(pair.first);
            pack_buffer(buf, index, pair.second);
            index += size(pair.second);
        }
    }

    template <typename T>
    inline void pack_buffer(e::buffer &buf,
            uint32_t index, const std::unordered_set<T>& t)
    {
        size_t num_keys = t.size();
        buf.pack_at(index) << num_keys;
        index += sizeof(size_t);
        for (T &elem : t)
        {
            pack_buffer(buf, index, elem);
            index += size(elem);
        }
    }

    template <typename T, typename... Args>
    inline void pack_buffer(e::buffer &buf, uint32_t index, const T& t, const Args&... args)
    {
        pack_buffer(buf, index, t);
        pack_buffer(buf, index+size(t), args...);
    }

    template <typename... Args>
    inline size_t
    prepare_message(message &m, enum msg_type given_type, const Args&... args)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        size_t bytes_to_pack = size(args...);
        m.type = given_type;
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));

        m.buf->pack_at(index) << given_type;
        pack_buffer(*m.buf, index + sizeof(enum msg_type), args...);

        return bytes_to_pack;
    }
} //namespace message

#endif //__MESSAGE__
