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
        REACHABLE_DONE,
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
            void prep_client_rr_reply(bool reachable);
            void unpack_client_rr_reply(bool *reachable);
            // Create functions, coordinator to shards
            void prep_node_create(size_t req_id, uint64_t creat_time);
            void unpack_node_create(size_t *req_id, uint64_t *creat_time);
            void prep_edge_create(size_t req_id, size_t local_node, size_t remote_node, 
                int remote_server, uint64_t remote_node_creat_time, uint64_t edge_creat_time);
            void unpack_edge_create(size_t *req_id, size_t *local_node,
                std::unique_ptr<common::meta_element> *remote_node,
                uint64_t *edge_creat_time);
            void prep_node_create_ack(size_t req_id, size_t mem_addr);
            void prep_edge_create_ack(size_t req_id, size_t mem_addr);
            void unpack_create_ack(size_t *req_id, size_t *mem_addr);
            // Update functions
            void prep_node_delete(size_t req_id, size_t node_handle, uint64_t del_time);
            void unpack_node_delete(size_t *req_id, size_t *node_handle, uint64_t *del_time);
            void prep_edge_delete(size_t req_id, size_t node_handle, size_t edge_handle, uint64_t del_time);
            void unpack_edge_delete(size_t *req_id, size_t *node_handle, size_t *edge_handle, uint64_t *del_time);
            void prep_node_delete_ack(size_t req_id, std::unique_ptr<std::vector<size_t>> cached_req_ids);
            void prep_edge_delete_ack(size_t req_id, std::unique_ptr<std::vector<size_t>> cached_req_ids);
            void unpack_delete_ack(size_t *req_id, std::unique_ptr<std::vector<size_t>> *cached_req_ids);
            void prep_add_prop(size_t req_id, size_t node_handle, size_t edge_handle, 
                uint32_t key, size_t value, uint64_t time);
            void unpack_add_prop(size_t *req_id, size_t *node_handle, size_t *edge_handle,
                std::unique_ptr<common::property> *new_prop, uint64_t *time);
            void prep_del_prop(size_t req_id, size_t node_handle, size_t edge_handle,
                uint32_t key, uint64_t time);
            void unpack_del_prop(size_t *req_id, size_t *node_handle, size_t *edge_handle,
                uint32_t *key, uint64_t *time);
            // Reachability functions
            void prep_reachable_prop(std::vector<size_t> *src_nodes,
                int src_loc,
                size_t dest_node,
                int dest_loc,
                size_t req_id,
                size_t prev_req_id,
                std::shared_ptr<std::vector<common::property>> edge_props,
                std::shared_ptr<std::vector<uint64_t>> vector_clock);
            std::unique_ptr<std::vector<size_t>> unpack_reachable_prop(
                int *src_loc,
                size_t *dest_node,
                int *dest_loc,
                size_t *req_id,
                size_t *prev_req_id,
                std::shared_ptr<std::vector<common::property>> *edge_props,
                std::shared_ptr<std::vector<uint64_t>> *vector_clock,
                int myid);
            void prep_reachable_rep(size_t req_id, 
                bool is_reachable,
                size_t src_node,
                int src_loc,
                std::unique_ptr<std::vector<size_t>> del_nodes,
                std::unique_ptr<std::vector<uint64_t>> del_times,
                size_t cache_req_id);
            void unpack_reachable_rep(size_t *req_id, 
                bool *is_reachable,
                size_t *src_node,
                int *src_loc,
                size_t *num_del_nodes,
                std::unique_ptr<std::vector<size_t>> *del_nodes,
                std::unique_ptr<std::vector<uint64_t>> *del_times,
                size_t *cache_req_id);
            void prep_done_request(size_t id);
            void unpack_done_request(size_t *id);
            // Error message
            void prep_error();

        private:
            void prep_create_ack(size_t req_id, size_t mem_addr, bool node);
            void prep_delete_ack(size_t req_id, std::unique_ptr<std::vector<size_t>> cached_req_ids, bool node);
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
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> (*port);
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
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> (*port)
            >> (*elem);
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
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> (*port)
            >> (*elem1) >> (*elem2);
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
        uint32_t code;
        buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        assert((enum msg_type)code == CLIENT_ADD_EDGE_PROP);
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(uint32_t)) >> (*elem1) >> (*elem2)
            >> (*key) >> (*value);
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
        uint32_t code;
        buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        assert((enum msg_type)code == CLIENT_DEL_EDGE_PROP);
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(uint32_t)) >> (*elem1) >> (*elem2) >> (*key);
    }

    inline void
    message :: prep_client_rr_req(uint16_t port, size_t elem1, size_t elem2,
        std::shared_ptr<std::vector<common::property>> edge_props)
    {
        size_t num_props = edge_props->size();
        size_t i;
        int index;
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
        size_t num_props, i;
        int index;
        uint32_t code;
        buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        assert((enum msg_type)code == CLIENT_REACHABLE_REQ);
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> (*port)
            >> (*elem1) >> (*elem2) >> num_props;
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
        buf->unpack_from(index) >> _type;
        assert(_type == NODE_CREATE_REQ);
        type = NODE_CREATE_REQ;

        index += sizeof(enum msg_type);
        buf->unpack_from(index) >> (*req_id) >> (*creat_time);
    }

    inline void
    message :: prep_edge_create(size_t req_id, size_t local_node, size_t remote_node, 
        int remote_server, uint64_t remote_node_creat_time, uint64_t edge_creat_time)
    {
        type = EDGE_CREATE_REQ;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            sizeof(enum msg_type) +
            sizeof(size_t) + //request id
            2 * sizeof(size_t) + //nodes
            sizeof(int) + // remote_server
            2 * sizeof(uint64_t))); // create times
        
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << req_id << local_node << remote_node
            << remote_server << remote_node_creat_time << edge_creat_time;
    }

    inline void
    message :: unpack_edge_create(size_t *req_id, size_t *local_node,
        std::unique_ptr<common::meta_element> *remote_node,
        uint64_t *edge_creat_time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        int loc;
        size_t mem_addr;
        uint64_t remote_node_time;
        buf->unpack_from(index) >> _type;
        assert(_type == EDGE_CREATE_REQ);
        type = EDGE_CREATE_REQ;
        
        index += sizeof(enum msg_type);
        buf->unpack_from(index) >> (*req_id) >> (*local_node) >> mem_addr >> loc 
            >> remote_node_time >> (*edge_creat_time);
        
        remote_node->reset(new common::meta_element(loc, remote_node_time,
            MAX_TIME, (void*)mem_addr));
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
    message :: unpack_create_ack(size_t *req_id, size_t *mem_addr)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        buf->unpack_from(index) >> _type;
        assert((_type == NODE_CREATE_ACK) || (_type == EDGE_CREATE_ACK));
        type = (enum msg_type)_type;
        index += sizeof(uint32_t);

        buf->unpack_from(index) >> (*req_id) >> (*mem_addr);
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
    message :: unpack_node_delete(size_t *req_id, size_t *node_handle, uint64_t *del_time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        buf->unpack_from(index) >> _type;
        assert(_type == NODE_DELETE_REQ);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> (*req_id) >> (*node_handle) >> (*del_time);
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
    message :: unpack_edge_delete(size_t *req_id, size_t *node_handle, size_t *edge_handle, uint64_t *del_time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        buf->unpack_from(index) >> _type;
        assert(_type == EDGE_DELETE_REQ);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> (*req_id) >> (*node_handle) >> (*edge_handle) >> (*del_time);
    }

    inline void
    message :: prep_delete_ack(size_t req_id, std::unique_ptr<std::vector<size_t>> cached_req_ids, bool node)
    {
        int index;
        size_t i;
        size_t len = cached_req_ids->size();
        if (node) {
            type = NODE_DELETE_ACK;
        } else {
            type = EDGE_DELETE_ACK;
        }
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + 
            sizeof(size_t) + // req_id
            sizeof(size_t) + // len of cached_req_ids
            len * sizeof(size_t))); // cached_req_ids
        
        index = BUSYBEE_HEADER_SIZE; 
        buf->pack_at(index) << type << req_id << len;
        index += (sizeof(enum msg_type) + 2*sizeof(size_t));
        for (i = 0; i < len; i++, index += sizeof(size_t))
        {
            buf->pack_at(index) << (*cached_req_ids)[i];
        }
    }


    inline void
    message :: prep_node_delete_ack(size_t req_id, std::unique_ptr<std::vector<size_t>> cached_req_ids)
    {
        prep_delete_ack(req_id, std::move(cached_req_ids), true);
    }

    inline void
    message :: prep_edge_delete_ack(size_t req_id, std::unique_ptr<std::vector<size_t>> cached_req_ids)
    {
        prep_delete_ack(req_id, std::move(cached_req_ids), false);
    }

    inline void
    message :: unpack_delete_ack(size_t *req_id, std::unique_ptr<std::vector<size_t>> *cached_req_ids)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        size_t i, len, cri;
        buf->unpack_from(index) >> _type;
        assert((_type == NODE_DELETE_ACK) || (_type == EDGE_DELETE_ACK));
        type = (enum msg_type)_type;
        index += sizeof(uint32_t);

        buf->unpack_from(index) >> (*req_id) >> len;
        index += (2*sizeof(size_t));
        for (i = 0; i < len; i++, index += sizeof(size_t))
        {
            buf->unpack_from(index) >> cri;
            (*cached_req_ids)->push_back(cri);
        }
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
    message :: unpack_add_prop(size_t *req_id, size_t *node_handle, size_t *edge_handle,
        std::unique_ptr<common::property> *new_prop, uint64_t *time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type, key;
        size_t value;
        buf->unpack_from(index) >> _type;
        assert(_type == EDGE_ADD_PROP);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> (*req_id) >> (*node_handle) >> (*edge_handle)
            >> key >> value >> (*time);
        new_prop->reset(new common::property(key, value, *time));
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
    message :: unpack_del_prop(size_t *req_id, size_t *node_handle, size_t *edge_handle,
        uint32_t *key, uint64_t *time)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        buf->unpack_from(index) >> _type;
        assert(_type == EDGE_DELETE_PROP);
        index += sizeof(enum msg_type);
        
        buf->unpack_from(index) >> (*req_id) >> (*node_handle) >> (*edge_handle)
            >> (*key) >> (*time);
    }

    inline void
    message :: prep_reachable_prop(std::vector<size_t> *src_nodes,
        int src_loc,
        size_t dest_node,
        int dest_loc,
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
            sizeof(int) + //src_loc
            sizeof(size_t) + //dest_node
            sizeof(int) + //dest_loc
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
        buf->pack_at(index) << src_loc << dest_node << dest_loc << req_id << prev_req_id;
    }

    inline std::unique_ptr<std::vector<size_t>>
    message :: unpack_reachable_prop(int *src_loc,
        size_t *dest_node,
        int *dest_loc,
        size_t *req_id,
        size_t *prev_req_id,
        std::shared_ptr<std::vector<common::property>> *edge_props,
        std::shared_ptr<std::vector<uint64_t>> *vector_clock,
        int myid)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        std::unique_ptr<std::vector<size_t>> src(new std::vector<size_t>());
        size_t num_nodes, num_props, i, temp;
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
        common::property prop(0, 0, myclock);
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
        buf->unpack_from(index) >> (*src_loc) >> (*dest_node)
            >> (*dest_loc) >> (*req_id) >> (*prev_req_id);
        return src;
    }

    inline void
    message :: prep_reachable_rep(size_t req_id, 
        bool is_reachable,
        size_t src_node,
        int src_loc,
        std::unique_ptr<std::vector<size_t>> del_nodes,
        std::unique_ptr<std::vector<uint64_t>> del_times,
        size_t cache_req_id)
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
            sizeof(int) + // src_loc
            sizeof(size_t) + //no. of deleted nodes
            del_nodes->size() * sizeof(size_t) +
            del_times->size() * sizeof(uint64_t) +
            sizeof(size_t))); // cache_req_id

        buf->pack_at(index) << type << del_nodes->size();
        index += sizeof(enum msg_type) + sizeof(size_t);
        for (i = 0; i < del_nodes->size(); i++, index += (sizeof(size_t) + sizeof(uint64_t)))
        {
            buf->pack_at(index) << (*del_nodes)[i] << (*del_times)[i];
        }
        buf->pack_at(index) << req_id << temp << src_node << src_loc << cache_req_id;
    }

    inline void
    message :: unpack_reachable_rep(size_t *req_id, 
        bool *is_reachable,
        size_t *src_node,
        int *src_loc,
        size_t *num_del_nodes,
        std::unique_ptr<std::vector<size_t>> *del_nodes,
        std::unique_ptr<std::vector<uint64_t>> *del_times,
        size_t *cache_req_id)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        uint32_t reachable;
        size_t i, del_node;
        uint64_t del_time;
        buf->unpack_from(index) >> _type;
        assert(_type == REACHABLE_REPLY);
        type = REACHABLE_REPLY;

        index += sizeof(enum msg_type);
        buf->unpack_from(index) >> (*num_del_nodes);
        index += sizeof(size_t);
        for (i = 0; i < *num_del_nodes; i++, index += (sizeof(size_t) + sizeof(uint64_t)))
        {
            buf->unpack_from(index) >> del_node >> del_time;
            (**del_nodes).push_back(del_node);
            (**del_times).push_back(del_time);
        }
        buf->unpack_from(index) >> (*req_id) >> reachable >> (*src_node) >> (*src_loc) >> (*cache_req_id);
        *is_reachable = (bool)reachable;
    }

    inline void
    message :: prep_done_request(size_t id)
    {
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type) + // type
            sizeof(size_t))); // id
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type << id;
    }

    inline void
    message :: unpack_done_request(size_t *id)
    {
        buf->unpack_from(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)) >> (*id);
    }

} //namespace message

#endif //__MESSAGE__
