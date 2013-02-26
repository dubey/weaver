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
        CLIENT_CLUSTERING_REPLY,
        NODE_REFRESH_REQ,
        NODE_REFRESH_REPLY,
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
        CACHE_UPDATE,
        CACHE_UPDATE_ACK,
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
                std::shared_ptr<std::vector<uint64_t>> vector_clock,
                std::vector<size_t> ignore_cache);
            std::unique_ptr<std::vector<size_t>> unpack_reachable_prop(
                int *src_loc,
                size_t *dest_node,
                int *dest_loc,
                size_t *req_id,
                size_t *prev_req_id,
                std::shared_ptr<std::vector<common::property>> *edge_props,
                std::shared_ptr<std::vector<uint64_t>> *vector_clock,
                std::vector<size_t> *ignore_cache,
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
            void prep_cache_update(std::vector<size_t> good, std::vector<size_t> bad);
            void unpack_cache_update(std::vector<size_t> *good, std::vector<size_t> *bad);
            void prep_done_cache();
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
        std::shared_ptr<std::vector<uint64_t>> vector_clock,
        std::vector<size_t> ignore_cache)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        size_t num_nodes = src_nodes->size();
        size_t num_props = edge_props->size();
        size_t num_ignore = ignore_cache.size();
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
            NUM_SHARDS * sizeof(uint64_t) + //vector clock
            sizeof(size_t) + // num_ignore
            ignore_cache.size() * sizeof(size_t))); // ignore cached ids

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
        buf->pack_at(index) << num_ignore;
        index += sizeof(size_t);
        for (i = 0; i < num_ignore; i++, index += sizeof(size_t))
        {
            buf->pack_at(index) << ignore_cache[i];
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
        std::vector<size_t> *ignore_cache,
        int myid)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint32_t _type;
        std::unique_ptr<std::vector<size_t>> src(new std::vector<size_t>());
        size_t num_nodes, num_props, num_ignore, i, temp;
        uint64_t myclock;

        buf->unpack_from(index) >> _type;
        assert(_type == REACHABLE_PROP);
        type = REACHABLE_PROP;
        index += sizeof(enum msg_type);
        
        vector_clock->reset(new std::vector<uint64_t>(NUM_SHARDS, 0));
        for (i = 0; i < NUM_SHARDS; i++, index += sizeof(uint64_t))
        {
            buf->unpack_from(index) >> (**vector_clock)[i]; //dereferencing pointer to pointer
            if (i == (size_t)myid) {
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
        buf->unpack_from(index) >> num_ignore;
        index += sizeof(size_t);
        for (i = 0; i < num_ignore; i++, index += sizeof(size_t))
        {
            buf->unpack_from(index) >> temp;
            ignore_cache->push_back(temp);
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

    inline void
    message :: prep_cache_update(std::vector<size_t> good, std::vector<size_t> bad)
    {
        size_t gsize = good.size();
        size_t bsize = bad.size();
        uint32_t index = BUSYBEE_HEADER_SIZE;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE +
            2 * sizeof(size_t) + // sizes of vectors
            (gsize + bsize) * sizeof(size_t))); // vectors
        buf->pack_at(index) << type << gsize << bsize;
        index += (2*sizeof(size_t));
        for (size_t i = 0; i < gsize; i++, index += sizeof(size_t))
        {
            buf->pack_at(index) << good[i];
        }
        for (size_t i = 0; i < bsize; i++, index += sizeof(size_t))
        {
            buf->pack_at(index) << bad[i];
        }
    }

    inline void
    message :: unpack_cache_update(std::vector<size_t> *good, std::vector<size_t> *bad)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE + sizeof(enum msg_type);
        size_t gsize, bsize, temp;
        buf->unpack_from(index) >> gsize >> bsize;
        index += (2*sizeof(size_t));
        for (size_t i = 0; i < gsize; i++, index += sizeof(size_t))
        {
            buf->unpack_from(index) >> temp;
            good->push_back(temp);
        }
        for (size_t i = 0; i < bsize; i++, index += sizeof(size_t))
        {
            buf->unpack_from(index) >> temp;
            bad->push_back(temp);
        }
    }

    inline void
    message :: prep_done_cache()
    {
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + 
            sizeof(enum msg_type))); // type
        buf->pack_at(BUSYBEE_HEADER_SIZE) << type;
    }


// size templates
    inline size_t size(const bool &t)
    {
        return sizeof(uint16_t);
    }
    inline size_t size(const uint16_t &t)
    {
        return sizeof(uint16_t);
    }
    inline size_t size(const uint32_t &t)
    {
        return sizeof(uint32_t);
    }
    inline size_t size(const uint64_t &t)
    {
        return sizeof(uint64_t);
    }
    inline size_t size(const int &t)
    {
        return sizeof(int);
    }
    inline size_t size(const po6::net::location& t)
    {
        return sizeof(uint32_t) + sizeof(uint16_t);
    }
    inline size_t size(const common::property& t)
    {
        return sizeof(uint32_t)+sizeof(size_t);
    }

    template <typename T1, typename T2>
    inline size_t size(const std::pair<T1, T2>& t)
    {
        return size(t.first) + size(t.second);
    }

    template <typename T>
    inline size_t size(const std::unordered_set<T> &t)
    {
        // O(n) size operation can handle elements of differing sizes
        size_t total_size = 0;
        for(const T &elem : t)
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
        for (const std::pair<T1,T2> &pair : t) //XXX change to reference
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

    template <typename T, typename... Args>
    inline size_t size(const T& t, const Args&... args)
    {
        return size(t) + size(args...);
    }
// packing templates
    inline void pack_buffer(e::buffer &buf, uint32_t index, const bool &t)
    {
        uint16_t to_pack = t ? 1 : 0;
        buf.pack_at(index) << to_pack;
    }
    inline void pack_buffer(e::buffer &buf, uint32_t index, const uint16_t &t)
    {
        buf.pack_at(index) << t;
    }
    inline void pack_buffer(e::buffer &buf, uint32_t index, const uint32_t &t)
    {
        buf.pack_at(index) << t;
    }
    inline void pack_buffer(e::buffer &buf, uint32_t index, const uint64_t &t)
    {
        buf.pack_at(index) << t;
    }
    inline void pack_buffer(e::buffer &buf, uint32_t index, const int &t)
    {
        buf.pack_at(index) << t;
    }

    inline void pack_buffer(e::buffer &buf, uint32_t index, const double& t)
    {
        uint64_t dbl;
        memcpy(&dbl, &t, sizeof(double)); //to avoid casting issues, probably could avoid copy
        buf.pack_at(index) << dbl;
    }

    inline void pack_buffer(e::buffer &buf,
            uint32_t index, const po6::net::location& t)
    {
        uint32_t addr = (const_cast<po6::net::location&>(t).get_addr()); //watch out
        buf.pack_at(index) << addr << t.port;
    }

    inline void pack_buffer(e::buffer &buf,
            uint32_t index, const common::property& t)
    {
        buf.pack_at(index) << t.key << t.value;
    }

    template <typename T1, typename T2>
    inline void pack_buffer(e::buffer &buf,
            uint32_t index, const std::pair<T1, T2>& t)
    {
        // assumes constant size
        pack_buffer(buf, index, t.first);
        pack_buffer(buf, index + size(t.first), t.second);
    }

    template <typename T> 
    inline void pack_buffer(e::buffer &buf,
            uint32_t index, const std::vector<T>& t)
    {
        // !assumes constant element size
        size_t num_elems = t.size();
        buf.pack_at(index) << num_elems;
        index += sizeof(size_t);
        if (num_elems > 0){
            size_t element_size = size(t[0]);
            for (const T &elem : t)
            {
                pack_buffer(buf, index, elem);
                index += element_size;
                assert(element_size == size(elem));//slow
            }
        }
    }

    template <typename T>
    inline void pack_buffer(e::buffer &buf,
            uint32_t index, const std::unordered_set<T>& t)
    {
        size_t num_keys = t.size();
        buf.pack_at(index) << num_keys;
        index += sizeof(size_t);
        for (const T &elem : t)
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
        for (const std::pair<T1,T2> &pair : t)
        {
            pack_buffer(buf, index, pair.first);
            index += size(pair.first);
            pack_buffer(buf, index, pair.second);
            index += size(pair.second);
        }
    }


    template <typename T, typename... Args>
    inline void pack_buffer(e::buffer &buf, uint32_t index, const T& t, const Args&... args)
    {
        pack_buffer(buf, index, t);
        pack_buffer(buf, index+size(t), args...);
    }

    template <typename... Args>
    inline void
    prepare_message(message &m, const enum msg_type given_type, const Args&... args)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        size_t bytes_to_pack = size(args...);
        m.type = given_type;
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack+ 4));

        m.buf->pack_at(index) << given_type;
        pack_buffer(*m.buf, index + sizeof(enum msg_type), args...);
    }
// unpacking templates
    inline void unpack_buffer(e::buffer &buf, uint32_t index, bool &t)
    {
        uint16_t temp;
        buf.unpack_from(index) >> temp;
        t = (temp != 0);
    }
    inline void unpack_buffer(e::buffer &buf, uint32_t index, uint16_t &t)
    {
        buf.unpack_from(index) >> t;
    }
    inline void unpack_buffer(e::buffer &buf, uint32_t index, uint32_t &t)
    {
        buf.unpack_from(index) >> t;
    }
    inline void unpack_buffer(e::buffer &buf, uint32_t index, uint64_t &t)
    {
        buf.unpack_from(index) >> t;
    }
    inline void unpack_buffer(e::buffer &buf, uint32_t index, int &t)
    {
        buf.unpack_from(index) >> t;
    }

    inline void unpack_buffer(e::buffer &buf,
            uint32_t index, common::property& t)
    {
        uint32_t key;
        size_t val;
        buf.unpack_from(index) >> key >> val;
        t.key = key;
        t.value = val;
    }

    inline void unpack_buffer(e::buffer &buf, uint32_t index, double& t)
    {
        uint64_t dbl;
        buf.unpack_from(index) >> dbl;
        memcpy(&t, &dbl, sizeof(double)); //to avoid casting issues, probably could avoid copy
    }

    inline void unpack_buffer(e::buffer &buf,
            uint32_t index, po6::net::location& t)
    {
        uint32_t ipaddr;
        uint16_t port;
        buf.unpack_from(index) >> ipaddr >> port;
        
        t = po6::net::location(ipaddr, port);
    }

    template <typename T1, typename T2>
    inline void unpack_buffer(e::buffer &buf,
            uint32_t index, std::pair<T1, T2>& t)
    {
        //assumes constant size
        unpack_buffer(buf, index, t.first);
        unpack_buffer(buf, index+size(t.first), t.second);
    }

    template <typename T> 
    inline void unpack_buffer(e::buffer &buf,
            uint32_t index, std::vector<T>& t)
    {
        assert(t.size() == 0);
        size_t elements_left;
        buf.unpack_from(index) >> elements_left;

        t.reserve(elements_left);

        index += sizeof(size_t);
        while (elements_left > 0){
            T to_add;
            unpack_buffer(buf, index, to_add);
            t.push_back(to_add);
            index += size(to_add);
            elements_left--;
        }
    }

    template <typename T>
    inline void unpack_buffer(e::buffer &buf,
            uint32_t index, std::unordered_set<T>& t)
    {
        assert(t.size() == 0);
        size_t elements_left;
        buf.unpack_from(index) >> elements_left;

        t.rehash(elements_left*1.25); // set number of buckets to 1.25*elements it will contain

        index += sizeof(size_t);

        while (elements_left > 0){
            T to_add;
            unpack_buffer(buf, index, to_add);
            t.insert(to_add);
            index += size(to_add);
            elements_left--;
        }
    }

    template <typename T1, typename T2>
    inline void unpack_buffer(e::buffer &buf,
            uint32_t index, std::unordered_map<T1, T2> &t)
    {
        assert(t.size() == 0);
        size_t elements_left;
        buf.unpack_from(index) >> elements_left;
        // set number of buckets to 1.25*elements it will contain
        // did not use reserve as max_load_factor is default 1
        t.rehash(elements_left*1.25); 

        index += sizeof(size_t);

        while (elements_left > 0){
            T1 key_to_add;
            T2 val_to_add;

            unpack_buffer(buf, index, key_to_add);
            index += size(key_to_add);

            unpack_buffer(buf, index, val_to_add);
            index += size(val_to_add);

            t[key_to_add] = val_to_add;
            elements_left--;
        }
    }


    template <typename T, typename... Args>
    inline void unpack_buffer(e::buffer &buf, uint32_t index, T &t, Args&... args)
    {
        unpack_buffer(buf, index, t);
        unpack_buffer(buf, index+size(t), args...);
    }

    template <typename... Args>
    inline void
    unpack_message(const message &m, const enum msg_type expected_type, Args&... args)
    {
        uint32_t _type;
        m.buf->unpack_from(BUSYBEE_HEADER_SIZE) >> _type;
        assert((enum msg_type)_type == expected_type);

        unpack_buffer(*m.buf, BUSYBEE_HEADER_SIZE + sizeof(enum msg_type), args...);
    }
} //namespace message

#endif //__MESSAGE__
