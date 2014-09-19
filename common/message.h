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

#ifndef weaver_common_message_h_
#define weaver_common_message_h_

#include <memory>
#include <vector>
#include <deque>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <string>
#include <e/buffer.h>
#include <po6/net/location.h>
#include <busybee_constants.h>

#include "common/vclock.h"
#include "common/transaction.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/base_classes.h"
#include "node_prog/property.h"
#include "db/remote_node.h"
#include "db/property.h"

namespace db
{
    namespace element
    {
        class element;
        class node;
        class edge;
    }
}

namespace node_prog
{
    struct edge_cache_context;
    struct node_cache_context;
}

namespace message
{
    enum msg_type
    {
        // client messages
        CLIENT_TX_INIT = 0,
        CLIENT_TX_SUCCESS,
        CLIENT_TX_ABORT,
        CLIENT_NODE_PROG_REQ,
        CLIENT_NODE_PROG_REPLY,
        START_MIGR,
        ONE_STREAM_MIGR,
        EXIT_WEAVER,
        // graph update messages
        TX_INIT,
        TX_DONE,
        PERMANENTLY_DELETED_NODE,
        // node program messages
        NODE_PROG,
        NODE_PROG_RETURN,
        NODE_CONTEXT_FETCH,
        NODE_CONTEXT_REPLY,
        CACHE_UPDATE,
        CACHE_UPDATE_ACK,
        // migration messages
        MIGRATE_SEND_NODE,
        MIGRATED_NBR_UPDATE,
        MIGRATED_NBR_ACK,
        MIGRATION_TOKEN,
        CLIENT_NODE_COUNT,
        NODE_COUNT_REPLY,
        // initial graph loading
        LOADED_GRAPH,
        // coordinator group
        VT_CLOCK_UPDATE,
        VT_CLOCK_UPDATE_ACK,
        VT_NOP,
        VT_NOP_ACK,
        DONE_MIGR,

        ERROR
    };

    const char* to_string(const msg_type &t);

    class message
    {
        public:
            enum msg_type type;
            std::auto_ptr<e::buffer> buf;

            message() : type(ERROR), buf(NULL) { }
            message(enum msg_type t) : type(t), buf(NULL) { }
            message(message &copy) : type(copy.type) { buf.reset(copy.buf->copy()); }

            void change_type(enum msg_type t) { type = t; }

            void prepare_message(const enum msg_type given_type);
            template <typename... Args> void prepare_message(const enum msg_type given_type, const Args&... args);
            template <typename... Args> void unpack_message(const enum msg_type expected_type, Args&... args);
            template <typename... Args> void unpack_partial_message(const enum msg_type expected_type, Args&... args);
            enum msg_type unpack_message_type();

        private:
            template <typename... Args> void unpack_message_internal(bool check_empty, const enum msg_type expected_type, Args&... args);
    };

    uint64_t size(const enum msg_type &);
    uint64_t size(const enum node_prog::prog_type&);
    uint64_t size(const enum transaction::update_type&);
    uint64_t size(const enum transaction::tx_type&);
    uint64_t size(const node_prog::Node_Parameters_Base &t);
    uint64_t size(const node_prog::Node_State_Base &t);
    uint64_t size(const node_prog::Cache_Value_Base &t);
    uint64_t size(const bool&);
    uint64_t size(const char&);
    uint64_t size(const uint16_t&);
    uint64_t size(const uint32_t&);
    uint64_t size(const uint64_t&);
    uint64_t size(const int64_t&);
    uint64_t size(const int&);
    uint64_t size(const double&);
    uint64_t size(const std::string &t);
    uint64_t size(const vc::vclock &t);
    uint64_t size(const node_prog::property &t);
    uint64_t size(const db::element::property &t);
    uint64_t size(const db::element::remote_node &t);
    uint64_t size(const transaction::pending_update* const &ptr_t);
    uint64_t size(const transaction::nop_data* const &ptr_t);
    uint64_t size(const transaction::pending_tx &t);
    template <typename T1, typename T2> inline uint64_t size(const std::unordered_map<T1, T2>& t);
    template <typename T> inline uint64_t size(const std::unordered_set<T>& t);
    template <typename T> inline uint64_t size(const std::vector<T>& t);
    template <typename T> inline uint64_t size(const std::deque<T>& t);
    template <typename T1, typename T2, typename T3> uint64_t size(std::priority_queue<T1, T2, T3>);
    template <typename T1, typename T2> inline uint64_t size(const std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline uint64_t size(const std::tuple<T1, T2, T3>& t);
    template <typename T> inline uint64_t size(const std::shared_ptr<T> &ptr_t);
    template <typename T> inline uint64_t size(const std::unique_ptr<T> &ptr_t);
    uint64_t size(const node_prog::node_cache_context &t);
    uint64_t size(const node_prog::edge_cache_context &t);
    uint64_t size(const db::element::element &t);
    uint64_t size(const db::element::edge &t);
    uint64_t size(const db::element::edge* const &t);
    uint64_t size(const db::element::node &t);

    void pack_buffer(e::buffer::packer &packer, const node_prog::Node_Parameters_Base &t);
    void pack_buffer(e::buffer::packer &packer, const node_prog::Node_State_Base &t);
    void pack_buffer(e::buffer::packer &packer, const node_prog::Cache_Value_Base *&t);
    void pack_buffer(e::buffer::packer &packer, const enum msg_type &t);    
    void pack_buffer(e::buffer::packer &packer, const enum node_prog::prog_type &t);
    void pack_buffer(e::buffer::packer &packer, const enum transaction::update_type &t);
    void pack_buffer(e::buffer::packer &packer, const enum transaction::tx_type &t);
    void pack_buffer(e::buffer::packer &packer, const bool &t);
    void pack_buffer(e::buffer::packer &packer, const uint8_t &t);
    void pack_buffer(e::buffer::packer &packer, const uint16_t &t);
    void pack_buffer(e::buffer::packer &packer, const uint32_t &t);
    void pack_buffer(e::buffer::packer &packer, const uint64_t &t);
    void pack_buffer(e::buffer::packer &packer, const int64_t &t);
    void pack_buffer(e::buffer::packer &packer, const int &t);
    void pack_buffer(e::buffer::packer &packer, const double &t);
    void pack_string(e::buffer::packer &packer, const std::string &t, const uint32_t sz);
    void pack_buffer(e::buffer::packer &packer, const std::string &t);
    void pack_buffer(e::buffer::packer &packer, const vc::vclock &t);
    void pack_buffer(e::buffer::packer &packer, const node_prog::property &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::property &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::remote_node &t);
    void pack_buffer(e::buffer::packer &packer, const transaction::pending_update* const &ptr_t);
    void pack_buffer(e::buffer::packer &packer, const transaction::nop_data* const &ptr_t);
    void pack_buffer(e::buffer::packer &packer, const transaction::pending_tx &t);
    template <typename T1, typename T2> void pack_buffer(e::buffer::packer& packer, const std::unordered_map<T1, T2>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::unordered_set<T>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::vector<T>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::deque<T>& t);
    template <typename T1, typename T2, typename T3> void pack_buffer(e::buffer::packer&, std::priority_queue<T1, T2, T3>);
    template <typename T1, typename T2> inline void pack_buffer(e::buffer::packer &packer, const std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline void pack_buffer(e::buffer::packer &packer, const std::tuple<T1, T2, T3>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::shared_ptr<T> &ptr_t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::unique_ptr<T> &ptr_t);
    void pack_buffer(e::buffer::packer &packer, const node_prog::node_cache_context &t);
    void pack_buffer(e::buffer::packer &packer, const node_prog::edge_cache_context &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::element &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::edge &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::edge* const &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::node &t);

    void unpack_buffer(e::unpacker &unpacker, node_prog::Node_Parameters_Base &t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::Node_State_Base &t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::Cache_Value_Base &t);
    void unpack_buffer(e::unpacker &unpacker, enum msg_type &t);
    void unpack_buffer(e::unpacker &unpacker, enum node_prog::prog_type &t);
    void unpack_buffer(e::unpacker &unpacker, enum transaction::update_type &t);
    void unpack_buffer(e::unpacker &unpacker, enum transaction::tx_type &t);
    void unpack_buffer(e::unpacker &unpacker, bool &t);
    void unpack_buffer(e::unpacker &unpacker, uint8_t &t);
    void unpack_buffer(e::unpacker &unpacker, uint16_t &t);
    void unpack_buffer(e::unpacker &unpacker, uint32_t &t);
    void unpack_buffer(e::unpacker &unpacker, uint64_t &t);
    void unpack_buffer(e::unpacker &unpacker, int64_t &t);
    void unpack_buffer(e::unpacker &unpacker, int &t);
    void unpack_buffer(e::unpacker &unpacker, double &t);
    void unpack_string(e::unpacker &unpacker, std::string &t, const uint32_t sz);
    void unpack_buffer(e::unpacker &unpacker, std::string &t);
    void unpack_buffer(e::unpacker &unpacker, vc::vclock &t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::property &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::property &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::remote_node& t);
    void unpack_buffer(e::unpacker &unpacker, transaction::pending_update* &ptr_t);
    void unpack_buffer(e::unpacker &unpacker, transaction::nop_data* &ptr_t);
    void unpack_buffer(e::unpacker &unpacker, transaction::pending_tx &t);
    template <typename T1, typename T2> void unpack_buffer(e::unpacker& unpacker, std::unordered_map<T1, T2>& t);
    template <typename T> void unpack_buffer(e::unpacker& unpacker, std::unordered_set<T>& t);
    template <typename T> void unpack_buffer(e::unpacker& unpacker, std::vector<T>& t);
    template <typename T> void unpack_buffer(e::unpacker& unpacker, std::deque<T>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker&, std::priority_queue<T1, T2, T3>&);
    template <typename T1, typename T2> void unpack_buffer(e::unpacker& unpacker, std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker& unpacker, std::tuple<T1, T2, T3>& t);
    template <typename T> void unpack_buffer(e::unpacker& unpacker, std::shared_ptr<T> &ptr_t);
    template <typename T> void unpack_buffer(e::unpacker& unpacker, std::unique_ptr<T> &ptr_t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::node_cache_context &t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::edge_cache_context &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::element &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::edge &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::edge *&t);
    void unpack_buffer(e::unpacker &unpacker, db::element::node &t);

    // size templates

    template <typename T1, typename T2>
    inline uint64_t size(const std::pair<T1, T2> &t)
    {
        return size(t.first) + size(t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(const std::tuple<T1, T2, T3> &t){
        return size(std::get<0>(t)) + size(std::get<1>(t)) + size(std::get<2>(t));
    }

    template <typename T>
    inline uint64_t size(const std::shared_ptr<T> &ptr_t)
    {
        if (ptr_t.get() == NULL) {
            return 0;
        } else {
            return size(*ptr_t);
        }
    }

    template <typename T>
    inline uint64_t size(const std::unique_ptr<T> &ptr_t)
    {
        if (ptr_t.get() == NULL) {
            return 0;
        } else {
            return size(*ptr_t);
        }
    }

    template <typename T>
    inline uint64_t size(const std::unordered_set<T> &t)
    {
        // O(n) size operation can handle elements of differing sizes
        uint64_t total_size = sizeof(uint32_t);
        for (const T &elem : t) {
            total_size += size(elem);
        }
        return total_size;
    }

    template <typename T1, typename T2>
    inline uint64_t size(const std::unordered_map<T1, T2> &t)
    {
        // O(n) size operation can handle keys and values of differing sizes
        uint64_t total_size = sizeof(uint32_t);
        for (const std::pair<const T1, T2> &pair : t) {
            total_size += size(pair.first) + size(pair.second);
        }
        return total_size;
    }

    template <typename T>
    inline uint64_t size(const std::vector<T> &t)
    {
        uint64_t tot_size = sizeof(uint32_t);
        for (const T &elem: t) {
            tot_size += size(elem);
        }
        return tot_size;
    }
    
    template <typename T>
    inline uint64_t size(const std::deque<T> &t)
    {
        uint64_t tot_size = sizeof(uint32_t);
        for (const T &elem: t) {
            tot_size += size(elem);
        }
        return tot_size;
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(std::priority_queue<T1, T2, T3> t)
    {
        // cannot iterate pqueue so create a copy, no reference
        uint64_t sz = sizeof(uint32_t);
        while (!t.empty()) {
            sz += size(t.top());
            t.pop();
        }
        return sz;
    }

    // base case for recursive size_wrapper()
    template <typename T>
    inline uint64_t size_wrapper(const T &t)
    {
        return size(t);
    }

    // recursive wrapper around variadic templated size()
    template <typename T, typename... Args>
    inline uint64_t size_wrapper(const T &t, const Args&... args)
    {
        return size_wrapper(t) + size_wrapper(args...);
    }


    // packing templates

    template <typename T1, typename T2>
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::pair<T1, T2> &t)
    {
        // assumes constant size
        pack_buffer(packer, t.first);
        pack_buffer(packer, t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline void pack_buffer(e::buffer::packer &packer, const std::tuple<T1, T2, T3> &t){
        pack_buffer(packer, std::get<0>(t));
        pack_buffer(packer, std::get<1>(t));
        pack_buffer(packer, std::get<2>(t));
    }

    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::shared_ptr<T> &ptr_t)
    {
        if (ptr_t.get() != NULL) {
            pack_buffer(packer, *ptr_t);
        }
    }

    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::unique_ptr<T> &ptr_t)
    {
        if (ptr_t.get() != NULL) {
            pack_buffer(packer, *ptr_t);
        }
    }

    template <typename T> 
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::vector<T> &t)
    {
        // !assumes constant element size
        assert(t.size() <= UINT32_MAX);
        uint32_t num_elems = t.size();
        pack_buffer(packer, num_elems);
        for (const T &elem: t) {
            pack_buffer(packer, elem);
        }
    }

    template <typename T> 
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::deque<T> &t)
    {
        // !assumes constant element size
        assert(t.size() <= UINT32_MAX);
        uint32_t num_elems = t.size();
        pack_buffer(packer, num_elems);
        for (const T &elem : t) {
            pack_buffer(packer, elem);
        }
    }

    template <typename T1, typename T2, typename T3>
    inline void
    pack_buffer(e::buffer::packer &packer, std::priority_queue<T1, T2, T3> t)
    {
        assert(t.size() <= UINT32_MAX);
        uint32_t num_elems = t.size();
        packer = packer << num_elems;
        while (!t.empty()) {
            pack_buffer(packer, t.top());
            t.pop();
        }
    }

    template <typename T>
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::unordered_set<T> &t)
    {
        assert(t.size() <= UINT32_MAX);
        uint32_t num_keys = t.size();
        pack_buffer(packer, num_keys);
        for (const T &elem : t) {
            pack_buffer(packer, elem);
        }
    }

    template <typename T1, typename T2>
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::unordered_map<T1, T2> &t)
    {
        assert(t.size() <= UINT32_MAX);
        uint32_t num_keys = t.size();
        pack_buffer(packer, num_keys);
        for (const std::pair<const T1, T2> &pair : t) {
            pack_buffer(packer, pair.first);
            pack_buffer(packer, pair.second);
        }
    }

    // base case for recursive pack_buffer_wrapper()
    template <typename T>
    inline void 
    pack_buffer_wrapper(e::buffer::packer &packer, const T &t)
    {
        uint32_t before = packer.remain();
        pack_buffer(packer, t);
        assert((before - packer.remain()) == size(t) && "reserved size for type not same as number of bytes packed");
        UNUSED(before);
    }

    // recursive wrapper around variadic templated pack_buffer()
    // also performs buffer size sanity checks
    template <typename T, typename... Args>
    inline void 
    pack_buffer_wrapper(e::buffer::packer &packer, const T &t, const Args&... args)
    {
        pack_buffer_wrapper(packer, t);
        pack_buffer_wrapper(packer, args...);
    }

    // prepare message with only message_type and no additional payload
    inline void
    message :: prepare_message(const enum msg_type given_type)
    {
        uint64_t bytes_to_pack = size(given_type);
        type = given_type;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = buf->pack_at(BUSYBEE_HEADER_SIZE); 

        pack_buffer(packer, given_type);
        assert(packer.remain() == 0 && "reserved size for message not same as number of bytes packed");
    }

    template <typename... Args>
    inline void
    message :: prepare_message(const enum msg_type given_type, const Args&... args)
    {
        uint64_t bytes_to_pack = size_wrapper(args...) + size(given_type);
        type = given_type;
        buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = buf->pack_at(BUSYBEE_HEADER_SIZE); 

        pack_buffer(packer, given_type);
        pack_buffer_wrapper(packer, args...);
        assert(packer.remain() == 0 && "reserved size for message not same as number of bytes packed");
    }


    // unpacking templates

    template <typename T1, typename T2>
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::pair<T1, T2> &t)
    {
        //assumes constant size
        unpack_buffer(unpacker, t.first);
        unpack_buffer(unpacker, t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline void unpack_buffer(e::unpacker& unpacker, std::tuple<T1, T2, T3>& t){
        unpack_buffer(unpacker, std::get<0>(t));
        unpack_buffer(unpacker, std::get<1>(t));
        unpack_buffer(unpacker, std::get<2>(t));
    }

    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::shared_ptr<T> &ptr_t)
    {
        ptr_t.reset(new T());
        unpack_buffer(unpacker, *ptr_t);
    }

    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::unique_ptr<T> &ptr_t)
    {
        ptr_t.reset(new T());
        unpack_buffer(unpacker, *ptr_t);
    }

    template <typename T> 
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::vector<T> &t)
    {
        assert(t.size() == 0);
        uint32_t elements_left;
        unpack_buffer(unpacker, elements_left);

        t.resize(elements_left);

        for (uint32_t i = 0; i < elements_left; i++) {
            unpack_buffer(unpacker, t[i]);
        }
    }

    template <typename T> 
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::deque<T> &t)
    {
        assert(t.size() == 0);
        uint32_t elements_left;
        unpack_buffer(unpacker, elements_left);

        t.resize(elements_left);

        for (uint32_t i = 0; i < elements_left; i++) {
            unpack_buffer(unpacker, t[i]);
        }
    }

    template <typename T1, typename T2, typename T3>
    inline void
    unpack_buffer(e::unpacker &unpacker, std::priority_queue<T1, T2, T3> &t)
    {
        assert(t.size() == 0);
        uint32_t elements_left = 0;
        unpack_buffer(unpacker, elements_left);
        while (elements_left > 0) {
            T1 to_add;
            unpack_buffer(unpacker, to_add);
            t.push(std::move(to_add));
            elements_left--;
        }
    }

    template <typename T>
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::unordered_set<T> &t)
    {
        assert(t.size() == 0);
        uint32_t elements_left;
        unpack_buffer(unpacker, elements_left);

        t.reserve(elements_left);

        while (elements_left > 0) {
            T new_elem;
            unpack_buffer(unpacker, new_elem);
            t.emplace(new_elem);
            elements_left--;
        }
    }

    template <typename T1, typename T2>
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::unordered_map<T1, T2> &t)
    {
        assert(t.size() == 0);
        uint32_t elements_left;
        unpack_buffer(unpacker, elements_left);

        t.reserve(elements_left);

        while (elements_left > 0) {
            T1 key_to_add;
            unpack_buffer(unpacker, key_to_add);
            auto retPair = t.emplace(std::piecewise_construct, std::forward_as_tuple(key_to_add),
              std::forward_as_tuple()); // emplace key with no-arg constructor value
            unpack_buffer(unpacker, retPair.first->second); // unpacks value in place in map
            elements_left--;
        }
    }

    // base case for recursive unpack_buffer_wrapper()
    template <typename T>
    inline void
    unpack_buffer_wrapper(e::unpacker &unpacker, T &t)
    {
        unpack_buffer(unpacker, t);
    }

    // recursive weapper around variadic templated pack_buffer()
    template <typename T, typename... Args>
    inline void 
    unpack_buffer_wrapper(e::unpacker &unpacker, T &t, Args&... args)
    {
        unpack_buffer_wrapper(unpacker, t);
        unpack_buffer_wrapper(unpacker, args...);
    }

    template <typename... Args>
    inline void
    message :: unpack_message_internal(bool check_empty, const enum msg_type expected_type, Args&... args)
    {
        enum msg_type received_type;
        e::unpacker unpacker = buf->unpack_from(BUSYBEE_HEADER_SIZE);
        assert(!unpacker.error());

        unpack_buffer(unpacker, received_type);
        assert(received_type == expected_type);
        UNUSED(expected_type);

        unpack_buffer_wrapper(unpacker, args...);
        assert(!unpacker.error());
        if (check_empty) {
            assert(unpacker.empty()); // assert whole message was unpacked
        }
    }

    template <typename... Args>
    inline void
    message :: unpack_message(const enum msg_type expected_type, Args&... args)
    {
        unpack_message_internal(true, expected_type, args...);
    }

    template <typename... Args>
    inline void
    message :: unpack_partial_message(const enum msg_type expected_type, Args&... args)
    {
        unpack_message_internal(false, expected_type, args...);
    }

}

#endif
