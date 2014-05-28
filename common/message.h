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

#include "common/weaver_constants.h"
#include "common/vclock.h"
#include "common/transaction.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/base_classes.h"
#include "node_prog/property.h"
#include "db/element/remote_node.h"
#include "db/element/property.h"

namespace db
{
    namespace element
    {
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
        CLIENT_NODE_CREATE_REQ = 0,
        CLIENT_EDGE_CREATE_REQ,
        CLIENT_NODE_DELETE_REQ,
        CLIENT_EDGE_DELETE_REQ,
        CLIENT_NODE_SET_PROP,
        CLIENT_EDGE_SET_PROP,
        CLIENT_TX_INIT,
        CLIENT_TX_DONE,
        CLIENT_TX_FAIL,
        CLIENT_REPLY,
        CLIENT_COMMIT_GRAPH,
        CLIENT_NODE_PROG_REQ,
        CLIENT_NODE_PROG_REPLY,
        CLIENT_NODE_LOC_REQ,
        CLIENT_NODE_LOC_REPLY,
        START_MIGR,
        ONE_STREAM_MIGR,
        EXIT_WEAVER,
        // graph update messages
        TX_INIT,
        TX_DONE,
        NODE_CREATE_REQ,
        EDGE_CREATE_REQ,
        NODE_DELETE_REQ,
        EDGE_DELETE_REQ,
        NODE_SET_PROP,
        EDGE_SET_PROP,
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
        COORD_CLOCK_REQ,
        COORD_CLOCK_REPLY,
        MIGRATED_NBR_UPDATE,
        MIGRATED_NBR_ACK,
        MIGRATION_TOKEN,
        MSG_COUNT,
        CLIENT_MSG_COUNT,
        CLIENT_NODE_COUNT,
        NODE_COUNT_REPLY,
        // initial graph loading
        LOADED_GRAPH,
        // coordinator group
        COORD_LOC_REQ,
        COORD_LOC_REPLY,
        VT_CLOCK_UPDATE,
        VT_CLOCK_UPDATE_ACK,
        VT_NOP,
        VT_NOP_ACK,
        PREP_DEL_TX,
        DONE_DEL_TX,
        DONE_MIGR,
        // fault tolerance
        SET_QTS,

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
            message();
            message(enum msg_type t);
            message(message &copy);

        public:
            enum msg_type type;
            std::auto_ptr<e::buffer> buf;

        public:
            void change_type(enum msg_type t);
    };

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
    uint64_t size(const db::element::edge &t);
    uint64_t size(const db::element::edge* const &t);
    uint64_t size(const db::element::node &t);

    template <typename T1, typename T2> inline void pack_buffer(e::buffer::packer& packer, const std::unordered_map<T1, T2>& t);
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
    void pack_buffer(e::buffer::packer &packer, const db::element::edge &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::edge* const &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::node &t);

    template <typename T1, typename T2> inline void unpack_buffer(e::unpacker& unpacker, std::unordered_map<T1, T2>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::unordered_set<T>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::vector<T>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::deque<T>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker&, std::priority_queue<T1, T2, T3>&);
    template <typename T1, typename T2> inline void unpack_buffer(e::unpacker& unpacker, std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline void unpack_buffer(e::unpacker& unpacker, std::tuple<T1, T2, T3>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::shared_ptr<T> &ptr_t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::unique_ptr<T> &ptr_t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::node_cache_context &t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::edge_cache_context &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::edge &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::edge *&t);
    void unpack_buffer(e::unpacker &unpacker, db::element::node &t);

    inline
    message :: message()
        : type(ERROR)
          , buf(NULL)
    { }

    inline
    message :: message(enum msg_type t)
        : type(t)
          , buf(NULL)
    { }

    inline 
    message :: message(message &copy)
        : type(copy.type)
    {
        buf.reset(copy.buf->copy());
    }

    inline void
    message :: change_type(enum msg_type t)
    {
        type = t;
    }

    // size templates

    inline uint64_t size(const enum msg_type &)
    {
        return sizeof(uint8_t);
    }

    inline uint64_t size(const enum node_prog::prog_type&)
    {
        return sizeof(uint8_t);
    }

    inline uint64_t size(const enum transaction::update_type&)
    {
        return sizeof(uint8_t);
    }

    inline uint64_t size(const node_prog::Node_Parameters_Base &t)
    {
        return t.size();
    }

    inline uint64_t size(const node_prog::Node_State_Base &t)
    {
        return t.size();
    }

    inline uint64_t size(const node_prog::Cache_Value_Base &t)
    {
        return t.size();
    }

    inline uint64_t size(const bool&)
    {
        return sizeof(uint8_t);
    }

    inline uint64_t size(const char&)
    {
        return sizeof(uint8_t);
    }

    inline uint64_t size(const uint16_t&)
    {
        return sizeof(uint16_t);
    }

    inline uint64_t size(const uint32_t&)
    {
        return sizeof(uint32_t);
    }

    inline uint64_t size(const uint64_t&)
    {
        return sizeof(uint64_t);
    }

    inline uint64_t size(const int64_t&)
    {
        return sizeof(int64_t);
    }

    inline uint64_t size(const int&)
    {
        return sizeof(int);
    }

    inline uint64_t size(const double&)
    {
        return sizeof(uint64_t);
    }

    inline uint64_t size(const std::string &t)
    {
        return t.size() + sizeof(uint32_t);
    }

    inline uint64_t size(const vc::vclock &t)
    {
        return size(t.vt_id)
            + size(t.clock);
    }

    inline uint64_t size(const node_prog::property &t)
    {
        return size(t.key)
            + size(t.value);
    }

    inline uint64_t size(const db::element::property &t)
    {
        return size(t.key)
            + size(t.value)
            + 2*size(t.creat_time); // for del time
    }

    inline uint64_t size(const db::element::remote_node &t)
    {
        return size(t.loc) + size(t.id);
    }

    inline uint64_t
    size(const std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        transaction::pending_update &t = *ptr_t;
        uint64_t sz = size(t.type)
             + size(t.qts)
             + size(t.id)
             + size(t.elem1)
             + size(t.elem2)
             + size(t.loc1)
             + size(t.loc2);
        if (t.type == transaction::NODE_SET_PROPERTY
         || t.type == transaction::EDGE_SET_PROPERTY) {
            sz += size(*t.key)
             + size(*t.value);
        }
        return sz;
    }

    inline uint64_t
    size(const transaction::pending_tx &t)
    {
        return size(t.id)
             + size(t.client_id)
             + size(t.writes)
             + size(t.timestamp);
    }

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

    inline void pack_buffer(e::buffer::packer &packer, const node_prog::Node_Parameters_Base &t)
    {
        t.pack(packer);
    }

    inline void pack_buffer(e::buffer::packer &packer, const node_prog::Node_State_Base &t)
    {
        t.pack(packer);
    }

    inline void pack_buffer(e::buffer::packer &packer, const node_prog::Cache_Value_Base *&t)
    {
        t->pack(packer);
    }

    inline void pack_buffer(e::buffer::packer &packer, const enum msg_type &t)
    {
        assert(t <= UINT8_MAX);
        uint8_t temp = (uint8_t) t;
        packer = packer << temp;
    }
    
    inline void pack_buffer(e::buffer::packer &packer, const enum node_prog::prog_type &t)
    {
        assert(t <= UINT8_MAX);
        uint8_t temp = (uint8_t) t;
        packer = packer << temp;
    }

    inline void pack_buffer(e::buffer::packer &packer, const enum transaction::update_type&t)
    {
        assert(t <= UINT8_MAX);
        uint8_t temp = (uint8_t) t;
        packer = packer << temp;
    }

    inline void pack_buffer(e::buffer::packer &packer, const bool &t)
    {
        uint8_t to_pack = 0;
        if (t) {
            to_pack = 1;
        }
        packer = packer << to_pack;
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const uint8_t &t)
    {
        packer = packer << t;
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const uint16_t &t)
    {
        packer = packer << t;
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const uint32_t &t)
    {
        packer = packer << t;
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const uint64_t &t)
    {
        packer = packer << t;
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const int64_t &t)
    {
        packer = packer << t;
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const int &t)
    {
        packer = packer << t;
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const double &t)
    {
        uint64_t dbl;
        memcpy(&dbl, &t, sizeof(double)); //to avoid casting issues, probably could avoid copy
        packer = packer << dbl;
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const std::string &t)
    {
        assert(t.size() <= UINT32_MAX);
        uint32_t strlen = t.size();
        packer = packer << strlen;

        uint32_t words = strlen / 8;
        uint32_t leftover_chars = strlen % 8;

        const char *rawchars = t.data();
        const uint64_t *rawwords = (const uint64_t*) rawchars;

        for (uint32_t i = 0; i < words; i++) {
            pack_buffer(packer, rawwords[i]);
        }

        for (uint32_t i = 0; i < leftover_chars; i++) {
            pack_buffer(packer, (uint8_t) rawchars[words*8+i]);
        }
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const vc::vclock &t)
    {
        pack_buffer(packer, t.vt_id);
        pack_buffer(packer, t.clock);
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const node_prog::property &t)
    {
        pack_buffer(packer, t.key);
        pack_buffer(packer, t.value);
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const db::element::property &t)
    {
        pack_buffer(packer, t.key);
        pack_buffer(packer, t.value);
        pack_buffer(packer, t.creat_time);
        pack_buffer(packer, t.del_time);
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const db::element::remote_node &t)
    {
        pack_buffer(packer, t.loc);
        pack_buffer(packer, t.id);
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        transaction::pending_update &t = *ptr_t;
        pack_buffer(packer, t.type);
        pack_buffer(packer, t.qts);
        pack_buffer(packer, t.id);
        pack_buffer(packer, t.elem1);
        pack_buffer(packer, t.elem2);
        pack_buffer(packer, t.loc1);
        pack_buffer(packer, t.loc2);
        if (t.type == transaction::NODE_SET_PROPERTY
         || t.type == transaction::EDGE_SET_PROPERTY) {
            pack_buffer(packer, *t.key);
            pack_buffer(packer, *t.value);
        }
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const transaction::pending_tx &t)
    {
        pack_buffer(packer, t.id);
        pack_buffer(packer, t.client_id);
        pack_buffer(packer, t.writes);
        pack_buffer(packer, t.timestamp);
    }

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
    prepare_message(message &m, const enum msg_type given_type)
    {
        uint64_t bytes_to_pack = size(given_type);
        m.type = given_type;
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = m.buf->pack_at(BUSYBEE_HEADER_SIZE); 


        pack_buffer(packer, given_type);
        assert(packer.remain() == 0 && "reserved size for message not same as number of bytes packed");
    }

    template <typename... Args>
    inline void
    prepare_message(message &m, const enum msg_type given_type, const Args&... args)
    {
        uint64_t bytes_to_pack = size_wrapper(args...) + size(given_type);
        m.type = given_type;
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = m.buf->pack_at(BUSYBEE_HEADER_SIZE); 

        pack_buffer(packer, given_type);
        pack_buffer_wrapper(packer, args...);
        assert(packer.remain() == 0 && "reserved size for message not same as number of bytes packed");
    }

    inline void
    shift_buffer(message &m, uint32_t shift_off)
    {
        m.buf->shift(shift_off);
    }

    inline bool
    empty_buffer(message &m)
    {
        return m.buf->empty();
    }


    // unpacking templates

    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::Node_Parameters_Base &t)
    {
        t.unpack(unpacker);
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::Node_State_Base &t)
    {
        t.unpack(unpacker);
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::Cache_Value_Base &t)
    {
        t.unpack(unpacker);
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, enum msg_type &t)
    {
        uint8_t _type;
        unpacker = unpacker >> _type;
        t = (enum msg_type)_type;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, enum node_prog::prog_type &t)
    {
        uint8_t _type;
        unpacker = unpacker >> _type;
        t = (enum node_prog::prog_type)_type;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, enum transaction::update_type &t)
    {
        uint8_t _type;
        unpacker = unpacker >> _type;
        t = (enum transaction::update_type)_type;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, bool &t)
    {
        uint8_t temp;
        unpacker = unpacker >> temp;
        t = (temp != 0);
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, uint8_t &t)
    {
        unpacker = unpacker >> t;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, uint16_t &t)
    {
        unpacker = unpacker >> t;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, uint32_t &t)
    {
        unpacker = unpacker >> t;
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, uint64_t &t)
    {
        unpacker = unpacker >> t;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, int64_t &t)
    {
        unpacker = unpacker >> t;
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, int &t)
    {
        unpacker = unpacker >> t;
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, double &t)
    {
        uint64_t dbl;
        unpacker = unpacker >> dbl;
        memcpy(&t, &dbl, sizeof(double)); //to avoid casting issues, probably could avoid copy
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, std::string &t)
    {
        uint32_t strlen;
        unpack_buffer(unpacker, strlen);
        t.resize(strlen);

        uint32_t words = strlen / 8;
        uint32_t leftover_chars = strlen % 8;

        const char* rawchars = t.data();
        uint8_t* rawuint8s = (uint8_t*) rawchars;
        uint64_t* rawwords = (uint64_t*) rawchars;

        for (uint32_t i = 0; i < words; i++) {
            unpack_buffer(unpacker, rawwords[i]);
        }

        for (uint32_t i = 0; i < leftover_chars; i++) {
            unpack_buffer(unpacker, rawuint8s[words*8+i]);
        }
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, vc::vclock &t)
    {
        unpack_buffer(unpacker, t.vt_id);
        unpack_buffer(unpacker, t.clock);
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, node_prog::property &t)
    {
        unpack_buffer(unpacker, t.key);
        unpack_buffer(unpacker, t.value);
    }
    inline void 
    unpack_buffer(e::unpacker &unpacker, db::element::property &t)
    {
        unpack_buffer(unpacker, t.key);
        unpack_buffer(unpacker, t.value);
        t.creat_time.clock.clear();
        t.del_time.clock.clear();
        unpack_buffer(unpacker, t.creat_time);
        unpack_buffer(unpacker, t.del_time);
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, db::element::remote_node& t)
    {
        unpack_buffer(unpacker, t.loc);
        unpack_buffer(unpacker, t.id);
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        ptr_t.reset(new transaction::pending_update());
        transaction::pending_update &t = *ptr_t;
        unpack_buffer(unpacker, t.type);
        unpack_buffer(unpacker, t.qts);
        unpack_buffer(unpacker, t.id);
        unpack_buffer(unpacker, t.elem1);
        unpack_buffer(unpacker, t.elem2);
        unpack_buffer(unpacker, t.loc1);
        unpack_buffer(unpacker, t.loc2);
        if (t.type == transaction::NODE_SET_PROPERTY
         || t.type == transaction::EDGE_SET_PROPERTY) {
            t.key.reset(new std::string());
            t.value.reset(new std::string());
            unpack_buffer(unpacker, *t.key);
            unpack_buffer(unpacker, *t.value);
        }
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, transaction::pending_tx &t)
    {
        unpack_buffer(unpacker, t.id);
        unpack_buffer(unpacker, t.client_id);
        unpack_buffer(unpacker, t.writes);
        unpack_buffer(unpacker, t.timestamp);
    }

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

        for (int i = 0; i < elements_left; i++) {
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

        for (int i = 0; i < elements_left; i++) {
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
    unpack_message_internal(bool check_empty, const message &m, const enum msg_type expected_type, Args&... args)
    {
        enum msg_type received_type;
        e::unpacker unpacker = m.buf->unpack_from(BUSYBEE_HEADER_SIZE);
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
    unpack_message(const message &m, const enum msg_type expected_type, Args&... args)
    {
        unpack_message_internal(true, m , expected_type, args...);
    }

    template <typename... Args>
    inline void
    unpack_partial_message(const message &m, const enum msg_type expected_type, Args&... args)
    {
        unpack_message_internal(false, m , expected_type, args...);
    }

    inline enum msg_type
    unpack_message_type(const message &m)
    {
        enum msg_type mtype;
        auto unpacker = m.buf->unpack_from(BUSYBEE_HEADER_SIZE);
        unpack_buffer(unpacker, mtype);
        return mtype;
    }

    inline void
    unpack_client_tx(message &m, transaction::pending_tx &tx)
    {
        uint32_t num_tx;
        enum msg_type mtype;
        e::unpacker unpacker = m.buf->unpack_from(BUSYBEE_HEADER_SIZE);
        unpack_buffer(unpacker, mtype);
        assert(mtype == CLIENT_TX_INIT);
        unpack_buffer(unpacker, num_tx);

        while (num_tx-- > 0) {
            auto upd = *(tx.writes.emplace(tx.writes.end(), std::make_shared<transaction::pending_update>()));
            unpack_buffer(unpacker, mtype);
            switch (mtype) {
                case CLIENT_NODE_CREATE_REQ:
                    upd->type = transaction::NODE_CREATE_REQ;
                    unpack_buffer(unpacker, upd->id); 
                    break;

                case CLIENT_EDGE_CREATE_REQ:
                    upd->type = transaction::EDGE_CREATE_REQ;
                    unpack_buffer(unpacker, upd->id);
                    unpack_buffer(unpacker, upd->elem1);
                    unpack_buffer(unpacker, upd->elem2);
                    break;

                case CLIENT_NODE_DELETE_REQ:
                    upd->type = transaction::NODE_DELETE_REQ;
                    unpack_buffer(unpacker, upd->elem1);
                    break;

                case CLIENT_EDGE_DELETE_REQ:
                    upd->type = transaction::EDGE_DELETE_REQ;
                    unpack_buffer(unpacker, upd->elem1);
                    unpack_buffer(unpacker, upd->elem2);
                    break;

                case CLIENT_NODE_SET_PROP:
                    upd->type = transaction::NODE_SET_PROPERTY;
                    upd->key.reset(new std::string());
                    upd->value.reset(new std::string());
                    unpack_buffer(unpacker, upd->elem1);
                    unpack_buffer(unpacker, *upd->key);
                    unpack_buffer(unpacker, *upd->value);
                    break;

                case CLIENT_EDGE_SET_PROP:
                    upd->type = transaction::EDGE_SET_PROPERTY;
                    upd->key.reset(new std::string());
                    upd->value.reset(new std::string());
                    unpack_buffer(unpacker, upd->elem1);
                    unpack_buffer(unpacker, upd->elem2);
                    unpack_buffer(unpacker, *upd->key);
                    unpack_buffer(unpacker, *upd->value);
                    break;

                default:
                    WDEBUG << "bad msg type: " << mtype << std::endl;
            }
        }
    }
}

#endif
