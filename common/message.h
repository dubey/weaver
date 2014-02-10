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
#include <vector>
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
#include "node_prog/base_classes.h" // used for packing Packable objects
#include "db/element/node.h"
#include "db/element/edge.h"
#include "db/element/remote_node.h"

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
        DONE_MIGR,

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
    template <typename T1, typename T2, typename T3> uint64_t size(std::priority_queue<T1, T2, T3>);
    template <typename T1, typename T2> inline uint64_t size(const std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline uint64_t size(const std::tuple<T1, T2, T3>& t);
    template <typename T> inline uint64_t size(const std::shared_ptr<T> &ptr_t);

    template <typename T1, typename T2> inline void pack_buffer(e::buffer::packer& packer, const std::unordered_map<T1, T2>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::unordered_set<T>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::vector<T>& t);
    template <typename T1, typename T2, typename T3> void pack_buffer(e::buffer::packer&, std::priority_queue<T1, T2, T3>);
    template <typename T1, typename T2> inline void pack_buffer(e::buffer::packer &packer, const std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline void pack_buffer(e::buffer::packer &packer, const std::tuple<T1, T2, T3>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::shared_ptr<T> &ptr_t);

    template <typename T1, typename T2> inline void unpack_buffer(e::unpacker& unpacker, std::unordered_map<T1, T2>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::unordered_set<T>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::vector<T>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker&, std::priority_queue<T1, T2, T3>&);
    template <typename T1, typename T2> inline void unpack_buffer(e::unpacker& unpacker, std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline void unpack_buffer(e::unpacker& unpacker, std::tuple<T1, T2, T3>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::shared_ptr<T> &ptr_t);

    uint64_t size(const db::caching::node_cache_context &t);
    uint64_t size(const db::element::edge &t);
    uint64_t size(const db::element::edge* const &t);
    uint64_t size(const db::element::node &t);
    uint64_t size(const db::element::property &t);
    uint64_t size(const node_prog::property &t);
    uint64_t size(const node_prog::node_handle &t);
    void pack_buffer(e::buffer::packer &packer, const db::caching::node_cache_context &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::edge &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::edge* const &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::node &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::property &t);
    void pack_buffer(e::buffer::packer &packer, const node_prog::property &t);
    void pack_buffer(e::buffer::packer &packer, const node_prog::node_handle &t);
    void unpack_buffer(e::unpacker &unpacker, db::caching::node_cache_context &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::edge &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::edge *&t);
    void unpack_buffer(e::unpacker &unpacker, db::element::node &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::property &t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::property &t);
    void unpack_buffer(e::unpacker &unpacker, node_prog::node_handle &t);
    void unpack_buffer(e::unpacker &unpacker, std::vector<node_prog::property> &t);
    void unpack_buffer(e::unpacker &unpacker, std::vector<node_prog::node_handle> &t);

    inline
    message :: message()
        : type(ERROR)
    {
    }

    inline
    message :: message(enum msg_type t)
        : type(t)
    {
    }

    inline 
    message :: message(message &copy)
        : type(copy.type)
    {
        buf = copy.buf;
    }

    inline void
    message :: change_type(enum msg_type t)
    {
        type = t;
    }

// size templates
    inline uint64_t size(const node_prog::prog_type&)
    {
        return sizeof(uint32_t);
    }
    /*
    inline uint64_t size(const node_prog::Packable &t)
    {
        return t.size();
    }
    */
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
        return sizeof(uint16_t);
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
        return t.size() + sizeof(uint64_t);
    }
    inline uint64_t size(const vc::vclock &t)
    {
        return size(t.vt_id)
            + size(t.clock);
    }
    inline uint64_t size(const db::element::remote_node &t)
    {
        return size(t.loc) + size(t.id);
    }

    inline uint64_t
    size(const std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        transaction::pending_update &t = *ptr_t;
        uint64_t sz = sizeof(t.type)
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

    template <typename T1, typename T2>
    inline uint64_t size(const std::pair<T1, T2> &t)
    {
        return size(t.first) + size(t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(const std::tuple<T1, T2, T3> &t){
        return size(std::get<0>(t)) + size(std::get<1>(t)) + size(std::get<2>(t));
    }

    template <typename T> inline uint64_t size(const std::shared_ptr<T> &ptr_t){
        if (ptr_t.get() == NULL){
            return 0;
        } else {
            return size(*ptr_t);
        }
    }

    template <typename T>
    inline uint64_t size(const std::unordered_set<T> &t)
    {
        // O(n) size operation can handle elements of differing sizes
        uint64_t total_size = 0;
        for(const T &elem : t) {
            total_size += size(elem);
        }
        return sizeof(uint64_t)+total_size;
    }

    template <typename T1, typename T2>
    inline uint64_t size(const std::unordered_map<T1, T2> &t)
    {
        uint64_t total_size = 0;
        // O(n) size operation can handle keys and values of differing sizes
        for (const std::pair<T1,T2> &pair : t) {
            total_size += size(pair.first) + size(pair.second);
        }
        return sizeof(uint64_t)+total_size;
    }

    template <typename T>
    inline uint64_t size(const std::vector<T> &t)
    {
        uint64_t tot_size = sizeof(uint64_t);
        for (const T &elem : t) {
            tot_size += size(elem);
        }
        return tot_size;
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(std::priority_queue<T1, T2, T3> t)
    {
        uint64_t sz = sizeof(uint64_t);
        while (!t.empty()) {
            sz += size(t.top());
            t.pop();
        }
        return sz;
    }

    template <typename T, typename... Args>
    inline uint64_t size(const T &t, const Args&... args)
    {
        return size(t) + size(args...);
    }

    // packing templates

    /*
    inline void pack_buffer(e::buffer::packer &packer, const node_prog::Packable &t)
    {
        t.pack(packer);
    }
    */
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
    inline void pack_buffer(e::buffer::packer &packer, const node_prog::prog_type &t)
    {
        packer = packer << t;
    }

    inline void pack_buffer(e::buffer::packer &packer, const bool &t)
    {
        uint16_t to_pack = t ? 1 : 0;
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
        uint8_t strchar;
        uint64_t strlen = t.size();
        pack_buffer(packer, strlen);
        for (uint64_t i = 0; i < strlen; i++) {
            strchar = (uint8_t)t[i];
            pack_buffer(packer, strchar);
        }
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const vc::vclock &t)
    {
        pack_buffer(packer, t.vt_id);
        pack_buffer(packer, t.clock);
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const db::element::remote_node &t)
    {
        packer = packer << t.loc << t.id;
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        transaction::pending_update &t = *ptr_t;
        packer = packer << t.type;
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

    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::shared_ptr<T> &ptr_t){
        if (ptr_t.get() != NULL) {
            pack_buffer(packer, *ptr_t);
        }
    }

    template <typename T> 
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::vector<T> &t)
    {
        // !assumes constant element size
        uint64_t num_elems = t.size();
        packer = packer << num_elems;
        if (num_elems > 0){
            for (const T &elem : t) {
                pack_buffer(packer, elem);
            }
        }
    }
    
    template <typename T1, typename T2, typename T3>
    inline void
    pack_buffer(e::buffer::packer &packer, std::priority_queue<T1, T2, T3> t)
    {
        uint64_t num_elems = t.size();
        packer = packer << num_elems;
        while (!t.empty()) {
            pack_buffer(packer, t.top());
            t.pop();
        }
    }

    /* vector with elements of constant size
    template <typename T> 
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::vector<T> &t)
    {
        // !assumes constant element size
        uint64_t num_elems = t.size();
        //std::cout << "pack buffer packed vector of size " << num_elems<< std::endl;
        packer = packer << num_elems;
        if (num_elems > 0){
            uint64_t element_size = size(t[0]);
            for (const T &elem : t) {
                pack_buffer(packer, elem);
                assert(element_size == size(elem));//slow
            }
        }
    }
    */

    template <typename T>
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::unordered_set<T> &t)
    {
        uint64_t num_keys = t.size();
        packer = packer << num_keys;
        for (const T &elem : t) {
            pack_buffer(packer, elem);
        }
    }

    template <typename T1, typename T2>
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::unordered_map<T1, T2> &t)
    {
        uint64_t num_keys = t.size();
        packer = packer << num_keys;
        for (const std::pair<T1, T2> &pair : t) {
            pack_buffer(packer, pair.first);
            pack_buffer(packer, pair.second);
        }
    }

    template <typename T>
    inline void 
    pack_buffer_checked(e::buffer::packer &packer, const T &t)
    {
        uint32_t before = packer.remain();
        pack_buffer(packer, t);
        assert((before - packer.remain()) == size(t) && "reserved size for type not same as number of bytes packed");
        UNUSED(before);
    }

    template <typename T, typename... Args>
    inline void 
    pack_buffer_checked(e::buffer::packer &packer, const T &t, const Args&... args)
    {
        pack_buffer_checked(packer, t);
        pack_buffer_checked(packer, args...);
    }

    inline void
    prepare_message(message &m, enum msg_type given_type)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        uint64_t bytes_to_pack =  sizeof(enum msg_type);
        m.type = given_type;
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));

        m.buf->pack_at(index) << given_type;
    }

    template <typename... Args>
    inline void
    prepare_message(message &m, const enum msg_type given_type, const Args&... args)
    {
        uint64_t bytes_to_pack = size(args...) + sizeof(enum msg_type);
        m.type = given_type;
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = m.buf->pack_at(BUSYBEE_HEADER_SIZE); 

        packer = packer << given_type;
        pack_buffer_checked(packer, args...);
        assert(packer.remain() == 0 && "reserved size for message not same as nubmer of bytes packed");
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
    /*
    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::Packable &t)
    {
        t.unpack(unpacker);
    }
    */
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
    unpack_buffer(e::unpacker &unpacker, node_prog::prog_type &t)
    {
        uint32_t _type;
        unpacker = unpacker >> _type;
        t = (enum node_prog::prog_type)_type;
    }
    inline void
    unpack_buffer(e::unpacker &unpacker, bool &t)
    {
        uint16_t temp;
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
        uint64_t strlen;
        uint8_t strchar;
        unpack_buffer(unpacker, strlen);
        t.resize(strlen);
        for (uint64_t i = 0; i < strlen; i++) {
            unpack_buffer(unpacker, strchar);
            t[i] = (char)strchar;
        }
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, vc::vclock &t)
    {
        unpack_buffer(unpacker, t.vt_id);
        unpack_buffer(unpacker, t.clock);
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, db::element::remote_node& t)
    {
        unpacker = unpacker >> t.loc >> t.id;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        uint32_t mtype;
        ptr_t.reset(new transaction::pending_update());
        transaction::pending_update &t = *ptr_t;
        unpacker = unpacker >> mtype;
        t.type = ((enum transaction::update_type)mtype);
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

    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::shared_ptr<T> &ptr_t){
        ptr_t.reset(new T());
        unpack_buffer(unpacker, *ptr_t);
    }

    template <typename T> 
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::vector<T> &t)
    {
        assert(t.size() == 0);
        uint64_t elements_left;
        unpacker = unpacker >> elements_left;

        t.reserve(elements_left);

        while (elements_left > 0) {
            T to_add;
            unpack_buffer(unpacker, to_add);
            t.emplace_back(std::move(to_add));
            elements_left--;
        }
    }

    template <typename T1, typename T2, typename T3>
    inline void
    unpack_buffer(e::unpacker &unpacker, std::priority_queue<T1, T2, T3> &t)
    {
        assert(t.size() == 0);
        uint64_t elements_left = 0;
        unpacker = unpacker >> elements_left;
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
        uint64_t elements_left;
        unpacker = unpacker >> elements_left;

        t.rehash(elements_left*1.25); // set number of buckets to 1.25*elements it will contain

        while (elements_left > 0) {
            T to_add;
            unpack_buffer(unpacker, to_add);
            t.emplace(std::move(to_add));
            elements_left--;
        }
    }

    template <typename T1, typename T2>
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::unordered_map<T1, T2> &t)
    {
        assert(t.size() == 0);
        uint64_t elements_left;
        unpacker = unpacker >> elements_left;
        // set number of buckets to 1.25*elements it will contain
        // did not use reserve as max_load_factor is default 1
        t.rehash(elements_left*1.25); 

        while (elements_left > 0) {
            T1 key_to_add;
            T2 val_to_add;

            unpack_buffer(unpacker, key_to_add);
            unpack_buffer(unpacker, val_to_add);
            t.emplace(key_to_add, std::move(val_to_add));
            elements_left--;
        }
    }

    template <typename T, typename... Args>
    inline void 
    unpack_buffer(e::unpacker &unpacker, T &t, Args&... args)
    {
        unpack_buffer(unpacker, t);
        unpack_buffer(unpacker, args...);
    }

    template <typename... Args>
    inline void
    unpack_message_internal(bool check_empty, const message &m, const enum msg_type expected_type, Args&... args)
    {
        uint32_t _type;
        e::unpacker unpacker = m.buf->unpack_from(BUSYBEE_HEADER_SIZE);
        assert(!unpacker.error());

        unpacker = unpacker >> _type;
        assert((enum msg_type)_type == expected_type);
        UNUSED(expected_type);

        unpack_buffer(unpacker, args...);
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

    inline void
    unpack_client_tx(message &m, transaction::pending_tx &tx)
    {
        uint64_t num_tx;
        uint32_t type;
        enum msg_type mtype;
        e::unpacker unpacker = m.buf->unpack_from(BUSYBEE_HEADER_SIZE);
        unpacker = unpacker >> type;
        mtype = (enum msg_type)type;
        assert(mtype == CLIENT_TX_INIT);
        unpack_buffer(unpacker, num_tx);
        while (num_tx-- > 0) {
            auto upd = std::make_shared<transaction::pending_update>();
            tx.writes.emplace_back(upd);
            unpacker = unpacker >> type;
            mtype = (enum msg_type)type;
            switch (mtype) {
                case CLIENT_NODE_CREATE_REQ:
                    upd->type = transaction::NODE_CREATE_REQ;
                    unpack_buffer(unpacker, upd->id); 
                    break;

                case CLIENT_EDGE_CREATE_REQ:
                    upd->type = transaction::EDGE_CREATE_REQ;
                    unpack_buffer(unpacker, upd->id, upd->elem1, upd->elem2);
                    break;

                case CLIENT_NODE_DELETE_REQ:
                    upd->type = transaction::NODE_DELETE_REQ;
                    unpack_buffer(unpacker, upd->elem1);
                    break;

                case CLIENT_EDGE_DELETE_REQ:
                    upd->type = transaction::EDGE_DELETE_REQ;
                    unpack_buffer(unpacker, upd->elem1, upd->elem2);
                    break;

                case CLIENT_NODE_SET_PROP:
                    upd->type = transaction::NODE_SET_PROPERTY;
                    upd->key.reset(new std::string());
                    upd->value.reset(new std::string());
                    unpack_buffer(unpacker, upd->elem1, *upd->key, *upd->value);
                    break;

                case CLIENT_EDGE_SET_PROP:
                    upd->type = transaction::EDGE_SET_PROPERTY;
                    upd->key.reset(new std::string());
                    upd->value.reset(new std::string());
                    unpack_buffer(unpacker, upd->elem1, upd->elem2, *upd->key, *upd->value);
                    break;

                default:
                    WDEBUG << "bad msg type: " << mtype << std::endl;
            }
        }
    }
}

#endif
